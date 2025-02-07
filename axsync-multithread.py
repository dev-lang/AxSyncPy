import os
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import argparse

# Función para descargar una parte de un archivo
def download_chunk(url, start, end, file_name, part_number):
    headers = {
        'Range': f'bytes={start}-{end}',
        'Accept-Encoding': 'identity'  # Evitar compresión gzip
    }
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    
    # Guardar la parte descargada en un archivo temporal
    part_file = f"{file_name}.part{part_number}"
    with open(part_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return part_file

# Función para combinar las partes descargadas
def combine_parts(file_name, parts):
    with open(file_name, 'wb') as f:
        for part in parts:
            with open(part, 'rb') as p:
                f.write(p.read())
            os.remove(part)  # Eliminar la parte después de combinarla

# Función principal para descargar un archivo en paralelo
def download_file(url, file_name, num_threads=8):
    # Obtener el tamaño total del archivo
    headers = {'Accept-Encoding': 'identity'}  # Evitar compresión gzip
    response = requests.head(url, headers=headers)
    total_size = int(response.headers.get('content-length', 0))
    
    # Calcular el tamaño de cada parte
    chunk_size = total_size // num_threads
    ranges = [(i * chunk_size, (i + 1) * chunk_size - 1) for i in range(num_threads)]
    ranges[-1] = (ranges[-1][0], total_size - 1)  # Asegurar que la última parte cubra el resto

    # Descargar las partes en paralelo
    parts = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, (start, end) in enumerate(ranges):
            futures.append(executor.submit(download_chunk, url, start, end, file_name, i))
        
        # Mostrar el progreso de la descarga
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(file_name)) as pbar:
            for future in as_completed(futures):
                part_file = future.result()
                parts.append(part_file)
                pbar.update(os.path.getsize(part_file))

    # Combinar las partes descargadas
    combine_parts(file_name, parts)
    print(f"{os.path.basename(file_name)} descargado con éxito.")

# Función para descargar todos los archivos de una carpeta FTP
def download_directory(base_url, destination_folder, num_threads=8):
    # Obtener el contenido de la página
    headers = {'Accept-Encoding': 'identity'}  # Evitar compresión gzip
    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Crear la carpeta de destino si no existe
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Encontrar todos los enlaces a archivos
    for link in soup.find_all('a'):
        file_name = link.get('href')
        
        # Ignorar enlaces que no son archivos (parámetros de URL o directorios)
        if not file_name or file_name.startswith('?') or file_name.endswith('/') or file_name == '../':
            continue
        
        # URL completa del archivo
        file_url = base_url + file_name
        
        # Ruta completa de destino
        file_path = os.path.join(destination_folder, file_name)
        
        # Descargar el archivo en paralelo
        try:
            download_file(file_url, file_path, num_threads)
        except Exception as e:
            print(f"Error al descargar {file_name}: {e}")

# Función principal para procesar el archivo de enlaces
def process_urls_from_file(input_file, base_destination, num_threads=8):
    # Leer las URLs del archivo de texto
    with open(input_file, 'r') as f:
        urls = f.read().splitlines()
    
    # Procesar cada URL
    for url in urls:
        # Obtener el nombre de la última carpeta de la URL
        parsed_url = urlparse(url)
        path_segments = parsed_url.path.strip('/').split('/')
        folder_name = path_segments[-1] if path_segments else "default_folder"
        
        # Carpeta de destino para esta URL
        destination_folder = os.path.join(base_destination, folder_name)
        
        print(f"\nDescargando carpeta: {folder_name}")
        print(f"URL: {url}")
        print(f"Destino: {destination_folder}")
        
        # Descargar todos los archivos de la carpeta
        download_directory(url, destination_folder, num_threads)

# Configuración de argumentos de línea de comandos
def parse_arguments():
    parser = argparse.ArgumentParser(description="Descargar archivos desde URLs FTP en paralelo.")
    parser.add_argument("--input", "-i", required=True, help="Archivo de texto con las URLs a descargar.")
    parser.add_argument("--output", "-o", required=True, help="Carpeta base de destino para las descargas.")
    parser.add_argument("--threads", "-t", type=int, default=8, help="Número de hilos para descargas en paralelo (por defecto: 8).")
    return parser.parse_args()

# Punto de entrada del script
if __name__ == "__main__":
    # Parsear argumentos de la línea de comandos
    args = parse_arguments()
    
    # Procesar todas las URLs del archivo
    process_urls_from_file(args.input, args.output, args.threads)
    print("\n¡Descarga masiva completada!")
