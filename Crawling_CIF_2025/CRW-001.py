"""
Ruta de Ejecución:
cd HiskanderTools/Python/Crawling_v0
python CRW-001.py
python CRW-001.py
"""
"""
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import tldextract
import re
import csv
import pandas as pd  # Importar Pandas
import time
import random  # Importar el módulo random
from concurrent.futures import ThreadPoolExecutor, TimeoutError


# Las funciones find_links_on_page, evaluate_match y crawl_website_recursive permanecen sin cambios

# Módulo para encontrar enlaces en sitios y cadena de caracteres
def find_links_on_page(url, target_domain, search_strings, current_depth, max_depth, verified_matches, max_requests):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        matching_links = []

        for search_string in search_strings:
            excluded_words = ["cifra", "cifras", "CIFRAS", "cifrado"]  # Agrega aquí las palabras que no deben considerarse coincidencias

            page_text = soup.get_text()  # Obtener el contenido de la página como texto
            index = page_text.lower().find(search_string.lower())
            if index != -1:
                start_index = max(0, index - 20)  # Comenzar 20 caracteres antes de la coincidencia
                end_index = min(len(page_text), index + len(search_string) + 20)  # Terminar 20 caracteres después de la coincidencia
                context_text = page_text[start_index:end_index]

                # Verificar si la coincidencia no es una palabra excluida
                is_excluded = any(excluded_word in context_text.lower() for excluded_word in excluded_words)
                if not is_excluded:
                    # Evaluar la coincidencia y contarla si cumple con un patrón
                    is_match, match_value = evaluate_match(search_string, context_text)
                    if is_match:
                        verified_matches += 1
                        print(f'{start_url} Encontrado "{search_string}" en {url} y coincidencia: "{match_value}". Coincidencia Nro {verified_matches}')
                        
                        output_csv_filename = 'cif_founded.csv'
                         # Guardar el valor de match_value en el archivo CSV
                        with open(output_csv_filename, mode='a', newline='') as csv_file:
                            fieldnames = ['URL', 'Search String', 'Match Value']
                            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                            # Si el archivo no existe, escribir la cabecera
                            if not csv_file.tell():
                                writer.writeheader()

                            # Escribir la fila con los datos
                            writer.writerow({'URL': url, 'Search String': search_string, 'Match Value': match_value})

        for link in soup.find_all('a'):
            href = link.get('href')
            if href:
                absolute_url = urljoin(url, href)
                parsed_url = urlparse(absolute_url)
                # Verificar si el dominio extraído contiene target_domain
                if target_domain in parsed_url.netloc:
                    matching_links.append(absolute_url)

        return matching_links, len(matching_links), verified_matches
    except Exception as e:
        print(f"Error al acceder a {url}: {e}")
        return [], 0, verified_matches

# Evaluar coincidencia buscando patrones similares


def evaluate_match(search_string, context_text):
    # Expresión regular para verificar el formato de un CIF español
    cif_pattern = r'[ABCDEFGHJKLMNPQRSUVW]{1}\d{7}[0-9A-J]{1}'

    # Buscar coincidencias que cumplan con el formato de CIF
    cif_matches = re.search(cif_pattern, context_text, re.IGNORECASE)
    
    # Verificar si se encontraron coincidencias y si alguna coincide con el formato de CIF
    if cif_matches:
        match_value = cif_matches.group(0)
        print("Si es cif")
        return True, match_value  # Se encontró al menos una coincidencia de CIF
    else:
        return False, None  # No se encontraron coincidencias de CIF


# Módulo para Rastrear Sitio y encontrar enlaces.


def crawl_website_recursive(url, target_domain, search_strings, max_depth, current_depth=1, max_requests=35):
    # Print Operativo
    print(f'Ejecutando la función crawl_website_recursive para: {url}')
    if current_depth > max_depth:
        return []

    visited_urls = set()
    to_visit = [(url, current_depth)]
    matching_links = []
    verified_matches = 0
    request_counter = 0

    with ThreadPoolExecutor() as executor:
        elapsed_time_iteration = 0
        while to_visit and (verified_matches < 1 ) and request_counter <= max_requests and elapsed_time_iteration<35:
            print(f'TV: {to_visit}, CIF Ver:{verified_matches}, Request:{request_counter} timeReq:{elapsed_time_iteration}')
            #print(f'Print Operativo1')
    
            start_time_iteration = time.time()
            url, current_depth = to_visit.pop()
            visited_urls.add(url)
            #print(f'Print Operativo2')
            # Esta función llama a la función find_links_on_page
            def process_url(url, verified_matches):
                #print(f'Print Operativo3')
                links_on_page, num_links, verified_matches = find_links_on_page(url, target_domain, search_strings, current_depth, max_depth, verified_matches, max_requests)
                return links_on_page

            try:
                links_on_page = executor.submit(process_url, url, verified_matches).result(timeout=35)
               # print(f'Print Operativo4')
            except TimeoutError:
                #print(f'Print Operativo5')
                print(f"La iteración para {url} superó el límite de tiempo de 35 segundos, continuando con la siguiente iteración.")
                print(start_time_iteration)
                elapsed_time_iteration = time.time() - start_time_iteration
                print(elapsed_time_iteration)
                continue  # Continuar con la siguiente iteración si la actual tarda más de 35 segundos
            #print(f'Print Operativo6')
            elapsed_time_iteration = time.time() - start_time_iteration
           # print(f'Print Operativo7')
            if elapsed_time_iteration > 35:
                print(f'Print Operativo8')
                print(f"La iteración para {url} tomó demasiado tiempo ({elapsed_time_iteration} segundos), continuando con la siguiente iteración.")
                continue  # Continuar con la siguiente iteración si la actual tarda más de 35 segundos
          # print(f'Print Operativo9')
            # Estos son los enlaces que coinciden con el url y el dominio.
            matching_links.extend(links_on_page)
          #  print(f'Print Operativo10')
            # Sistema para temporizar y pausar las iteraciones
            if request_counter % 15 == 0:
               # print(f'Print Operativo11')
                random_delay_Long = random.uniform(13, 15)
                print(f"Haciendo una pausa de {random_delay_Long} segundos... en request{request_counter}")
                time.sleep(random_delay_Long)
            else:
               # print(f'Print Operativo12')
                random_delay = random.uniform(5, 8)
                print(f"Esperando {random_delay} segundos... en request: {request_counter}")
                time.sleep(random_delay)

            for link in links_on_page:
                #print(f'Print Operativo13')
                if link not in visited_urls and current_depth < max_depth:
                    #print(f'Print Operativo14')
                    to_visit.append((link, current_depth + 1))

           # print(f'Print Operativo15')
            request_counter += 1
   # print(f'Print Operativo16')
    return matching_links, verified_matches


# ... (resto del código sin cambios)

# Función para leer las URLs desde un archivo CSV utilizando Pandas
def read_urls_from_csv(filename):
    try:
        df = pd.read_csv(filename)
        urls = df['URL'].tolist()
        return urls
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return []


# INICIO
# Nombre del archivo CSV que contiene las URLs
csv_filename = "urls.csv"

# Leer las URLs desde el archivo CSV ejecuta la función read_urls_from_csv
urls = read_urls_from_csv(csv_filename)

# Lista de cadenas (strings) a buscar en los enlaces
search_strings = [
    "CIF",
    "C.I.F.",
    "Código de Identificación Fiscal"
]

# Nivel de profundida de las url's hijas
max_depth = 5  

# Mecanismo para recorrer las urls del archivo
for start_url in urls:
    # Extraer el dominio del sitio web
    extracted = tldextract.extract(start_url)
    target_domain = f"{extracted.domain}.{extracted.suffix}"
    
    # Print Operativo
    print(f'Inicio Crawling para {start_url}: {target_domain}')
    
    # Ejecuta la función crawl_website_recursive 
    matching_links = crawl_website_recursive(start_url, target_domain, search_strings, max_depth)
    #print('matching_links')
    #print(matching_links)

    # Print Operativo
    print(f'matching links Culminado para {start_url}: {len(matching_links)}')
    
    if matching_links:
        print("Enlaces asociados al dominio:")
        #for link in matching_links:
        #    print(link)
    else:
        print(f"No se encontraron enlaces asociados al dominio para {start_url}.")

"""

# main.py
import tldextract
import pandas as pd
import random
import time
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from CRW_001_find_google import find_cif_on_google
from CRW_001_functions import crawl_website_recursive
from CRW_001_read_pandas import  read_urls_from_csv
from CRW_001_utils import prioritize_urls_with_keywords, search_strings_in_links,save_cif_info
import datetime

print("🟢  Iniciando el proceso de búsqueda de CIF en sitios web...")
# Nombre del archivo Fuente, con la información
csv_filename = "src_companies_for_CIF.csv"

# Leer las URLs desde el archivo CSV utilizando la función read_urls_from_csv
df_companies = read_urls_from_csv(csv_filename)

print("🟢  Archivo CSV leído exitosamente")
print()
print("🔽" * 25)
print()

# Establecer el número máximo de iteraciones deseado
#max_iterations = 10  # Cambia este valor al número deseado
max_iterations = num_rows = df_companies.shape[0]

# Inicializar un contador de iteraciones
iteration_count = 0

## Sistema para detectar que no se haya procesado antes una empresa
# Cargar cif_founded.csv en un DataFrame si existe
cif_founded_filename = 'cif_founded.csv'

if os.path.exists(cif_founded_filename):
    df_cif_founded = pd.read_csv(cif_founded_filename)
    df_processed_ids = df_cif_founded[['Empresa_ID']].drop_duplicates()

 ## Sistema para detectar que no se haya procesado antes una empresa   
print("🟢  Iniciando el la iteración...")
for _, row in df_companies.iterrows():

    # Verificar si se ha alcanzado el número máximo de iteraciones
    if iteration_count >= max_iterations:
        break
    
    url_company = row['website']
    id_company = row['Empresa_ID']
    name_company = row['Empresa']

    ## Sistema para detectar que no se haya procesado antes una empresa
    # Verifica si la id_company ya ha sido procesada
    this_id_in_df_processed_ids = id_company in df_processed_ids.values
    if this_id_in_df_processed_ids:
        print(f'ID {id_company} ya procesada. Saltando...')
        continue 

    ## Sistema para detectar que no se haya procesado antes una empresa
    
    # Sistema para buscar en google
    print(f'* Looking {name_company} on google company nro. {iteration_count}/{max_iterations}')
    search_on_google = find_cif_on_google(name_company)
    
    if search_on_google[0]:  # Si search_on_google[0] es True
        cif_founded = search_on_google[1]
        link_cif_founded = 'www.google.com'
    else:
        # SI search_on_google es False
        # Extraer el dominio del sitio web
        extracted = tldextract.extract(url_company)
        target_domain = f"{extracted.domain}.{extracted.suffix}"
        
        # Capturar la fecha y hora actual
        nowLink = datetime.datetime.now()
        # Print Operativo
        print(f'* Starting Crawling - ID: {id_company} -Company: {name_company} web: {url_company} Dom: {target_domain}. {iteration_count}/{max_iterations} at {nowLink}')
        
        # Llamar a crawl_website_recursive con url_company como parámetro
        crawling_links = crawl_website_recursive(url_company, target_domain)
        
        # Agregar un retraso aleatorio en segundos
        delay = random.uniform(0, 1)
        time.sleep(delay)
        
        # Priorizar las URL con palabras clave 'politica' y 'legal'
        keywords_to_prioritize = ['politica', 'legal','/es/']
        crawling_links = prioritize_urls_with_keywords(crawling_links, keywords_to_prioritize)
        
        # Buscar los strings en los enlaces
        search_strings_in_website = [
            "CIF",
            "C.I.F.",
            "Código de Identificación Fiscal"
        ]
        
        # Listado de palabras a Excluir
        excluded_words = [
            "cifra",
            "cifras",
            "CIFRAS",
            "cifrado"
        ]
        
        matching_links_with_strings = search_strings_in_links(crawling_links, search_strings_in_website,excluded_words)

    # Después de llamar a search_strings_in_links
        if matching_links_with_strings is not None:
            cif_founded = matching_links_with_strings[1]
            link_cif_founded = matching_links_with_strings[0]
        else:
            cif_founded = 'None'
            link_cif_founded = 'None'
        # SI search_on_google es False
  # Invocar la función para guardar la información en el archivo cif_founded.csv
    save_cif_info(int(id_company), name_company, url_company, cif_founded, link_cif_founded)
    
    if search_on_google[0]:  # Si search_on_google[0] es True
        delay = random.uniform(5, 15)
        time.sleep(delay)
        
    # Incrementar el contador de iteraciones
    iteration_count += 1
print("🔴  Proceso de búsqueda de CIF en sitios web finalizado exitosamente")