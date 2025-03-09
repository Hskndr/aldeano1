# functions.py
'''
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import csv
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# Lista de cadenas (strings) a buscar en los enlaces
search_strings = [
    "CIF",
    "C.I.F.",
    "Código de Identificación Fiscal"
]

max_depth = 5  # Cambia esto al nivel máximo de profundidad que desees

# Módulo para encontrar enlaces en sitios y cadena de caracteres
def find_links_on_page(url, target_domain, search_strings, current_depth, max_depth, verified_matches, max_requests):
    print('No',url)

    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        matching_links = []
        print('xxx')
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
                       # print(f'{start_url} Encontrado "{search_string}" en {url} y coincidencia: "{match_value}". Coincidencia Nro {verified_matches}')
                        
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
'''

# Módulo para Rastrear Sitio y encontrar enlaces.
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

def crawl_website_recursive(url_company, target_domain):
    
    def extract_links_from_page(url, target_domain):
        try:
            print('✅  response')
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print('✅  ok')
            else:
                print('❌  Fail')
            soup = BeautifulSoup(response.text, 'html.parser')
            # Utilizamos un conjunto para mantener enlaces únicos
            links = set()  


            for link in soup.find_all('a'):
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    parsed_url = urlparse(absolute_url)
                    # Verificar si el dominio extraído contiene target_domain
                    if target_domain in parsed_url.netloc:
                        # Agregamos el enlace al conjunto links = set()  
                        links.add(absolute_url)  

            # Convertimos el conjunto en una lista antes de retornarlo
            return list(links)  
               
        except requests.exceptions.Timeout as e:
            print(f"❌  La solicitud ha excedido el tiempo de espera: {e}")
            return []
        except requests.exceptions.RequestException as e:
            print(f"❌  Error de solicitud: {e}")
            return []
        except Exception as e:
            print(f"❌  ** Error al extraer enlaces de {url}: {e}")
            return []
   
  
    # Iniciar el rastreo desde la URL de la empresa
    print('** crawl_website_recursive',url_company)
    visited_urls = set()
    matching_links_with_domain = extract_links_from_page(url_company, target_domain)
    
    # Imprimir los enlaces encontrados
    #for link in matching_links_with_domain:
    #   print(f'Enlace encontrado: {link}')

    return matching_links_with_domain

# Ejemplo de uso:
# matching_links = crawl_website_recursive(url_company, target_domain)



