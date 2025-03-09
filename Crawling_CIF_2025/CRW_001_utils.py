import requests
import time
import random
from bs4 import BeautifulSoup
import re
import csv
import threading
from datetime import datetime
# utils.py

# Función para obtener la fecha y hora actual en formato amigable
def get_current_time():
    return datetime.now().strftime("%B %d, %Y %I:%M %p")

def prioritize_urls_with_keywords(urls, keywords):
    # Crear listas separadas para URLs que contienen y no contienen las palabras clave
    containing_keywords = []
    not_containing_keywords = []

    for url in urls:
        if any(keyword in url.lower() for keyword in keywords):
            containing_keywords.append(url)
        else:
            not_containing_keywords.append(url)

    # Reordenar las URL, poniendo primero las que contienen las palabras clave
    prioritized_urls = containing_keywords + not_containing_keywords

    return prioritized_urls

def evaluate_match(context_text):
    print(f'  ⭐️  Evaluating match in {context_text}')
    try:
        # Expresión regular para verificar el formato de un CIF español
        #cif_pattern = r'[ABCDEFGHJKLMNPQRSUVW]{1}\d{7}[0-9A-J]{1}'
        cif_pattern = r'[ABCDEFGHJKLMNPQRSUVW]{1}[-\s]?\d{7}[0-9A-J]{1}'

        # Buscar coincidencias que cumplan con el formato de CIF
        cif_matches = re.search(cif_pattern, context_text, re.IGNORECASE)
        
        # Verificar si se encontraron coincidencias y si alguna coincide con el formato de CIF
        if cif_matches:
            match_value = cif_matches.group(0)
            
            return True, match_value.upper()  # Se encontró al menos una coincidencia de CIF
        else:
            return False, None  # No se encontraron coincidencias de CIF
    except Exception as e:
        print(f"Error al evaluar la coincidencia: {e}")
# utils.py

def search_strings_in_links(links, search_strings, excluded_words):
    # Inicialmente, no se ha encontrado ninguna coincidencia CIF
    cif_context = None  
    
    # Inicializar el contador de coincidencias CIF
    cif_counter = 0  
    for iteration_number, link in enumerate(links, 1):  # El segundo argumento de enumerate() es el valor inicial del contador (1 en este caso)
        size_of_links = len(links)
         
        try:
            # Agregar un retraso aleatorio en segundos
            # Verificar si iteration_number es un múltiplo de 20
            if iteration_number % 20 == 0:
                delay = random.uniform(10, 20)
                time.sleep(delay)
            else:
                delay = random.uniform(3, 7)
                time.sleep(delay)
            
            # Ingresar al Enlace
            print(f'*** Try in {link} waiting {delay} {iteration_number}/{size_of_links}')
            response = requests.get(link, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
            
                # Obtener el contenido de la página como texto
                page_text = soup.get_text()
                page_content = page_text.lower()  # Convertir a minúsculas para la búsqueda
                
                # Verificar que ninguna de las palabras excluidas esté presente en el contenido
                if not any(word.lower() in page_content for word in excluded_words):
                    # Buscar los strings de búsqueda en el contenido de la página
                    for search_string in search_strings:
                        if search_string.lower() in page_content:
                            # Encontrar la posición del CIF en el contenido
                            index = page_content.lower().find(search_string.lower())
                            if index != -1:
                                start_index = max(0, index - 20)  # Comenzar 20 caracteres antes de la coincidencia
                                end_index = min(len(page_content), index + len(search_string) + 20)  # Terminar 20 caracteres después de la coincidencia
                                context_text = page_content[start_index:end_index]
                                    
                                # Evaluar CIF
                                is_context_text_valid = evaluate_match(context_text)
                                
                                # Incrementar el contador si se encuentra un CIF válido
                                if is_context_text_valid[0]:
                                    cif_counter += 1
                                    cif_context = (link, is_context_text_valid[1])  # Almacenar el contexto y el enlace
                                    
                                # Imprimir y agregar el enlace con el contexto
                                print('*** Found CIF:', is_context_text_valid[1],{get_current_time()})
                        
                    # Verificar si cif_counter es igual a 1 y salir del bucle
                    if cif_counter == 1:
                        break
            
            if iteration_number == 25:
                break            
        except Exception as e:
            print(f"*** Error al buscar enlaces en {link}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error de solicitud: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")
        
    # Imprimir el total de coincidencias CIF encontradas   
    print(f'*** Total de coincidencias CIF encontradas: {cif_counter} {get_current_time()}')

    return cif_context  # Retornar el contexto y el enlace cuando cif_counter sea igual a 1clear



def save_cif_info(id_company, name_company, url_company, cif, link):
    # Definir el nombre del archivo CSV
    csv_filename = "cif_founded.csv"

    # Crear un diccionario con la información
    cif_info = {
        'Empresa_ID': id_company,
        'Empresa': name_company,
        'website': url_company,
        'CIF': cif,
        'enlace CIF': link
    }

    # Abrir el archivo CSV en modo de escritura y agregar la información
    with open(csv_filename, mode='a', newline='') as csv_file:
        fieldnames = ['Empresa_ID', 'Empresa', 'website', 'CIF', 'enlace CIF']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Si el archivo está vacío, escribir la cabecera
        if csv_file.tell() == 0:
            writer.writeheader()

        # Escribir la información en el archivo CSV
        writer.writerow(cif_info)

