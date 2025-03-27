import tldextract
import pandas as pd
import random
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from CRW_001_functions import crawl_website_recursive
from CRW_001_read_pandas import read_urls_from_csv
from CRW_001_utils import prioritize_urls_with_keywords, search_strings_in_links, save_cif_info
from CRW_001_find_with_selenium import find_cif_with_selenium

def load_company_data(csv_filename):
    print("🟢  Iniciando el proceso de búsqueda de CIF en sitios web...")
    df_companies = read_urls_from_csv(csv_filename)
    print("🟢  Archivo CSV leído exitosamente")
    return df_companies

def load_processed_ids(cif_founded_filename):
    print("🟢  Cargando IDs de empresas procesadas...")
    print(os.path.exists(cif_founded_filename))
    try:
        if os.path.exists(cif_founded_filename):
            df_cif_founded = pd.read_csv(cif_founded_filename, encoding='latin-1')
            
            return df_cif_founded[['Empresa_ID']].drop_duplicates()
        return pd.DataFrame(columns=['Empresa_ID'])
    except Exception as e:
        print(f"Error en load_processed_ids: {e}")

def process_company(row, df_processed_ids, iteration_count, max_iterations, driver):
    """ Procesa una empresa buscando su CIF con Selenium. """
    url_company = row['website']
    id_company = row['Empresa_ID']
    name_company = row['Empresa']
    processed_company=False
    
    if id_company in df_processed_ids.values:
        print(f'ID {id_company} ya procesada. Saltando...')
        processed_company=True
        return iteration_count,processed_company
    
    print(f'* Buscando {name_company} en Google ({iteration_count}/{max_iterations})...')
    search_on_google = find_cif_with_selenium(name_company, driver)
    
    if search_on_google[0]:
        cif_founded = search_on_google[1]
        link_cif_founded = 'www.google.com'
    else:
        #cif_founded, link_cif_founded = process_crawling(url_company, id_company, name_company, iteration_count, max_iterations)
        cif_founded, link_cif_founded = None, None
    save_cif_info(int(id_company), name_company, url_company, cif_founded, link_cif_founded)
    
    time.sleep(random.uniform(5, 15))  # Pausa entre búsquedas para evitar bloqueos
    
    return iteration_count + 1,processed_company

def process_crawling(url_company, id_company, name_company, iteration_count, max_iterations):
    extracted = tldextract.extract(url_company)
    target_domain = f"{extracted.domain}.{extracted.suffix}"
    
    print(f'* Iniciando Crawling - ID: {id_company} - Empresa: {name_company}')
    
    crawling_links = crawl_website_recursive(url_company, target_domain)
    time.sleep(random.uniform(0, 1))
    
    keywords_to_prioritize = ['politica', 'legal','/es/']
    crawling_links = prioritize_urls_with_keywords(crawling_links, keywords_to_prioritize)
    
    search_strings_in_website = ["CIF", "C.I.F.", "Código de Identificación Fiscal"]
    excluded_words = ["cifra", "cifras", "CIFRAS", "cifrado"]
    
    matching_links_with_strings = search_strings_in_links(crawling_links, search_strings_in_website, excluded_words)
    
    if matching_links_with_strings:
        return matching_links_with_strings[1], matching_links_with_strings[0]
    return 'None', 'None'

def main():
    csv_filename = "src_companies_for_CIF.csv"
    cif_founded_filename = "cif_founded.csv"
    
    df_companies = load_company_data(csv_filename)
    df_processed_ids = load_processed_ids(cif_founded_filename)
    
    max_iterations = df_companies.shape[0]
    iteration_count = 0
    
    print("\n🟢  Iniciando Selenium WebDriver...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=False")  # Sin interfaz gráfica
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")  # Evitar detección de WebDriver
    
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://www.google.com")
    
    try:
        print("🟢  Iniciando iteración...")
        input("🔵 Presiona Enter después de aceptar los términos en Google...")
        for _, row in df_companies.iterrows():
            if iteration_count >= max_iterations:
                break
            
            iteration_count,processed_company = process_company(row, df_processed_ids, iteration_count, max_iterations, driver)
            if not processed_company:
                print("Pausado entre empresas..5 a 10 seg")
                time.sleep(random.uniform(5, 10))

                # **Cada 50 iteraciones, hacer una pausa de 90 segundos**
            if iteration_count % 50 == 0 and iteration_count > 0:
                print(f"🔵 Pausa larga de 90 segundos después de {iteration_count} iteraciones...")
                time.sleep(90)
    
    finally:
        print("\n🔴  Cerrando Selenium WebDriver...")
        driver.quit()
    
    print("🔴  Proceso de búsqueda de CIF en sitios web finalizado exitosamente")

if __name__ == "__main__":
    main()