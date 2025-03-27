from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import random
from CRW_001_utils import evaluate_match
from bs4 import BeautifulSoup
from datetime import datetime


# Función para obtener la fecha y hora actual en formato amigable
def get_current_time():
    return datetime.now().strftime("%B %d, %Y %I:%M %p")


def find_cif_with_selenium(company_name, driver):
    """ Busca el CIF en Google usando Selenium con un WebDriver ya inicializado. """
    print(f"🔍 Buscando {company_name} en Google con Selenium")
    
    try:
        driver.get("https://www.google.com")
        
        search_box = driver.find_element(By.NAME, "q")
        search_term = f"Cuál es el CIF de la empresa {company_name} en España?"

        # Simular la escritura más lenta
        for char in search_term:
            search_box.send_keys(char)
            time.sleep(random.uniform(0.12, 0.26))  # Retraso aleatorio entre 0.1 y 0.3 segundos
        
        search_box.send_keys(Keys.RETURN)
        print("Pausando..2 a 6 seg")
        time.sleep(random.uniform(2, 6))
    
        print("Buscando Resultados")
        
         # Obtener el HTML de la página después de la carga
        page_html = driver.page_source
        print("Pagina obtenida",{get_current_time()})
        print()
        
        # Parsear el HTML con BeautifulSoup
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Obtener todo el texto de la página
        page_text = soup.get_text()
                   
         # Evaluar si el HTML tiene algún CIF
        page_content_any_cif = evaluate_match(page_text)
        
        if page_content_any_cif:
            return page_content_any_cif 
        else:
            return False, None
    except Exception as e:
        print(f"⚠️ Error con Selenium: {e}")
    
    
