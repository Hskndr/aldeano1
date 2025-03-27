import requests
from bs4 import BeautifulSoup
from CRW_001_utils import evaluate_match

def find_cif_on_google(name_company):
    print(f'⭐️ Inside find_cif_on_google{name_company}')
    try:
        # Concatenar el texto "España CIF" al nombre de la empresa
        url_find = f"https://www.google.com/search?q={name_company} Empresa España CIF"
        
        # Realizar una solicitud GET a la página de resultados
        response = requests.get(url_find)
        
        # Comprobar si la solicitud fue exitosa (código de estado 200)
        if response.status_code == 200:
            print(f'  ⭐️ Iniciando SOUP')
            # Parsear la página de resultados con BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Obtener todo el texto de la página
            page_text = soup.get_text()
            
            #FIXME - Aqui se produjo un error con la compania "Firefish Capital, SL"
            print(' ❤️‍🔥  page_text')
            
            # evaluear si tiene algun cif
            page_content_any_cif = evaluate_match(page_text)
            
            # Retornar el texto de la página
            return page_content_any_cif
        else:
            return False,None

    except Exception as e:
        return f"Error: {str(e)}"


