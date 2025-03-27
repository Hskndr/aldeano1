# Función para leer las URLs desde un archivo CSV utilizando Pandas

import pandas as pd

# Función para procesar archivos de contactos
def process_contact_file(df):
    print("    💚  Es un archivo de contactos")
    
    # Crear tres DataFrames temporales, pero manejar la posibilidad de que las columnas no estén presentes
    df1 = process_company_columns(df, 'ID_Empresa1', 'Empresa_ID_Empresa1', 'website_ID_Empresa1')
    df2 = process_company_columns(df, 'ID_Empresa2', 'Empresa_ID_Empresa2', 'website_ID_Empresa2')
    df3 = process_company_columns(df, 'ID_Empresa3', 'Empresa_ID_Empresa3', 'website_ID_Empresa3')

    # Concatenar los DataFrames en uno solo
    result_df = pd.concat([df1, df2, df3], ignore_index=True)

    # Filtrar solo las filas donde el campo 'website' no esté vacío
    result_df = result_df[result_df['website'].notna()]

    # Eliminar duplicados basados en el campo 'ID_Empresa'
    result_df.drop_duplicates(subset=['Empresa_ID'], keep='first', inplace=True)

    return result_df

# Función para procesar archivos de empresas
def process_company_file(df):
    print("    💚  No es un archivo de contactos, es un archivo de Empresas")
    try:
        result_df = df[['Empresa_ID', 'Empresa', 'website']]     
        return result_df
    except KeyError as e:
        print(f"    🚨  Error: Faltan columnas necesarias en el archivo de empresas. {e}")
        return None

# Función para manejar las columnas de las empresas
def process_company_columns(df, id_column, empresa_column, website_column):
    if id_column in df.columns and empresa_column in df.columns and website_column in df.columns:
        df_temp = df[[id_column, empresa_column, website_column]].copy()
        df_temp.columns = ['Empresa_ID', 'Empresa', 'website']
        return df_temp
    else:
        return pd.DataFrame(columns=['Empresa_ID', 'Empresa', 'website'])


import pandas as pd

def clean_non_bmp(text):
    """ Elimina caracteres fuera del Basic Multilingual Plane (BMP) """
    return ''.join(c for c in text if c <= '\uFFFF')

def read_urls_from_csv(filename):
    print(f"  💚  Leyendo archivo CSV: {filename}")
    try:
        # Leer el archivo CSV con encoding forzado y manejo de errores
        df = pd.read_csv(filename, encoding="latin-1")
        print(f"  🔍 DataFrame original tiene {len(df)} filas y {len(df.columns)} columnas")

        # Limpiar nombres de columnas eliminando espacios en blanco
        df.columns = df.columns.str.strip()
        print(df)
        # Comprobar que df no esté vacío
        if df.empty:
            print("    💚  El DataFrame está vacío")
            return None
        else:
            print("    💚  El DataFrame no está vacío")

            # Limpiar datos (eliminar caracteres fuera del BMP en todas las columnas de tipo texto)
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).apply(clean_non_bmp)

            # Comprobar si es un archivo de contactos o empresas
            if 'Contacto_ID' in df.columns or 'Member_id' in df.columns:
                return process_contact_file(df)
            else:
                return process_company_file(df)

    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None
    
'''
   
# Función principal para leer el archivo CSV
def read_urls_from_csv(filename):
    print(f"  💚  Leyendo archivo CSV: {filename}")
    try:
        # Leer el archivo CSV
        df = pd.read_csv(filename)
        print(f"  🔍 DataFrame original tiene {len(df)} filas y {len(df.columns)} columnas")
        
        # Limpiar nombres de columnas eliminando espacios en blanco al inicio y al final
        df.columns = df.columns.str.strip()

        # Comprobar que df no esté vacío
        if df.empty:
            print("    💚  El DataFrame está vacío")
            return None
        else:
            print("    💚  El DataFrame no está vacío")

            # Comprobar si es un archivo de contactos
            if 'Contacto_ID' in df.columns or 'Member_id' in df.columns:
                return process_contact_file(df)
            else:
                return process_company_file(df)

    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None
'''

'''
import pandas as pd

def read_urls_from_csv(filename):
    print(f"  💚  Leyendo archivo CSV: {filename}")
    try:
        # Leer el archivo CSV
        df = pd.read_csv(filename)
        print(f"  🔍 DataFrame original tiene {len(df)} filas y {len(df.columns)} columnas")
        
        # Limpiar nombres de columnas eliminando espacios en blanco al inicio y al final
        df.columns = df.columns.str.strip()
        # Comprobar que df no esté vacío?
        if df.empty:
            print("    💚  El DataFrame está vacío")
        else:
            print("    💚  El DataFrame no está vacío")
            
            # Comprobar qe es un archivo de contactos
            if 'Contacto_ID' in df.columns or 'Member_id' in df.columns:
                print("    💚  Es un archivo de contactos")
                
                 # Crear tres DataFrames temporales, pero manejar la posibilidad de que las columnas no estén presentes
                if 'ID_Empresa1' in df.columns and 'Empresa_ID_Empresa1' in df.columns and 'website_ID_Empresa1' in df.columns:
                    df1 = df[['ID_Empresa1', 'Empresa_ID_Empresa1', 'website_ID_Empresa1']].copy()
                    print('   ⚠️  ID_Empresa1 completo:')
                    print(df1['ID_Empresa1'].values)
                    print('   ⚠️  ID_Empresa1 completo:')
                    df1.columns = ['Empresa_ID', 'Empresa', 'website']
                else:
                    df1 = pd.DataFrame(columns=['Empresa_ID', 'Empresa', 'website'])

                if 'ID_Empresa2' in df.columns and 'Empresa_ID_Empresa2' in df.columns and 'website_ID_Empresa2' in df.columns:
                    df2 = df[['ID_Empresa2', 'Empresa_ID_Empresa2', 'website_ID_Empresa2']].copy()
                    df2.columns = ['Empresa_ID', 'Empresa', 'website']
                    print(df2)
                else:
                    df2 = pd.DataFrame(columns=['Empresa_ID', 'Empresa', 'website'])

                if 'ID_Empresa3' in df.columns and 'Empresa_ID_Empresa3' in df.columns and 'website_ID_Empresa3' in df.columns:
                    df3 = df[['ID_Empresa3', 'Empresa_ID_Empresa3', 'website_ID_Empresa3']].copy()
                    df3.columns = ['Empresa_ID', 'Empresa', 'website']
                    print(df3)
                else:
                    df3 = pd.DataFrame(columns=['Empresa_ID', 'Empresa', 'website'])
                
                # Concatenar los DataFrames en uno solo
                result_df = pd.concat([df1, df2, df3], ignore_index=True)
                
                # Filtrar solo las filas donde el campo 'website' no esté vacío
                result_df = result_df[result_df['website'].notna()]
                
                # Eliminar duplicados basados en el campo 'ID_Empresa'
                result_df.drop_duplicates(subset=['Empresa_ID'], keep='first', inplace=True)

                
            else:
                print("    💚  No es un archivo de contactos es un archivo de Empresas") 
                print(df)    
                result_df = df[['Empresa_ID', 'Empresa', 'website']]     
                
        print(f"  ❤️   Leido archivo CSV: {filename}")
        return result_df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None


# Ejemplo de uso:
# df_combined = read_urls_from_csv('archivo.csv')
# print(df_combined)

'''