import pandas as pd
import mysql.connector
from rapidfuzz import fuzz, process

# Conexión a la base de datos MySQL
def connect_to_db():
    return mysql.connector.connect(
        host="3.88.128.188",          # Cambiar si necesario
        user="ciudadrenovablec_erp_usr",         # Cambiar por el usuario de tu base de datos
        password="fJPh,o6Xo]!L",  # Cambiar por tu contraseña
        database="ciudadrenovablec_erp"
    )

# Obtener datos de la tabla 'projects'
def fetch_projects_from_db():
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, name, code FROM projects")
    projects = cursor.fetchall()
    cursor.close()
    conn.close()
    return projects

# Asignar project_id y code basados en coincidencias aproximadas
def assign_project_ids_and_codes(dim_cc_list, projects):
    project_df = pd.DataFrame(projects)  # Convertir la lista de resultados a DataFrame
    project_ids, codes = [], []
    
    for dim in dim_cc_list:
        match = process.extractOne(dim, project_df['name'], scorer=fuzz.token_sort_ratio)
        if match and match[1] >= 80:  # Umbral de similitud, ajustable
            best_match = project_df.loc[project_df['name'] == match[0]]
            project_ids.append(best_match['id'].values[0])
            codes.append(best_match['code'].values[0])
        else:
            project_ids.append(311)  # ID por defecto
            codes.append(None)  # Code por defecto (si aplica)
    
    return project_ids, codes

# Define the cost_dict set with ".0" added to each entry
cost_dict = {f'{code}.0' for code in [
    '11200501', '11051001', '11051002', '11051003', '13809501', '14350101', '14350102', '24081001',
    '61350503', '61800102', '73050502', '73050503', '73050504', '73050509', '73050510', '73050512',
    '73202001', '73550501', '73953505', '73956005', '22050501', '425050'
]}

def main():
    # Leer el archivo Excel
    df = pd.read_excel('Comprobantes detallados.xlsx', sheet_name='Sheet1', skiprows=7)
    
    # Eliminar filas donde la columna 'Sucursal' sea NaN
    df = df.dropna(subset=['Sucursal'])
    
    # Asegurar que 'Código contable' sea string y eliminar espacios
    df['Código contable'] = df['Código contable'].astype(str).str.strip()
    
    # Filtrar el DataFrame según el conjunto `cost_dict`
    df_filtered = df[df['Código contable'].isin(cost_dict)]
    
    # Agrupar por 'Centro de costo'
    grouped = df_filtered.groupby('Centro de costo')
    
    # Procesar cada grupo
    for name, group in grouped:
        credito_rows = group[(group['Código contable'] == '61350503.0') & (group['Crédito'] != 0)]
        
        if not credito_rows.empty:
            for index, credito_row in credito_rows.iterrows():
                credito_value = credito_row['Crédito']
                debito_rows = group[(group['Débito'] >= credito_value) & (group['Débito'] != 0)]
                
                if not debito_rows.empty:
                    debito_index = debito_rows.index[0]
                    df_filtered.at[debito_index, 'Débito'] -= credito_value
                    df_filtered.at[index, 'Crédito'] = 0

    # Reiniciar el índice del DataFrame filtrado
    df_filtered.reset_index(drop=True, inplace=True)
    
    # Lista de Dim_CC proporcionada
    dim_cc_list = [
        "Administrativo", "Edificio Green", "Tronex Cipa", "FANALCA", "NU3", "Tiendas Ara Montería",
        "Mondrian", "Navitrans Itagüí", "Proyectos", "Samuel Gaviria", "Alta Campiña", "TDM",
        "Tienda Ara", "Ignacio Duque", "Andrés Julián Rendón", "Coonorte", "Luis Arango", "Comercial",
        "Aserrío La Autopista", "Juan Carlos Gómez", "Luis Miguel Toro", "Navitrans Aguacatala",
        "Planta Agris", "Sergio Ramírez", "José Saenz", "Luz María Vasquez", "Celsia Luz María Velasquez",
        "Repostería Migú", "Pablo Palacio", "Diax", "Juan Guillermo Osorio", "Samiplast", "Beatriz Hoyos",
        "Raul Mejía", "Ganados y Porcinos", "Oscar Restrepo", "Monserrate Medellin", "Hacienda Capiro",
        "Jaime Vallejo", "Navitrans Montería", "Navitrans Soledad", "Santiago Vélez", "Juanito Laguna",
        "Tejidos 2A", "Triturados Peñalisa", "Navitrans Américas", "Navitrans Tintalito", "Ana Duque",
        "Pitriza", "Sergio Lalinde", "Foresta", "Carlos Isaza", "Luis Felipe Vélez", "Felipe García",
        "Pablo Lara", "Vitelsa SA", "Dario Vargas", "Catalina Jaimes", "Contabler", "La Rufina",
        "Yokomotor SA", "Raúl Sánchez", "NN", "Ángela Giraldo", "Ricardo Sosa", "Electrocontrol Haceb",
        "Della Nonna", "CDA Autofull", "Bernardo Vieco", "Mario Restrepo", "Jaime Moreno", "Mateo Restrepo",
        "Rodrigo Isaza", "Brezzo Forest", "Juan José Mesa", "Gustavo Pastrana", "Luis Javier Arango",
        "Mantenimiento", "Clara Ferrer", "Luis Martinez", "Pasteur CEDI", "Terminal Caucasia",
        "Mauricio Molina", "Mauricio Duque", "Diego Ramírez", "Montacargas Master", "Sunvolt Luis Felipe Toro",
        "Jose Buitrago", "Maria Gilma", "Juan Carlos Guzmán", "Mauricio Arango", "Hotel Las Olas",
        "Casa Distracom", "Parcelación Montecapiro", "Jacobo Uribe", "Surtitodo", "Victoria Arango",
        "Namasté Holdings", "Puerta del Norte", "Maryori Mesa", "Carlos García", "Carlos Gómez",
        "Navitrans Duitama", "Monserrat Lugo", "Diana Cañola", "Jose Gómez", "Atlético Nacional",
        "Clara Pineda", "Carlos Gomez El retiro", "Hacienda San Julian", "Ed castropol señorial",
        "Paula Restrepo", "EDS Zeuss San Francisco", "Nuestro Urabá Apartadó", 
        "Politecnico Jaime Isaza Cadavid", "Juan Rodrigo Toro", "Camilo Londoño", "Ángela Gómez",
        "Juan Carlos Ochoa", "Óscar Echeverri", "Fundación Argos", "Celsia Leonor Villegas",
        "MGM Nuestro Urabá Apartadó", "Discom", "Andrés Saldarriaga", "Juan D. Hoyos", "Hilda Crespo",
        "Rodrigo Herrera", "Fernando Reyes", "Mundial GYP", "Isabel Trujillo", "Nancy Zapata",
        "Raul Sanchez", "Juan Velasquez"
    ]
    
    # Obtener datos de la tabla 'projects'
    projects = fetch_projects_from_db()
    
    # Asignar project_id y code basados en coincidencias aproximadas
    project_ids, codes = assign_project_ids_and_codes(dim_cc_list, projects)
    
    # Crear el DataFrame Dim_CC con las columnas adicionales
    df_dim_cc = pd.DataFrame({
        "Dim_CC": dim_cc_list,
        "id": project_ids,
        "code": codes
    })
    
    # Guardar el DataFrame en una hoja Excel
    with pd.ExcelWriter('cost_db.xlsx', engine='xlsxwriter') as writer:
        df_filtered.to_excel(writer, sheet_name='cost', index=False)  # Hoja original con df_filtered
        df_dim_cc.to_excel(writer, sheet_name='Dim_CC', index=False)  # Nueva hoja con Dim_CC
    
    print("Archivo Excel generado con los datos de Dim_CC y columnas adicionales.")

if __name__ == "__main__":
    main()

