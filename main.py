import pandas as pd

# Define the cost_dict set with ".0" added to each entry
cost_dict = {f'{code}.0' for code in [
    '11200501', '11051001', '11051002', '11051003', '13809501', '14350101', '14350102', '24081001',
    '61350503', '61800102', '73050502', '73050503', '73050504', '73050509', '73050510', '73050512',
    '73202001', '73550501', '73953505', '73956005', '22050501', '425050'
]}

def main():
    # Read the Excel file
    df = pd.read_excel('raw/Comprobantes detallados.xlsx', sheet_name='Sheet1', skiprows=7)
    
    # Drop rows where 'Sucursal' column is NaN
    df = df.dropna(subset=['Sucursal'])
    
    # Ensure 'Código contable' is a string and strip spaces
    df['Código contable'] = df['Código contable'].astype(str).str.strip()
    
    #Limpiar strings 'Descripción' y 'Detalle'
    df['Descripción'] = df['Descripción'].str.replace(r'Cuota.*', 'Cuota', regex=True)
    df['Detalle'] = df['Detalle'].str.replace(r'Cuota.*', 'Cuota', regex=True)

    # Filter the DataFrame based on the 'Código contable' column
    df_filtered = df[df['Código contable'].isin(cost_dict)]
    
    # Group by 'Centro de costo'
    grouped = df_filtered.groupby('Centro de costo')
    
    # List to store the indices of rows to drop
    rows_to_drop = []
    
    # Iterate over each group
    for centro_costo_name, centro_costo_group in grouped:
        print(f"Processing Centro de costo: {centro_costo_name}")
        
        # Group by 'Nombre tercero' within each 'Centro de costo' group
        tercero_grouped = centro_costo_group.groupby('Nombre tercero')
        
        for tercero_name, tercero_group in tercero_grouped:
            print(f"Processing Nombre tercero: {tercero_name}")
            
            # Identify rows where 'Código contable' is '61350503.0' and 'Crédito' is not 0
            credito_rows = tercero_group[(tercero_group['Código contable'] == '61350503.0') & (tercero_group['Crédito'] != 0)]
            
            if not credito_rows.empty:
                print(f"Found 'Crédito' rows in Centro de costo {centro_costo_name} and Nombre tercero {tercero_name} with 'Código contable' 61350503.0:")
                print(credito_rows)
            
                # Process each 'Crédito' row
                for index, credito_row in credito_rows.iterrows():
                    credito_value = credito_row['Crédito']
                    
                    # Find 'Débito' rows with value equal or bigger than the 'Crédito' value
                    debito_rows = tercero_group[(tercero_group['Débito'] >= credito_value) & (tercero_group['Débito'] != 0)]
                    
                    if not debito_rows.empty:
                        print(f"Found matching 'Débito' rows in Centro de costo {centro_costo_name} and Nombre tercero {tercero_name}:")
                        print(debito_rows)
                        
                        # Subtract the 'Crédito' value from the first matching 'Débito' row
                        debito_index = debito_rows.index[0]
                        df_filtered.at[debito_index, 'Débito'] -= credito_value
                        # Set the 'Crédito' value to 0 as it has been fully accounted for
                        df_filtered.at[index, 'Crédito'] = 0

            # Identify rows where 'Código contable' is '22050501.0' and 'Crédito' is not 0
            credito_22050501_rows = tercero_group[(tercero_group['Código contable'] == '22050501.0') & (tercero_group['Crédito'] != 0)]
            
            if not credito_22050501_rows.empty:
                print(f"Found 'Crédito' rows in Centro de costo {centro_costo_name} and Nombre tercero {tercero_name} with 'Código contable' 22050501.0:")
                print(credito_22050501_rows)
                
                # Process each 'Crédito' row for '22050501.0'
                for index, credito_row in credito_22050501_rows.iterrows():
                    credito_value = credito_row['Crédito']
                    credito_detalle = credito_row['Detalle']
                    
                    # Find 'Débito' rows with value equal to the 'Crédito' value and matching 'Detalle' or 'Descripción'
                    debito_rows = tercero_group[((tercero_group['Débito'] == credito_value) & (tercero_group['Detalle'] == credito_detalle)) | 
                                                ((tercero_group['Débito'] == credito_value) & (tercero_group['Descripción'] == credito_detalle))]
                    
                    if not debito_rows.empty:
                        print(f"Found matching 'Débito' rows for '22050501.0' in Centro de costo {centro_costo_name} and Nombre tercero {tercero_name}:")
                        print(debito_rows)
                        
                        # If a match is found, add both rows to the drop list
                        debito_index = debito_rows.index[0]
                        rows_to_drop.append(index)
                        rows_to_drop.append(debito_index)
            
            # Identify rows where 'Código contable' is '22050501.0' and 'Débito' is not 0
            debito_22050501_rows = tercero_group[(tercero_group['Código contable'] == '22050501.0') & (tercero_group['Débito'] != 0)]
            
            if not debito_22050501_rows.empty:
                print(f"Found 'Débito' rows in Centro de costo {centro_costo_name} and Nombre tercero {tercero_name} with 'Código contable' 22050501.0:")
                print(debito_22050501_rows)
                
                # Add these rows to the drop list
                rows_to_drop.extend(debito_22050501_rows.index)

    # Drop the identified rows
    df_filtered.drop(rows_to_drop, inplace=True)
    
    # Reset the index of the filtered DataFrame
    df_filtered.reset_index(drop=True, inplace=True)
    
    # Write the updated DataFrame to a new Excel file
    df_filtered.to_excel('processed/cost_db.xlsx', sheet_name='cost', engine='xlsxwriter')  # Specify the engine
    
    # Print the updated DataFrame
    print("Updated DataFrame:")
    print(df_filtered)

if __name__ == "__main__":
    main()
