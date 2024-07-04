import pandas as pd

# Define the cost_dict set with ".0" added to each entry
cost_dict = {f'{code}.0' for code in [
    '11200501', '11051001', '11051002', '11051003', '13809501', '14350101', '14350102', '24081001',
    '61350503', '61800102', '73050502', '73050503', '73050504', '73050509', '73050510', '73050512',
    '73202001', '73550501', '73953505', '73956005', '22050501', '425050'
]}

def main():
    # Read the Excel file
    df = pd.read_excel('Comprobantes detallados.xlsx', sheet_name='Sheet1', skiprows=7)
    
    # Drop rows where 'Sucursal' column is NaN
    df = df.dropna(subset=['Sucursal'])
    
    # Ensure 'Código contable' is a string and strip spaces
    df['Código contable'] = df['Código contable'].astype(str).str.strip()
    
    # Filter the DataFrame based on the 'Código contable' column
    df_filtered = df[df['Código contable'].isin(cost_dict)]
    
    # Group by 'Centro de costo'
    grouped = df_filtered.groupby('Centro de costo')
    
    # Iterate over each group
    for name, group in grouped:
        print(f"Processing Centro de costo: {name}")
        
        # Identify rows where 'Código contable' is '61350503.0' and 'Crédito' is not 0
        credito_rows = group[(group['Código contable'] == '61350503.0') & (group['Crédito'] != 0)]
        
        if not credito_rows.empty:
            print(f"Found 'Crédito' rows in Centro de costo {name} with 'Código contable' 61350503.0:")
            print(credito_rows)
        
            # Process each 'Crédito' row
            for index, credito_row in credito_rows.iterrows():
                credito_value = credito_row['Crédito']
                
                # Find 'Débito' rows with value equal or bigger than the 'Crédito' value
                debito_rows = group[(group['Débito'] >= credito_value) & (group['Débito'] != 0)]
                
                if not debito_rows.empty:
                    print(f"Found matching 'Débito' rows in Centro de costo {name}:")
                    print(debito_rows)
                    
                    # Subtract the 'Crédito' value from the first matching 'Débito' row
                    debito_index = debito_rows.index[0]
                    df_filtered.at[debito_index, 'Débito'] -= credito_value
                    # Set the 'Crédito' value to 0 as it has been fully accounted for
                    df_filtered.at[index, 'Crédito'] = 0

    # Reset the index of the filtered DataFrame
    df_filtered.reset_index(drop=True, inplace=True)
    
    # Write the updated DataFrame to a new Excel file
    df_filtered.to_excel('cost_db.xlsx', sheet_name='cost', engine='xlsxwriter')  # Specify the engine
    
    # Print the updated DataFrame
    print("Updated DataFrame:")
    print(df_filtered)

if __name__ == "__main__":
    main()
