from utils.db_connection import DatabaseConnection
import pandas as pd
import numpy as np
import datetime

def convert_year_to_date(year):
    if pd.isna(year):
        return datetime.date(1900, 1, 1)  # Default date if projected_eol_date is None
    if isinstance(year, (pd.Timestamp, datetime.datetime)):
        return year.date()
    return pd.to_datetime(f'{int(year)}-01-01').date()

def convert_purchase_date(date):
    if pd.isna(date):
        return datetime.date(1900, 1, 1)  # Default date if purchase_date is None
    if isinstance(date, (pd.Timestamp, datetime.datetime)):
        return date.date()
    return pd.to_datetime(date).date()

def upload_data(file_path, table_name):
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Replace NaN values with None
    df = df.replace({np.nan: None})

    # Convert year columns to date
    if 'projected_eol_date' in df.columns:
        df['projected_eol_date'] = df['projected_eol_date'].apply(convert_year_to_date)
    if 'purchase_date' in df.columns:
        df['purchase_date'] = df['purchase_date'].apply(convert_purchase_date)

    # Replace None values in prices with 0
    if 'purchase_price' in df.columns:
        df['purchase_price'] = df['purchase_price'].apply(lambda x: 0 if x is None else x)

    if 'replacement_price' in df.columns:
        df['replacement_price'] = df['replacement_price'].apply(lambda x: 0 if x is None else x)

    # Replace None values in specific columns with "NA"
    for col in ['company', 'model', 'serial', 'description']:
        df[col] = df[col].apply(lambda x: 'NA' if x is None else x)

    # Establish a database connection
    db = DatabaseConnection(
        host="127.0.0.1",
        user="root",
        password="root",
        database="mvault",
        port=3340
    )
    db.connect()
    
    # Iterate over the rows in the DataFrame and insert them into the database
    for index, row in df.iterrows():
        columns = []
        values = []
        placeholders = []

        for col in df.columns:
            if row[col] is not None:
                columns.append(f'`{col}`')
                value = row[col]
                if col == 'purchase_price' and value is None:
                    value = 0  # Ensure purchase_price is not None
                if isinstance(value, pd.Timestamp):
                    value = value.date()  # Convert Timestamp to date
                values.append(value)
                placeholders.append('%s')

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        db.execute_query(query, tuple(values))
    
    # Close the database connection
    db.close()

if __name__ == "__main__":
    file_path = 'Book1.xlsx'
    table_name = 'assets'
    upload_data(file_path, table_name)