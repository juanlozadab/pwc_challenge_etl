import os
from dotenv import load_dotenv
from supabase import create_client



def connect_supabase_bd():
    """ 
    Connects to the Supabase Database.
    
    Returns:
    - Supabase client: A client object for interacting with the Supabase Database.

    """
    try:
        load_dotenv('config.env')
        url = os.getenv("SUPABASE_DB_URL")
        key = os.getenv("SUPABASE_DB_KEY")
        supabase = create_client(url,key)
        return supabase
    except Exception as e:
        print(f"Error connecting to Supabase: {str(e)}")
        return None

def get_table_data(bd, table_name):
    """
    Retrieves data from a specific table in the database.

    Args:
    - bd: Supabase client object.
    - table_name: Name of the table to extract data from.

    Returns:
    - List: List of dictionaries representing the data in the table.
    """
    table_data = bd.table(f'{table_name}').select("*").execute()
    return table_data.data





def extract_data():
    """
    Extracts data from the source database.

    Returns:
    - dict: A dictionary containing extracted data from different tables.
    """
    supabase = connect_supabase_bd()
    tables_to_extract = ['patient','doctor','speciality','admission','stay_daily_cost', 'test_admission', 'test', 'test_cost']
    data_dict = {}

    for table_name in tables_to_extract:
        try:
            table_data = get_table_data(supabase, table_name)
            data_dict[table_name] = table_data
        except Exception as e: 
            print(f"Error extracting data from table {table_name}: {str(e)}")
        
           
    return data_dict
        
    

