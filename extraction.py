import os
from dotenv import load_dotenv
from supabase import create_client



def connect_supabase_bd():
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
    table_data = bd.table(f'{table_name}').select("*").execute()
    return table_data.data





def extract_data():
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
        
    

