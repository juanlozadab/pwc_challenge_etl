import os
from dotenv import load_dotenv
from supabase import create_client

def connect_supabase_dw():
    try:
        load_dotenv('config.env')
        url = os.getenv("SUPABASE_DW_URL")
        key = os.getenv("SUPABASE_DW_KEY")
        supabase = create_client(url,key)
        return supabase
    except Exception as e:
        print(f"Error connecting to Supabase: {str(e)}")
        return None

def load_data(transformed_data):
    supabase = connect_supabase_dw()
    tables_to_update = ['dim_date','dim_speciality','dim_doctor','dim_patient', 'dim_test','fact_patients_stay_cost', 'fact_tests_information']
   
    
    for i,df in enumerate(transformed_data):
        
        table_data = transformed_data[i].to_dict(orient='records')
        table_name = tables_to_update[i]
        print(f'dataframe = {i}, {tables_to_update[i]}')
        for record in table_data:
            try:
                data, count = supabase.table(table_name).insert(record).execute()
                print(f"{data} record inserted in {table_name}")
                
            except Exception as e:
                if int(e.code) == 23505:
                    print('Record already exists, to avoid duplicates it is not loaded.')
                else:
                    print(f'Warning code: {e.code},The record was not inserted because {e.message}.')
            