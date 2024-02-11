import pandas as pd
from datetime import datetime
import uuid

def transform_raw_to_df(raw_data):
    """
    
    """
    tables_data_frames = {}
    for table_name, table_data in raw_data.items():
        df = pd.DataFrame(table_data)
        tables_data_frames[table_name] = df
    return tables_data_frames

def get_test_info_df(dfs):
    """ 
    """
    tests_list = []
    
    for _, admission_row in dfs['test_admission'].iterrows():
        npi_number= admission_row['npi_number']
        patient_code = admission_row['patient_code']
        speciality_id = dfs['doctor'][dfs['doctor']['npi_number'] == npi_number]['speciality_id'].iloc[0]
        admission_test_date = admission_row['admission_datetime']
        test_dt = admission_row['test_datetime']
        test_code = admission_row['test_code']
        test_price = dfs['test_cost'][
                (dfs['test_cost']['test_code'] == test_code) &
                (dfs['test_cost']['price_date_from'] <= admission_test_date)
            ]['price'].iloc[-1]
        data = {
            'record_id': str(uuid.uuid4()),
            'speciality_id': speciality_id,
            'npi_number' : npi_number,
            'admission_test_date': admission_test_date.strftime('%Y-%m-%d'),
            'test_date' : test_dt.strftime('%Y-%m-%d'),
            'patient_code': patient_code,
            'test_code': test_code,
            'test_price': test_price,
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        tests_list.append(data)
    
    df_tests_info = pd.DataFrame(tests_list)
    
    return df_tests_info

def calculate_tests_per_stay(dfs):
    
    tests_per_stay_list = []
    
    for _, admission_row in dfs['admission'].iterrows():
        patient_code = admission_row['patient_code']
        admission_date = admission_row['admission_datetime']
        discharge_date = admission_row['discharge_datetime']
        day_price = dfs['stay_daily_cost'][dfs['stay_daily_cost']['price_date_from'] <= admission_date].iloc[-1]['price']      
        
        total_days = (discharge_date - admission_date).days + 1
        # Filter the test that had been taken on the stay
        tests_during_stay = dfs['test_admission'][
            (dfs['test_admission']['patient_code'] == patient_code) &
            (dfs['test_admission']['admission_datetime'] >= admission_date) &
            (dfs['test_admission']['admission_datetime'] <= discharge_date)
        ]
       
        test_count = len(tests_during_stay)  
        total_test_cost = 0  
                
        # Iterate on the tests that had been taken on the stay and calculate the price
        for _, test_row in tests_during_stay.iterrows():
            test_code = test_row['test_code']
            admission_test_date = test_row['admission_datetime']
            
            # found the right price for the test at that time
            test_price = dfs['test_cost'][
                (dfs['test_cost']['test_code'] == test_code) &
                (dfs['test_cost']['price_date_from'] <= admission_test_date)
            ]['price'].iloc[-1]  
            
            total_test_cost += test_price
        
        # Create a new record with all the information collected
        data = {
            'record_id': str(uuid.uuid4()),
            'patient_code': patient_code,
            'admission_date': admission_date.strftime('%Y-%m-%d'),
            'discharge_date': discharge_date.strftime('%Y-%m-%d'),
            'day_price' : day_price,
            'amount_stay_days' : total_days,
            'total_stay_cost': day_price * total_days,
            'amount_of_test_taken': test_count,
            'total_tests_cost': total_test_cost,
            'total_cost': total_test_cost + (day_price * total_days),
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        }
        # Add the new record to the list
        tests_per_stay_list.append(data)
    # Create a Pandas DataFrame with all the records    
    df_costs_per_stay =  pd.DataFrame(tests_per_stay_list)
    
    return df_costs_per_stay
        
def clean_dfs(df):
    """
    """
    
    # Patient
    df['patient']['patient_code'] = pd.to_numeric(df['patient']['patient_code'])
    df['patient']['phone_number'] = df['patient']['phone_number'].str.replace('\r\n', ' ').astype(str)
    # Doctor
    df['doctor']['npi_number'] = pd.to_numeric(df['doctor']['npi_number'])
    df['doctor']['speciality_id'] = pd.to_numeric(df['doctor']['speciality_id'])
    # Admission
    df['admission']['patient_code'] = pd.to_numeric(df['admission']['patient_code'])
    df['admission']['admission_datetime'] = pd.to_datetime(df['admission']['admission_datetime']).dt.date
    df['admission']['discharge_datetime'] = pd.to_datetime(df['admission']['discharge_datetime']).dt.date
    # Stay_daily_cost
    df['stay_daily_cost']['price_date_from'] = pd.to_datetime(df['stay_daily_cost']['price_date_from']).dt.date
    df['stay_daily_cost']['price'] = pd.to_numeric(df['stay_daily_cost']['price'])
    # Test_admission
    df['test_admission']['patient_code'] = pd.to_numeric(df['test_admission']['patient_code'])
    df['test_admission']['admission_datetime'] = pd.to_datetime(df['test_admission']['admission_datetime']).dt.date
    df['test_admission']['test_datetime'] = pd.to_datetime(df['test_admission']['test_datetime']).dt.date
    df['test_admission']['test_code'] = pd.to_numeric(df['test_admission']['test_code'])
    df['test_admission']['npi_number'] = pd.to_numeric(df['test_admission']['npi_number'])
    # Test
    df['test']['test_code'] = pd.to_numeric(df['test']['test_code'])
    # Test_costs
    df['test_cost']['test_code'] = pd.to_numeric(df['test_cost']['test_code'])
    df['test_cost']['price_date_from'] = pd.to_datetime(df['test_cost']['price_date_from']).dt.date
    df['test_cost']['price'] = pd.to_numeric(df['test_cost']['price'])
    # Speciality
    df['speciality']['speciality_id'] = pd.to_numeric(df['speciality']['speciality_id'])
    return df

def get_patients(dfs):
    """
    """
    patients_list = []
    for _, patient_row in dfs['patient'].iterrows():
        patient_code = patient_row['patient_code']
        patient_name = patient_row['patient_name']
        phone_number = patient_row['phone_number']
        data = {
            'record_id': str(uuid.uuid4()),
            'patient_code': patient_code,
            'patient_name': patient_name,
            'phone_number': phone_number,
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        patients_list.append(data)    
    df_patients = pd.DataFrame(patients_list)
    df_patients.drop_duplicates()
    return df_patients

def get_tests(dfs):
    """
    """
    tests_list = []
    for _, test_row in dfs['test'].iterrows():
        test_code = test_row['test_code']
        test_name = test_row['test_name']
        data = {
            'record_id': str(uuid.uuid4()),
            'test_code': test_code,
            'test_name': test_name,
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        tests_list.append(data)    
    df_tests= pd.DataFrame(tests_list)
    df_tests.drop_duplicates()
    
    return df_tests

def get_doctors(dfs):
    """
    """
    doctors_list = []
    for _, doctor_row in dfs['doctor'].iterrows():
        doctor_code = doctor_row['npi_number']
        doctor_name = doctor_row['doctor_name']
        speciality_id = doctor_row['speciality_id']
        
        speciality_name = dfs['speciality'][dfs['speciality']['speciality_id'] == speciality_id]['name'].iloc[0]
        data = {
            'record_id': str(uuid.uuid4()),
            'npi_number': doctor_code,
            'doctor_name': doctor_name,
            'speciality_id': speciality_id,
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        doctors_list.append(data)    
    df_doctors= pd.DataFrame(doctors_list)
    df_doctors.drop_duplicates()
    
    return df_doctors  

def get_specialitys(dfs):
    """
    """
    specialitys_list = []
    for _, speciality_row in dfs['speciality'].iterrows():
        speciality_id = speciality_row['speciality_id']
        speciality_name = speciality_row['name']
              
        data = {
            'speciality_id': speciality_id,
            'record_id': str(uuid.uuid4()),
            'speciality_name': speciality_name,
            'etl_ts':  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        specialitys_list.append(data)    
    df_specialitys= pd.DataFrame(specialitys_list)
    df_specialitys.drop_duplicates()
    
    return df_specialitys 
 
def get_dates(dfs):
    dates_list = []
    
    for _, row in dfs['admission'].iterrows():
        adm_date = row['admission_datetime']
        dis_date = row['discharge_datetime']
        dates_list.extend([adm_date,dis_date])
    for _, row in dfs['test_admission'].iterrows():
        test_adm_date = row['admission_datetime']
        test_date = row['test_datetime']
        dates_list.extend([test_adm_date,test_date])
    for _, row in dfs['stay_daily_cost'].iterrows():
        price_date = row['price_date_from']
        dates_list.append(price_date)
    for _, row in dfs['test_cost'].iterrows():
        test_price_date = row['price_date_from']
        dates_list.append(test_price_date)
    
    dates_list = list(set(dates_list))
    formatted_dates = []
    
    for date in dates_list:
        formatted_date = {
            'date': date.strftime('%Y-%m-%d'),
            'day': date.day,
            'month': date.month,
            'quarter': (date.month - 1) // 3 + 1,
            'year': date.year
        }
        formatted_dates.append(formatted_date)
    df_dates= pd.DataFrame(formatted_dates)
    
    return df_dates
        
def transform_data(raw_data):
    """
    Main transformation function, it transforms the data from lists to Pandas DataFrames, It does some cleaning,transformations and calculations.
    Args -> raw_data: {'table_name1':[table_data1], 'table_name2':[table_data2]}
    Returns -> final_dfs: [{'table_name1': <pandas dataframe object>}]
    """
 
    dfs = transform_raw_to_df(raw_data)  
    dfs = clean_dfs(dfs)
    df_test_per_stay = calculate_tests_per_stay(dfs)
    df_tests_info = get_test_info_df(dfs)
    df_dim_patients = get_patients(dfs)
    df_dim_tests = get_tests(dfs)
    df_dim_speciality = get_specialitys(dfs)
    df_dim_doctor = get_doctors(dfs)
    df_dim_date = get_dates(dfs)
    transformed_dfs = [df_dim_date,df_dim_speciality,df_dim_doctor,df_dim_patients,df_dim_tests,df_test_per_stay,df_tests_info]
    
    return transformed_dfs

# raw_data = extract_data()
# transform_data(raw_data)