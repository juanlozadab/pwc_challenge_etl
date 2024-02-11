# Import modules and functions
from extraction import extract_data
from transformation import transform_data
from loading import load_data

def main():
    # Extract data from the source
    raw_data = extract_data()

    # Transform the extracted data 
    transformed_data = transform_data(raw_data)
    
    #Load the transformed data in the DataWarehouse
    load_data(transformed_data)

if __name__ == "__main__":
    
    main()

     
