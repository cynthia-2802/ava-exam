import pandas as pd
import xlrd
import os


# Function to combine all Excel files in a directory and save as a parquet file
def combine_data(data_path: str, data_name: str) -> pd.DataFrame:
    data = pd.DataFrame()
    for file_name in os.listdir(data_path):
        file_path = os.path.join(data_path, file_name)
        workbook = xlrd.open_workbook(file_path, logfile=open(os.devnull, "w"))
        temp_data = pd.read_excel(workbook)
        data = pd.concat([data, temp_data], ignore_index=True)
    # Save the combined data as a parquet file for future use
    data.to_parquet(f"data-1/raw/{data_name}.parquet")
    return data

# Function to load or create parquet files for all main datasets
def load_main_datasets():
    try:
        groupage_data = pd.read_parquet("data-1/raw/groupage_data.parquet")
        innight_data = pd.read_parquet("data-1/raw/innight_data.parquet")
        pallet_data = pd.read_parquet("data-1/raw/pallet_data.parquet")
        road_freight_data = pd.read_parquet("data-1/raw/road_freight_data.parquet")
        customer_data = pd.read_parquet("data-1/raw/customer_data.parquet")
    except FileNotFoundError:
        groupage_data = combine_data("data-1/raw/dds/Groupage/", "groupage_data")
        innight_data = combine_data("data-1/raw/dds/Innight/", "innight_data")
        pallet_data = combine_data("data-1/raw/dds/Pallet/", "pallet_data")
        road_freight_data = combine_data("data-1/raw/dds/Road_Freight/", "road_freight_data")
        customer_data = pd.read_excel("data-1/raw/master-data/ADS_DDS_RF_CustomerNumbers_MasterData.xlsx")
        customer_data.to_parquet("data-1/raw/customer_data.parquet")
    return groupage_data, innight_data, pallet_data, road_freight_data, customer_data

# Function to preprocess combined data
def preprocess_data(combined_data: pd.DataFrame) -> pd.DataFrame:
    combined_data["Parcel/Pallet type"] = combined_data["Parcel/Pallet type"].fillna("PLL")
    if "FileName" in combined_data.columns:
        combined_data = combined_data.drop(columns=["FileName"])
    combined_data = combined_data.drop_duplicates(keep="first")
    combined_data = combined_data.drop_duplicates(subset=["Shipment Tracking Number"])
    combined_data = combined_data.dropna(subset=["Customer_ID"])
    return combined_data

# Function to merge data with customer data
def merge_with_customer_data(combined_data: pd.DataFrame, customer_data: pd.DataFrame) -> pd.DataFrame:
    combined_data = pd.merge(combined_data, customer_data, left_on="Customer_ID", right_on="Customer ID", how="left")
    combined_data = combined_data.drop(columns=["Customer ID"])
    return combined_data

# Function to merge with DK postcode data
def merge_with_dk_postcode(combined_data: pd.DataFrame) -> pd.DataFrame:
    dk_postcode = pd.read_excel("data-1/raw/master-data/New_PostCodes_Data_DK/DK_Geonames.xlsx")
    dk_postcode = dk_postcode[["Column1", "Postal Code", "Lat", "Long"]]
    dk_postcode = dk_postcode.rename(columns={"Lat": "Latitude", "Long": "Longitude", "Column1": "Country Code"})

    combined_data = pd.merge(combined_data, dk_postcode, left_on=["Consignor Location Nr", "ConsignorCountryCode"], right_on=["Postal Code", "Country Code"], how="left")
    combined_data = combined_data.rename(columns={"Latitude": "Consignor Latitude", "Longitude": "Consignor Longitude"})
    combined_data = combined_data.drop(columns=["Postal Code", "Country Code"])
    combined_data = pd.merge(combined_data, dk_postcode, left_on=["Consignee Location Nr", "ConsigneeCountryCode"], right_on=["Postal Code", "Country Code"], how="left")
    combined_data = combined_data.rename(columns={"Latitude": "Consignee Latitude", "Longitude": "Consignee Longitude"})
    combined_data = combined_data.drop(columns=["Postal Code", "Country Code"])
    return combined_data

# Merge with coordinates data
def merge_with_coords_data(combined_data: pd.DataFrame) -> pd.DataFrame:
    consignee_generated_coords = pd.read_csv("data-1/raw/master-data/consignee_generated_coords.csv")
    consignor_generated_coords = pd.read_csv("data-1/raw/master-data/consignor_generated_coords.csv")

    consignor_generated_coords = consignor_generated_coords.rename(
    columns={
            "Latitude (generated)":"ConsignorLatitude",
            "Longitude (generated)":"ConsignorLongitude"
        }
    )
    consignee_generated_coords = consignee_generated_coords.rename(
        columns={
            "Latitude (generated)":"ConsigneeLatitude",
            "Longitude (generated)":"ConsigneeLongitude"
        }
    )
    
    combined_data = pd.merge(
        combined_data, consignor_generated_coords,
        left_on=["ConsignorCountryCode", "Consignor Location Nr"],
        right_on=["Consignor Country Code", "Consignor Location Nr"],
        how="left"
    )
    combined_data = pd.merge(
        combined_data, consignee_generated_coords,
        left_on=["ConsigneeCountryCode", "Consignee Location Nr"],
        right_on=["Consignee Country Code", "Consignee Location Nr"],
        how="left"
    )
    return combined_data


# Execute data loading and processing
print("Loading data...")
groupage_data, innight_data, pallet_data, road_freight_data, customer_data = load_main_datasets()
print("Combining data...")
combined_data = pd.concat([groupage_data, innight_data, pallet_data, road_freight_data], ignore_index=True)
print("Preprocessing data...")
combined_data = preprocess_data(combined_data)
print("Merging with customer data...")
combined_data = merge_with_customer_data(combined_data, customer_data)
print("Fixing postcode data...")
combined_data = merge_with_coords_data(combined_data)

# Check if processed dir exists
if not os.path.exists("data-1/processed"):
    os.makedirs("data-1/processed")

# Save the final combined data
print("Saving data...")
combined_data.to_parquet("data-1/processed/combined_data_with_coords.parquet")
combined_data.to_csv("data-1/processed/combined_data_with_coords.csv", index=False)
print("DONE DONE!!!")
