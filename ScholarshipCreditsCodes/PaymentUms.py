import pandas as pd
import numpy as np
import requests
import logging
import json
import time
from dateutil import parser
import os
import pickle
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom JSON encoder to handle NaN values
class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isna(obj):
            return None  # Converts NaN values to None
        return json.JSONEncoder.default(self, obj)


# Global Variables and Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NEW_GOOGLE_SHEET_ID = '1AiLS3sSiAJHGGVA_2rtkiI_I8jk5aXmCfbD-HwMsIY4'
NEW_SHEET_NAME = 'Testing'
BUBBLE_API_ENDPOINT = 'https://app.edoofa.com/api/1.1/obj/Payments'
BUBBLE_HEADERS = {
    "Authorization": "Bearer 786720e8eb68de7054d1149b56cc04f9",
    "Content-Type": "application/json"
}

def load_google_credentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds

def fetch_data_from_google_sheet(service, sheet_id, subsheet_name):
    try:
        # Specify the range of the subsheet, e.g., "Sheet1!A:E" to fetch columns A to E in Sheet1
        range_name = f'{subsheet_name}!A:E'  # Adjust this range to include the required columns
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            logging.warning('No data found in the subsheet.')
            return pd.DataFrame()
        else:
            df = pd.DataFrame(values[1:], columns=values[0])  # Assuming the first row is the header
            logging.info('Data fetched successfully from Google Sheets.')
            logging.debug(f'First few rows from Google Sheets:\n{df.head()}')
            df['payment-date'] = pd.to_datetime(df['payment-date']).dt.strftime('%m/%d/%Y')
            logging.debug(f'Payment-date format in Google Sheets:\n{df["payment-date"].head()}')
            return df
    except Exception as e:
        logging.error(f'Error fetching data from Google Sheets: {e}')
        return pd.DataFrame()

def fetch_data_from_bubble(api_endpoint, headers):
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code

        data = response.json()['response']['results']
        if data:
            df = pd.DataFrame(data)
            logging.info('Data fetched successfully from Bubble.')
            logging.debug(f'First few rows from Bubble:\n{df.head()}')
            df['payment-date'] = df['payment-date'].apply(lambda x: parser.parse(x).strftime('%m/%d/%Y'))
            logging.debug(f'Payment-date format in Bubble:\n{df["payment-date"].head()}')
            return df
        else:
            logging.warning('No data found in Bubble.')
            return pd.DataFrame()

    except requests.HTTPError as http_err:
        logging.error(f'HTTP error occurred while fetching data from Bubble: {http_err}')
    except Exception as e:
        logging.error(f'Error fetching data from Bubble: {e}')

    return pd.DataFrame()

# Find Unique Entries
def find_unique_entries(google_sheet_df, bubble_df):
    key_columns = ['admissions-group-name','payment-date','payment-type', 'payment-category']
    for col in key_columns:
        if col not in google_sheet_df or col not in bubble_df:
            logging.error(f"Key column '{col}' not found in one of the DataFrames.")
            return pd.DataFrame()
    
    google_sheet_df['combined_key'] = google_sheet_df[key_columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
    bubble_df['combined_key'] = bubble_df[key_columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

    unique_df = google_sheet_df[~google_sheet_df['combined_key'].isin(bubble_df['combined_key'])]
    unique_df = unique_df.drop('combined_key', axis=1)  # Drop the temporary combined key column

    if unique_df.empty:
        logging.info("No unique entries found.")
    else:
        logging.info(f"Found {len(unique_df)} unique entries.")

    return unique_df

# Bulk Upload to Bubble
def bulk_upload_to_bubble(api_endpoint, headers, data, batch_size=1000):
    # Determine the total number of records to upload
    total_records = len(data)
    print(f"Total records to upload: {total_records}")

    # Calculate the number of batches required based on the batch size
    num_batches = (total_records + batch_size - 1) // batch_size
    print(f"Number of batches: {num_batches}")

    # Loop through each batch and upload it
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = min((batch_num + 1) * batch_size, total_records)
        batch_data = data.iloc[start_index:end_index]

        # Convert DataFrame batch to list of dictionaries
        records_to_upload = batch_data.to_dict(orient='records')

        # Generate JSON payload using the custom encoder, ensuring no newlines within JSON objects
        json_payload = '\n'.join(json.dumps(record, cls=CustomJsonEncoder).replace('\n', '') for record in records_to_upload)

        # Update headers to include 'Content-Type': 'text/plain'
        updated_headers = headers.copy()
        updated_headers['Content-Type'] = 'text/plain'

        # Make the POST request to the bulk endpoint
        bulk_endpoint = f"{api_endpoint}/bulk"  # Adjust this endpoint as per your Bubble app's URL
        response = requests.post(bulk_endpoint, headers=updated_headers, data=json_payload)

        print(f"Batch {batch_num + 1}/{num_batches} - Response Status Code: {response.status_code}")
        print(f"Batch {batch_num + 1}/{num_batches} - Response Body: {response.text}")

        if response.status_code != 200:
            print(f"Error in batch {batch_num + 1}/{num_batches} - Bulk uploading to Bubble.io: " + response.text)
        else:
            print(f"Batch {batch_num + 1}/{num_batches} - Bulk data uploaded successfully to Bubble.io")

        # Optionally, add a short delay between batches to avoid rate limiting issues
        time.sleep(1)


def update_google_sheets(service, sheet_id, data_to_update):
    try:
        logging.info("Starting to fetch data from the 'Testing' subsheet.")
        # Fetch all data from the "Testing" subsheet
        range_testing = 'Testing!A:E'  # Adjust the range to include all necessary columns
        result_testing = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_testing).execute()
        testing_data = result_testing.get('values', [])
        
        if not testing_data:
            logging.error('No data found in "Testing" sheet.')
            return
        else:
            logging.info("Data successfully fetched from the 'Testing' subsheet.")

        logging.info("Starting to create combined keys for sheet data.")
        # Create a combined key from multiple columns for each row in the sheet
        combined_keys_sheet = ['|'.join(row[:5]) for row in testing_data[1:]]  # Assuming the first 5 columns are the ones to match
        logging.info("Combined keys for sheet data created successfully.")

        # Update the "Inserted" status for each entry in data_to_update
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Starting to update the 'Testing' subsheet with unique entries.")
        for entry in data_to_update.itertuples():
            # Use indexing to access column values
            combined_key_entry = '|'.join([
                str(entry[data_to_update.columns.get_loc('admissions-group-name') + 1]),
                str(entry[data_to_update.columns.get_loc('paid-amount') + 1]),
                str(entry[data_to_update.columns.get_loc('payment-type') + 1]),
                str(entry[data_to_update.columns.get_loc('payment-date') + 1]),
                str(entry[data_to_update.columns.get_loc('payment-category') + 1])
            ])
            
            # Find all matching row indices for this combined key
            matching_row_indices = [i for i, key in enumerate(combined_keys_sheet) if key == combined_key_entry]
            
            if not matching_row_indices:
                combined_key_entry
                #logging.warning(f"No matching rows found for combined key: {combined_key_entry}")
            
            for row_index in matching_row_indices:
                actual_row_number = row_index + 2  # Adjusting for header row and 0-based indexing
                # Update both "Inserted" status and timestamp
                range_to_update = f'Testing!F{actual_row_number}:G{actual_row_number}'
                body = {'values': [['Inserted', current_timestamp]]}
                update_response = service.spreadsheets().values().update(
                    spreadsheetId=sheet_id, range=range_to_update,
                    valueInputOption='USER_ENTERED', body=body
                ).execute()
                logging.info(f'Inserted status and timestamp updated for row {actual_row_number} in "Testing" subsheet.')

        logging.info("Finished updating the 'Testing' subsheet with unique entries.")

        # Append data to the "Logs" subsheet for each unique entry
        logging.info("Starting to append data to the 'Logs' subsheet.")
        for entry in data_to_update.itertuples():
            admissions_group_name = entry[data_to_update.columns.get_loc('admissions-group-name') + 1]
            log_row = [admissions_group_name, current_timestamp]
            body_logs = {'values': [log_row]}
            append_response = service.spreadsheets().values().append(
                spreadsheetId=sheet_id, range='Logs!A:B',
                valueInputOption='USER_ENTERED', body=body_logs,
                insertDataOption='INSERT_ROWS'
            ).execute()
            logging.info(f'Data appended to Logs subsheet for {admissions_group_name}.')

        logging.info("Finished appending data to the 'Logs' subsheet.")

    except Exception as e:
        logging.error(f'Error updating Google Sheets: {e}')



def get_last_row_index_in_sheet3(service, sheet_id):
    try:
        # Get the values in the last row of Sheet3
        range_name_sheet3 = 'Logs!A:A'  # Modify 'YourSheetName' to your sheet name
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name_sheet3
        ).execute()

        values = result.get('values', [])
        if values:
            # Calculate the last row index by counting the number of rows in the result
            last_row_index_sheet3 = len(values)
            return last_row_index_sheet3
        else:
            # If there are no values, assume the first row is the header, so the last row index is 0
            return 0

    except Exception as e:
        logging.error(f'Error getting last row index in Sheet3: {e}')
        return 0  # Return 0 in case of an error

# Main Function
def main():
    try:
        creds = load_google_credentials()
        service = build('sheets', 'v4', credentials=creds)

        google_sheet_df = fetch_data_from_google_sheet(service, NEW_GOOGLE_SHEET_ID, NEW_SHEET_NAME)
        bubble_df = fetch_data_from_bubble(BUBBLE_API_ENDPOINT, BUBBLE_HEADERS)

        unique_entries_df = find_unique_entries(google_sheet_df, bubble_df)

        if not unique_entries_df.empty:
            bulk_upload_to_bubble(BUBBLE_API_ENDPOINT, BUBBLE_HEADERS, unique_entries_df)
            update_google_sheets(service, NEW_GOOGLE_SHEET_ID, unique_entries_df)
        else:
            logging.info("No unique entries to upload to Bubble.")

    except Exception as ex:
        logging.error("An error occurred: %s", ex)

if __name__ == "__main__":
    main()
