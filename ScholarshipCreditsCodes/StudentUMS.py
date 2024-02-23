import pandas as pd
import numpy as np
import requests
import logging
import json
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
            return None  # Convert NaN to None, which will become 'null' in JSON
        return json.JSONEncoder.default(self, obj)

# Global Variables and Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GOOGLE_SHEET_ID = '1OgFzWKqH6XvUO-9PqtQQJItevqVgwz5-JwPmxlTGByg'
BUBBLE_API_ENDPOINT = 'https://app.edoofa.com/api/1.1/obj/student'
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
        # Specify the range of the subsheet, e.g., "Sheet1!A:Z" to fetch all columns in Sheet1
        range_name = f'{subsheet_name}!A:AM'
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            logging.warning('No data found in the subsheet.')
            return pd.DataFrame()
        else:
            df = pd.DataFrame(values[1:], columns=values[0])  # Assuming the first row is the header
            logging.info('Data fetched successfully from Google Sheets.')
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
            return df
        else:
            logging.warning('No data found in Bubble.')
            return pd.DataFrame()

    except requests.HTTPError as http_err:
        logging.error(f'HTTP error occurred while fetching data from Bubble: {http_err}')
    except Exception as e:
        logging.error(f'Error fetching data from Bubble: {e}')

    return pd.DataFrame()

def find_unique_entries(google_sheet_df, bubble_df):
    # Ensure the key column exists in both DataFrames
    key_column = 'admissions-group-name'
    if key_column not in google_sheet_df or key_column not in bubble_df:
        logging.error(f"Key column '{key_column}' not found in one of the DataFrames.")
        return pd.DataFrame()

    # Find unique entries
    unique_df = google_sheet_df[~google_sheet_df[key_column].isin(bubble_df[key_column])]
    
    if unique_df.empty:
        logging.info("No unique entries found.")
    else:
        logging.info(f"Found {len(unique_df)} unique entries.")

    return unique_df

def fetch_and_map_country_data(api_endpoint, headers, df, country_column='country'):
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()
        countries = response.json()['response']['results']
        country_map = {country['name']: country['_id'] for country in countries}

        # Map country names in DataFrame to their corresponding IDs
        df[country_column] = df[country_column].map(country_map)
        logging.info("Country names mapped to IDs successfully.")

    except requests.HTTPError as http_err:
        logging.error(f'HTTP error occurred while fetching and mapping country data: {http_err}')
    except Exception as e:
        logging.error(f'Error fetching and mapping country data: {e}')


def fetch_and_map_user_fields(api_endpoint, headers, df, fields):
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()
        users = response.json()['response']['results']
        user_map = {user['name']: user['_id'] for user in users}

        # Map user names in DataFrame to their corresponding IDs for each field
        for field in fields:
            if field in df.columns:
                df[field] = df[field].map(user_map).fillna('')  # Use an empty string '' to represent empty values
        logging.info("User fields mapped to IDs successfully.")

    except requests.HTTPError as http_err:
        logging.error(f'HTTP error occurred while fetching and mapping user data: {http_err}')
    except Exception as e:
        logging.error(f'Error fetching and mapping user data: {e}')


def prepare_data_for_upload(df):
    default_number = None  # Default value for phone numbers is None
    default_date = None  # Use None for dates

    # Handle phone number fields
    for field in ['india-ph-number', 'mother-contact', 'father-contact', 'phone-number']:
        if field in df.columns:
            try:
                df[field] = df[field].replace('', default_number).fillna(default_number).astype(float)
            except Exception as e:
                print(f"Error processing field {field}: {e}")

    # Handle date fields
    date_fields = ['visa-expiry', 'date-of-arrival', 'date-of-birth', 'enrollment-date', 'frro-expiry-date']
    for field in date_fields:
        if field in df.columns:
            try:
                df[field] = df[field].astype(object).replace('', default_date).fillna(default_date)
            except Exception as e:
                print(f"Error processing date field {field}: {e}")

    return df


def bulk_upload_to_bubble(api_endpoint, headers, data):
    # Convert DataFrame to a list of dictionaries
    records_to_upload = data.to_dict(orient='records')

    # Define fields to skip for empty values
    date_fields = ['visa-expiry', 'date-of-arrival', 'date-of-birth', 'enrollment-date', 'frro-expiry-date']
    number_fields = ['india-ph-number', 'mother-contact', 'father-contact', 'phone-number']
    user_fields = ['admissions-officer', 'counsellor', 'dsw-officer', 'ewyl-mentor']

    # Process each record to omit empty date, number, and user fields
    processed_records = []
    for record in records_to_upload:
        # Create a new record dictionary, skipping empty date, number, and user fields
        processed_record = {
            k: v if (k not in date_fields or (v != '' and v is not None)) and
                   (k not in number_fields or v != '') and
                   (k not in user_fields or v != '')
            else None
            for k, v in record.items()
        }
        processed_records.append(processed_record)

    # Generate JSON payload using the custom encoder, now using processed_records
    json_payload = '\n'.join(json.dumps(record, cls=CustomJsonEncoder) for record in processed_records)

    # Log the JSON payload for inspection
    logging.debug("JSON payload being uploaded: " + json_payload)

    # Update headers to include 'Content-Type': 'text/plain'
    headers = headers.copy()
    headers['Content-Type'] = 'text/plain'

    # Make the POST request to the bulk endpoint
    bulk_endpoint = f"{api_endpoint}/bulk"  # Modify this endpoint as per your Bubble app's URL
    response = requests.post(bulk_endpoint, headers=headers, data=json_payload)

    logging.debug(f"Response Status Code: {response.status_code}")
    logging.debug(f"Response Body: {response.text}")

    if response.status_code != 200:
        logging.error("Error in bulk uploading to Bubble.io: " + response.text)
    else:
        logging.info("Bulk data uploaded successfully to Bubble.io")


def update_google_sheets(service, sheet_id, data_to_update):
    try:
        # Fetch all data from the "Testing" subsheet
        range_testing = 'IE DATA!A:G'
        result_testing = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_testing).execute()
        testing_data = result_testing.get('values', [])
        if not testing_data:
            logging.error('No data found in "IE DATA" sheet.')
            return

        # Map admissions-group-name to row number in "Testing" sheet
        admissions_group_name_to_row = {row[0]: idx + 2 for idx, row in enumerate(testing_data[1:]) if row}

        # Update the "Inserted" status and timestamp for each entry in data_to_update in "Testing" sheet
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for entry in data_to_update.itertuples():
            admissions_group_name = entry._1
            row_number = admissions_group_name_to_row.get(admissions_group_name)
            if row_number:
                # Update both "Inserted" status and timestamp in columns F and G
                range_to_update = f'IE DATA!AN{row_number}:AO{row_number}'
                body = {'values': [['Inserted', current_timestamp]]}
                service.spreadsheets().values().update(
                    spreadsheetId=sheet_id, range=range_to_update,
                    valueInputOption='USER_ENTERED', body=body
                ).execute()
                logging.info(f'Inserted status and timestamp updated for {admissions_group_name} at row {row_number}')
            else:
                logging.warning(f'admissions-group-name {admissions_group_name} not found in "IE DATA" sheet.')

        # Append data to Sheet3 (Logs)
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sheet3_data = [[entry._1, current_timestamp] for entry in data_to_update.itertuples()]
        range_name_sheet3 = 'Logs!A:B'
        body_sheet3 = {'values': sheet3_data}
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id, range=range_name_sheet3,
            valueInputOption='USER_ENTERED', body=body_sheet3,
            insertDataOption='INSERT_ROWS'
        ).execute()
        logging.info('Data appended successfully in Sheet3 (Logs).')

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

def main():
    try:
        creds = load_google_credentials()
        service = build('sheets', 'v4', credentials=creds)
        subsheet_name = 'IE DATA'

        google_sheet_df = fetch_data_from_google_sheet(service, GOOGLE_SHEET_ID, subsheet_name)
        bubble_df = fetch_data_from_bubble(BUBBLE_API_ENDPOINT, BUBBLE_HEADERS)

        country_api_endpoint = 'https://app.edoofa.com/api/1.1/obj/country'
        user_api_endpoint = 'https://app.edoofa.com/api/1.1/obj/User'
        fields_to_map = ['admissions-officer', 'counsellor', 'dsw-officer', 'ewyl-mentor']
        unique_entries_df = find_unique_entries(google_sheet_df, bubble_df)

        if not unique_entries_df.empty:
            fetch_and_map_country_data(country_api_endpoint, BUBBLE_HEADERS, unique_entries_df)
            #reverse_map_country_data(country_api_endpoint, BUBBLE_HEADERS, unique_entries_df, 'country')
            fetch_and_map_user_fields(user_api_endpoint, BUBBLE_HEADERS, unique_entries_df, fields_to_map)
            prepared_df = prepare_data_for_upload(unique_entries_df)    
            bulk_upload_to_bubble(BUBBLE_API_ENDPOINT, BUBBLE_HEADERS, prepared_df)
            update_google_sheets(service, GOOGLE_SHEET_ID, prepared_df)

        else:
            logging.info("No unique entries to upload to Bubble.")

    except Exception as ex:
        logging.error("An error occurred: %s", ex)

if __name__ == "__main__":
    main()