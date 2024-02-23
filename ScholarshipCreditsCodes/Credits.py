import json
import pickle
import os
import io
import logging
import time
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from datetime import datetime
import requests


# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isna(obj):
            return None  # Converts NaN values to None
        return json.JSONEncoder.default(self, obj)


def load_credentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise FileNotFoundError("No valid credentials available. Please run the authentication flow again.")
    return creds

# Function to find the file in Google Drive
def find_file_in_drive(service, folder_id, month, year):
    query = f"'{folder_id}' in parents and name = 'Scholarship Credits' and mimeType = 'application/json'"
    response = service.files().list(q=query).execute()
    if not response.get('files'):
        raise FileNotFoundError("Scholarship Credits folder not found.")
    scholarship_credits_id = response.get('files')[0].get('id')
    month_folder_name = f"{month} {year}"
    query = f"'{scholarship_credits_id}' in parents and name = '{month_folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    response = service.files().list(q=query).execute()
    if not response.get('files'):
        raise FileNotFoundError(f"{month_folder_name} folder not found.")
    month_folder_id = response.get('files')[0].get('id')
    query = f"'{month_folder_id}' in parents and mimeType = 'application/vnd.ms-excel'"
    response = service.files().list(q=query).execute()
    files = response.get('files', [])
    if not files:
        raise FileNotFoundError("No CSV file found in the specified month folder.")
    return files[0].get('id')

# Function to download CSV data from Google Drive using token.pickle
def download_json_from_drive(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    # Print file content for debugging
    file_content = fh.read()
    print("File Content:", file_content)

    # Reset the file handle to the beginning
    fh.seek(0)

    try:
        data = json.loads(file_content)  # Load JSON data
    except json.JSONDecodeError as e:
        logging.error(f"JSON parsing error: {e}")
        return None  # Or handle the error as appropriate

    df = pd.DataFrame(data)  # Convert to DataFrame
    return df

#Fetch all data from Bubble.io and convert it to a DataFrame."""
def fetch_and_convert_bubble_data_to_df(api_endpoint, headers):
    def fetch_all_data_from_bubble(api_endpoint, headers):
        all_data = []
        cursor = 0
        limit = 100
        while True:
            paginated_endpoint = f"{api_endpoint}?limit={limit}&cursor={cursor}"
            try:
                response = requests.get(paginated_endpoint, headers=headers)
                if response.status_code == 200:
                    data = response.json()['response']['results']
                    all_data.extend(data)
                    if len(data) < limit:
                        break
                    cursor += limit
                else:
                    logging.error("Error fetching data from Bubble.io: " + response.text)
                    break
            except requests.exceptions.RequestException as e:
                logging.error("HTTP Request failed: " + str(e))
                break
        return all_data

    bubble_data = fetch_all_data_from_bubble(api_endpoint, headers)
    bubble_df = pd.DataFrame(bubble_data)
    return bubble_df


#Find unique entries by comparing JSON DataFrame with Bubble.io DataFrame."""
def find_unique_entries(json_df, bubble_df):
    # Define the key columns for comparison
    key_columns = ['lead-name']
    
    # Finding unique rows in json_df that are not in bubble_df
    unique_df = pd.merge(json_df, bubble_df, on=key_columns, how='left', indicator=True)
    unique_df = unique_df[unique_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    return unique_df


#Fetch all data from Bubble.io with pagination
def fetch_all_data_from_bubble(api_endpoint, headers):
    all_data = []
    cursor = 0
    limit = 100  # Adjust this limit based on Bubble.io's constraints

    while True:
        paginated_endpoint = f"{api_endpoint}?limit={limit}&cursor={cursor}"
        try:
            response = requests.get(paginated_endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()['response']['results']
                all_data.extend(data)

                if len(data) < limit:  # No more data to fetch
                    break
                cursor += limit  # Increment the cursor by the limit
            else:
                print("Error fetching data from Bubble.io:", response.text)
                break
        except requests.exceptions.RequestException as e:
            print("HTTP Request failed:", e)
            break

    return all_data

#Bulk upload data to Bubble.io
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
        time.sleep(10)



def find_json_file_in_drive(service, folder_id):
    """
    Find the first JSON file in the given Google Drive folder.
    """
    query = f"'{folder_id}' in parents and mimeType = 'application/json'"
    response = service.files().list(q=query).execute()
    files = response.get('files', [])
    if not files:
        raise FileNotFoundError("No JSON file found in the specified folder.")
    return files[0].get('id')

def main():
    try:
        creds = load_credentials()
        service = build('drive', 'v3', credentials=creds)

        # Replace this with your actual folder ID
        folder_id = "1_jgETuMxuJ1s800pS5PY1twjuxTEYTtF"

        # Find JSON file in Google Drive and download it
        #json_file_ids = find_json_file_in_drive(service, folder_id)
        json_file_ids = '1pt89q5AsFqyVPmTwMmN20vtXhM1VQThY'
        json_data = download_json_from_drive(service, json_file_ids)
        json_df = pd.DataFrame(json_data)
        logging.debug(f"JSON Data: {json_data}")
        # Define Bubble.io API endpoint and headers
        bubble_api_endpoint = "https://app.edoofa.com/api/1.1/obj/scholarship-credits"
        headers = {
            "Authorization": "Bearer 786720e8eb68de7054d1149b56cc04f9",
            "Content-Type": "application/json"
        }

        # Fetch data from Bubble.io and convert to DataFrame
        bubble_df = fetch_and_convert_bubble_data_to_df(bubble_api_endpoint, headers)
        logging.debug(f"Bubble.io Data: {bubble_df.head()}")
        # Find unique entries
        key_columns = ['admissions-group-name','lead-name','project-name']
        is_unique = ~json_df[key_columns].apply(tuple,1).isin(bubble_df[key_columns].apply(tuple,1))
        unique_entries_df = json_df[is_unique]

        # Bulk upload unique entries to Bubble.io
        if not unique_entries_df.empty:
            bulk_upload_to_bubble(bubble_api_endpoint, headers, unique_entries_df)
        else:
            logging.info("No unique entries to upload.")

    except Exception as ex:
        logging.error("An error occurred: %s", ex)

if __name__ == "__main__":
    main()