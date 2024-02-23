import os
import requests
import json
import pickle
import pandas as pd
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
import base64
import io
from googleapiclient.http import MediaIoBaseDownload, HttpError

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def create_drive_service():
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']  # Adjust the scope as needed

    # Load the service account credentials (you should replace with your own credentials JSON file)
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json', scopes=SCOPES)

    # Create the Drive API service
    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service

def google_drive_auth():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None

    # Check if token.pickle file exists which stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service

def get_file_data(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        file_io.seek(0)
        # Encode file content as base64 and then decode to a string
        base64_encoded_string = base64.b64encode(file_io.read()).decode('utf-8')
        return base64_encoded_string  # Return the base64 encoded string
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        return None

def list_folder_contents(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    items = []
    page_token = None

    while True:
        response = service.files().list(q=query,
                                        spaces='drive',
                                        fields="nextPageToken, files(id, name, mimeType, createdTime, webViewLink)",
                                        pageToken=page_token).execute()
        items.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return items

def get_folder_details(service, folder_id):
    folder = service.files().get(fileId=folder_id, fields='createdTime').execute()
    return folder.get('createdTime', '')

def process_folder(service, folder_id, admissions_group_name, df, is_student_folder=False, parent_folder_link='',parent_folder_name=None,uploaded_date=None):
    #logging.info(f"Processing folder: {admissions_group_name}")
    items = list_folder_contents(service, folder_id)
    rows_to_add = []

    if is_student_folder and items:
        
        # Use the uploaded_date for the student folder if provided
        student_uploaded_date = pd.to_datetime(uploaded_date).strftime('%m/%d/%Y') if uploaded_date else ''
        rows_to_add.append({
            'admissions-group-name': admissions_group_name,
            'doc-type': 'Folder',
            'document-link': '',
            'fileorfolder-id': folder_id,
            'folder-link': parent_folder_link,
            'name': admissions_group_name,
            'status': 'Uploaded',
            'uploaded-date': student_uploaded_date,  # Use the provided uploaded_date here
            'file': None  # No file data for folders
        })

    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            #logging.info(f"Processing subfolder: {item['name']}")
            # For subfolders, use their own creation date
            
            subfolder_uploaded_date = pd.to_datetime(item.get('createdTime', '')).strftime('%m/%d/%Y') if item.get('createdTime') else ''
            rows_to_add.append({
                'admissions-group-name': admissions_group_name,
                'doc-type': 'Folder',
                'document-link': '',
                'fileorfolder-id': item['id'],
                'folder-link': item.get('webViewLink', ''),
                'name': item['name'],
                'status': 'Uploaded',
                'uploaded-date': subfolder_uploaded_date,
                'file': None  # No file data for folders
            })
            df = process_folder(service, item['id'], admissions_group_name, df, False, item.get('webViewLink', ''), item['name'])
        else:
           # logging.info(f"Processing file: {item['name']}")
            file_data = get_file_data(service, item['id'])
            # For files, use their own creation date
            file_uploaded_date = pd.to_datetime(item.get('createdTime', '')).strftime('%m/%d/%Y') if item.get('createdTime') else ''
            rows_to_add.append({
                'admissions-group-name': admissions_group_name,
                'doc-type': parent_folder_name,
                'document-link': item.get('webViewLink', ''),
                'fileorfolder-id': item['id'],
                'folder-link': parent_folder_link,
                'name': item['name'],
                'status': 'Uploaded',
                'uploaded-date': file_uploaded_date,
                'file': file_data  # Including base64 encoded file data
            })

    if rows_to_add:
        df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)

    return df

def file_upload_to_bubble(api_endpoint, headers, file_data, file_name):
    # Prepare the payload with the actual file content
    payload = {
        'file-name': file_name,
        'file': base64.b64encode(file_data).decode('utf-8')  # Base64 encode file data
    }

    # Send the POST request to Bubble's FileManager API
    response = requests.post(api_endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        # Parse the response to get the file URL
        response_data = response.json()
        file_url = response_data['response']['url']  # Adjust this based on the actual response structure
        logging.info(f"File '{file_name}' uploaded successfully. URL: {file_url}")
        return file_url
    else:
        logging.error(f"Error uploading '{file_name}': {response.text}")
        return None

def update_record_in_docs_table(api_endpoint, headers, record_id, file_url):
    payload = {
        'id': record_id, 
        'file': file_url
    }

    # Send the POST request to update the record in the docs table
    response = requests.post(api_endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        logging.info(f"Record {record_id} updated successfully with file URL.")
    else:
        logging.error(f"Error updating record {record_id}: {response.text}")

def process_and_upload_files(df, file_manager_endpoint, docs_table_endpoint, headers):
    for index, row in df.iterrows():
        if pd.notnull(row['file']): 
            file_data = base64.b64decode(row['file'])  
            file_url = file_upload_to_bubble(file_manager_endpoint, headers, file_data, row['name'])
            if file_url:
                # Update the DataFrame with the file URL, replacing the base64 encoded data or raw bytes
                df.at[index, 'file'] = file_url

    return df

def fetch_data_from_bubble(api_endpoint, headers, limit=100):
    all_data = []
    cursor = 0  # Initialize cursor to 0

    while True:
        paginated_endpoint = f"{api_endpoint}?limit={limit}&cursor={cursor}"
        try:
            response = requests.get(paginated_endpoint, headers=headers)
            response.raise_for_status()  # Raises HTTPError for bad responses

            data = response.json()['response']['results']
            if data:
                all_data.extend(data)
                logging.info(f'Fetched {len(data)} records in this iteration.')
                
                if len(data) < limit:
                    logging.info('No more records remaining. Exiting loop.')
                    break  # Exit loop if fewer records are returned than the limit

                cursor += len(data)  # Increment cursor by the number of records fetched

            else:
                logging.warning('No data found in this iteration.')
                break  # Exit loop if no data is found

        except requests.HTTPError as http_err:
            logging.error(f'HTTP error occurred while fetching data from Bubble: {http_err}')
            break  # Exit loop on HTTP error

        except Exception as e:
            logging.error(f'Error fetching data from Bubble: {e}')
            break  # Exit loop on any other error

    if all_data:
        df = pd.DataFrame(all_data)
        logging.info(f'Total records fetched: {len(all_data)}')
        return df
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data was fetched

def create_composite_key(row, key_columns):
    return '-'.join([str(row[col]) for col in key_columns])

def binary_search(arr, low, high, key):
    if high >= low:
        mid = (high + low) // 2

        if arr[mid] == key:
            return mid
        elif arr[mid] > key:
            return binary_search(arr, low, mid - 1, key)
        else:
            return binary_search(arr, mid + 1, high, key)
    else:
        return -1

def find_unique_entries(google_sheet_df, bubble_df, key_columns):
    for key_column in key_columns:
        if key_column not in google_sheet_df.columns or key_column not in bubble_df.columns:
            logging.error(f"Key column '{key_column}' not found in one of the DataFrames.")
            return pd.DataFrame()

    # Create composite keys for Google Sheets data and Bubble data
    google_sheet_df['composite_key'] = google_sheet_df.apply(lambda row: create_composite_key(row, key_columns), axis=1)
    bubble_df['composite_key'] = bubble_df.apply(lambda row: create_composite_key(row, key_columns), axis=1)

    # Sort the composite keys from Bubble data for binary search
    sorted_bubble_keys = sorted(bubble_df['composite_key'].tolist())

    unique_entries = []
    for _, row in google_sheet_df.iterrows():
        composite_key = row['composite_key']
        # Perform binary search on sorted composite keys from Bubble data
        result = binary_search(sorted_bubble_keys, 0, len(sorted_bubble_keys)-1, composite_key)

        if result == -1:  # Composite key not found in Bubble data
            logging.debug(f"Unique entry found: '{composite_key}' not in existing Bubble data.")
            unique_entries.append(row.drop('composite_key'))  # Drop the composite key before appending

    if not unique_entries:
      logging.info("No unique entries found.")
      return pd.DataFrame()
    else:
     unique_df = pd.DataFrame(unique_entries)
     logging.info(f"Found {len(unique_df)} unique entries.")
  
    if 'composite_key' in unique_df.columns:
        return unique_df.drop(columns=['composite_key'])
    else:
        return unique_df

def bulk_upload_to_bubble(api_endpoint, headers, data):
    records_to_upload = data.to_dict(orient='records')
    json_payload = '\n'.join(json.dumps(record) for record in records_to_upload)
    logging.debug("JSON payload being uploaded: " + json_payload)
    headers['Content-Type'] = 'text/plain'
    bulk_endpoint = f"{api_endpoint}/bulk"
    response = requests.post(bulk_endpoint, headers=headers, data=json_payload)

    logging.debug(f"Response Status Code: {response.status_code}")
    logging.debug(f"Response Body: {response.text}")

    if response.status_code != 200:
        logging.error("Error in bulk uploading to Bubble.io: " + response.text)
    else:
        logging.info("Bulk data uploaded successfully to Bubble.io")

def main():
    service = google_drive_auth()
    parent_folder_id = '1bg8OmaJMtnv3vRHK_RYPxR8RJA-JEr4g'  # Replace with your actual parent folder ID
    df_columns = ['admissions-group-name', 'doc-type', 'document-link', 'fileorfolder-id', 'folder-link', 'name', 'status', 'uploaded-date', 'file']
    df = pd.DataFrame(columns=df_columns)

    # Start processing from the parent folder
    student_folders = list_folder_contents(service, parent_folder_id)
    batch_size = 10  # Define the batch size

    logging.info(f"Total number of student folders: {len(student_folders)}")

    for i in range(0, len(student_folders), batch_size):
        batch_folders = student_folders[i:i+batch_size]
        logging.info(f"Processing batch {i//batch_size + 1} of {len(student_folders)//batch_size + 1}")

        for student_folder in batch_folders:
            logging.info(f"Processing folder: {student_folder['name']}")

            if student_folder['mimeType'] == 'application/vnd.google-apps.folder':
                uploaded_date = get_folder_details(service, student_folder['id'])
                student_folder_details = service.files().get(fileId=student_folder['id'], fields='webViewLink').execute()
                student_folder_link = student_folder_details.get('webViewLink', '')
                df = process_folder(service, student_folder['id'], student_folder['name'], df, is_student_folder=True, parent_folder_link=student_folder_link, parent_folder_name=student_folder['name'], uploaded_date=uploaded_date)

        # Fetch existing data from Bubble
        api_endpoint = "https://app.edoofa.com/api/1.1/obj/docs"
        headers = {
            "Authorization": "Bearer 786720e8eb68de7054d1149b56cc04f9"
        }
        bubble_df = fetch_data_from_bubble(api_endpoint, headers)
        key_columns = [
            'admissions-group-name',
            'doc-type',
            'fileorfolder-id',
            'name',
            'document-link'
        ]
        # Find unique entries compared to existing data in Bubble
        unique_entries_df = find_unique_entries(df, bubble_df,key_columns)

        if not unique_entries_df.empty:
            # Define your Bubble.io FileManager API endpoint for file upload
            file_manager_endpoint = "https://app.edoofa.com/version-test/api/1.1/wf/fileUploader"
            headers = {
            "Authorization": "Bearer 786720e8eb68de7054d1149b56cc04f9"
            }
            # Process and upload files for unique entries
            updated_unique_entries_df = process_and_upload_files(unique_entries_df, file_manager_endpoint,api_endpoint, headers)

            # Bulk upload updated unique entries to Bubble
            bulk_upload_to_bubble(api_endpoint, headers, updated_unique_entries_df)
            print(updated_unique_entries_df)
        else:
            print("No unique entries to upload.")

        updated_df = pd.concat([df, unique_entries_df]).drop_duplicates(subset=['admissions-group-name'], keep='last')
        updated_df.to_csv('output.csv', index=False)
        logging.info("Final DataFrame saved to 'output.csv'.")
        
if __name__ == '__main__':
    main()

