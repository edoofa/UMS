import os
import requests
import json
import logging
import pickle
import time
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
import base64
import io
from googleapiclient.http import MediaIoBaseDownload, HttpError

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom JSON encoder to handle NaN values
class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isna(obj):
            return None  # Converts NaN values to None
        return json.JSONEncoder.default(self, obj)


def create_drive_service():
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json', scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service

def google_drive_auth():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
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
        return base64.b64encode(file_io.read()).decode('utf-8')
    except HttpError as error:
        return None


def list_folder_contents(service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    items = []
    page_token = None
    while True:
        response = service.files().list(q=query, spaces='drive', fields="nextPageToken, files(id, name, mimeType, createdTime, webViewLink)", pageToken=page_token).execute()
        items.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return items

def get_folder_details(service, folder_id):
    folder = service.files().get(fileId=folder_id, fields='createdTime').execute()
    return folder.get('createdTime', '')

def process_folder(service, folder_id, admissions_group_name, df, is_student_folder=False, parent_folder_link='', parent_folder_name=None, uploaded_date=None):
    items = list_folder_contents(service, folder_id)
    rows_to_add = []

    if is_student_folder and items:
        student_uploaded_date = pd.to_datetime(uploaded_date).strftime('%m/%d/%Y') if uploaded_date else ''
        rows_to_add.append({
            'admissions-group-name': admissions_group_name,
            'doc-type': 'Folder',
            'document-link': '',
            'fileorfolder-id': folder_id,
            'folder-link': parent_folder_link,
            'name': admissions_group_name,
            'status': 'Uploaded',
            'uploaded-date': student_uploaded_date,
            'file': None
        })

    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
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
                'file': None
            })
            df = process_folder(service, item['id'], admissions_group_name, df, False, item.get('webViewLink', ''), item['name'])
        else:
            file_data = get_file_data(service, item['id'])
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
                'file': file_data
            })

    if rows_to_add:
        df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)

    return df

def process_and_upload_files(df, file_manager_endpoint, headers):
    for index, row in df.iterrows():
        if pd.notnull(row['file']):
            file_data = base64.b64decode(row['file'])
            payload = {'file-name': row['name'], 'file': base64.b64encode(file_data).decode('utf-8')}
            response = requests.post(file_manager_endpoint, headers=headers, json=payload)
            if response.status_code == 200:
                file_url = response.json()['response']['url']
                df.at[index, 'file'] = file_url
            else:
                df.at[index, 'file'] = None
    return df

def fetch_data_from_bubble(api_endpoint, headers, limit=100):
    all_data = []
    cursor = 0
    while True:
        paginated_endpoint = f"{api_endpoint}?limit={limit}&cursor={cursor}"
        response = requests.get(paginated_endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()['response']['results']
            if data:
                all_data.extend(data)
                if len(data) < limit:
                    break
                cursor += len(data)
            else:
                break
        else:
            break
    return pd.DataFrame(all_data)

def find_unique_entries(google_sheet_df, bubble_df, key_columns):
    if not all(col in google_sheet_df.columns and col in bubble_df.columns for col in key_columns):
        return pd.DataFrame()
    google_sheet_df['composite_key'] = google_sheet_df.apply(lambda row: '-'.join([str(row[col]) for col in key_columns]), axis=1)
    bubble_df['composite_key'] = bubble_df.apply(lambda row: '-'.join([str(row[col]) for col in key_columns]), axis=1)
    unique_entries = google_sheet_df[~google_sheet_df['composite_key'].isin(bubble_df['composite_key'])].drop(columns=['composite_key'])
    return unique_entries

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



def main():
    service = google_drive_auth()
    parent_folder_id = '1bg8OmaJMtnv3vRHK_RYPxR8RJA-JEr4g'
    df_columns = ['admissions-group-name', 'doc-type', 'document-link', 'fileorfolder-id', 'folder-link', 'name', 'status', 'uploaded-date', 'file']
    df = pd.DataFrame(columns=df_columns)

    student_folders = list_folder_contents(service, parent_folder_id)
    batch_size = 10  # Define the batch size
    num_batches = (len(student_folders) + batch_size - 1) // batch_size  # Calculate the number of batches

    logging.info(f"Total number of student folders: {len(student_folders)}")
    logging.info(f"Total number of batches: {num_batches}")

    for batch_num in range(num_batches):
        logging.info(f"Processing batch {batch_num + 1} of {num_batches}")
        
        # Calculate the start and end indices for the current batch
        start_index = batch_num * batch_size
        end_index = min((batch_num + 1) * batch_size, len(student_folders))

        # Get the student folders for the current batch
        batch_folders = student_folders[start_index:end_index]

        # Process each folder in the current batch
        for student_folder in batch_folders:
            folder_name = student_folder['name']
            logging.info(f"Processing folder: {folder_name}")
            if student_folder['mimeType'] == 'application/vnd.google-apps.folder':
                uploaded_date = get_folder_details(service, student_folder['id'])
                student_folder_details = service.files().get(fileId=student_folder['id'], fields='webViewLink').execute()
                student_folder_link = student_folder_details.get('webViewLink', '')
                df = process_folder(service, student_folder['id'], student_folder['name'], df, is_student_folder=True, parent_folder_link=student_folder_link, parent_folder_name=student_folder['name'], uploaded_date=uploaded_date)

    api_endpoint = "https://app.edoofa.com/api/1.1/obj/docs"
    headers = {"Authorization": "Bearer 786720e8eb68de7054d1149b56cc04f9"}
    bubble_df = fetch_data_from_bubble(api_endpoint, headers)
    key_columns = ['admissions-group-name', 'doc-type', 'fileorfolder-id', 'name']
    unique_entries_df = find_unique_entries(df, bubble_df, key_columns)

    if not unique_entries_df.empty:
        file_manager_endpoint = "https://app.edoofa.com/api/1.1/wf/fileUploader"
        updated_unique_entries_df = process_and_upload_files(unique_entries_df, file_manager_endpoint, headers)
        bulk_upload_to_bubble(api_endpoint, headers, updated_unique_entries_df)
        print(updated_unique_entries_df)
    else:
        print("No unique entries to upload.")

    updated_df = pd.concat([df, unique_entries_df])
    updated_df.to_csv('output.csv', index=False)
    print("Final DataFrame saved to 'output.csv'.")




if __name__ == '__main__':
    main()
