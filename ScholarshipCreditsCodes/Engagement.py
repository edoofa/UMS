import pickle
import os.path
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import gspread
from datetime import datetime, timedelta
import requests
import pytz
import re
import logging


# Function to sanitize file names
def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', '_', filename)

# Create the folder if it doesn't exist
folder_name = "Kustomer Chats"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler('engagement_log.txt', 'a')  # Append mode to a file
    ]
)

# OAuth2 authentication setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def auth_gspread():
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
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return gspread.authorize(creds)

gc = auth_gspread()

# Access the Google Sheets
spreadsheet_id = "1BTmsnHrKO9NKigDiJdab7QssIl4mi2oBAqbmgTg4lFI"
spreadsheet = gc.open_by_key(spreadsheet_id)
sheet1 = spreadsheet.worksheet("Sheet1")  # Data appending sheet
sheet6 = spreadsheet.worksheet("Sheet6")  # User lookup sheet

# API setup
api_url = "https://edoofa.api.kustomerapp.com/v1/customers"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY1MGVjMDVkNjg4MGQwNmJlNTNjZWVhMyIsInVzZXIiOiI2NTBlYzA1YmEzODAxZTQ4YjAwOGNhMDQiLCJvcmciOiI2M2JiZGEzNzY4ODFhYTZlMzdmZGRjNmMiLCJvcmdOYW1lIjoiZWRvb2ZhIiwidXNlclR5cGUiOiJtYWNoaW5lIiwicG9kIjoicHJvZDIiLCJyb2xlcyI6WyJvcmcucGVybWlzc2lvbi5tZXNzYWdlLnJlYWQiLCJvcmcucGVybWlzc2lvbi5ub3RlLnJlYWQiLCJvcmcudXNlci5jdXN0b21lci5yZWFkIiwib3JnLnVzZXIubWVzc2FnZS5yZWFkIiwib3JnLnVzZXIubm90ZS5yZWFkIiwib3JnLnBlcm1pc3Npb24uY3VzdG9tZXIucmVhZCJdLCJhdWQiOiJ1cm46Y29uc3VtZXIiLCJpc3MiOiJ1cm46YXBpIiwic3ViIjoiNjUwZWMwNWJhMzgwMWU0OGIwMDhjYTA0In0.IJI5P-BtBDCda9faVA3gfUYHA_rOZnWYuGM0np2Fbng"  # Replace with your actual API key
headers = {
    "Authorization": f"Bearer {api_key}",
    "Accept": "application/json"
}

# Date setup for yesterday
timezone = pytz.timezone("Asia/Kolkata")
yesterday = datetime.now(timezone) - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# Fetch user IDs and names from Sheet6
user_ids = sheet6.col_values(1)[1:]  # Skipping the header row
user_names = sheet6.col_values(2)[1:]  # Skipping the header row

# Initialize an empty DataFrame
df_columns = ['Sent At', 'Preview', 'User Name', 'Customer Name', 'Direction']
df = pd.DataFrame(columns=df_columns)

logging.info("Starting to fetch and process customer data...")

# Initialize variables for customer data pagination
current_page = 1
total_pages = 1
customer_counter = 0  # Initialize a counter to track the number of customers processed


# Loop through all pages of customer data
while current_page <= total_pages:
    response = requests.get(f"{api_url}?page={current_page}&pageSize=100", headers=headers)
    page_data = response.json()
    total_pages = page_data.get('meta', {}).get('totalPages', total_pages)

    for customer in page_data['data']:
        customer_counter += 1
        customer_id = customer['id']
        first_name = customer['attributes'].get('firstName', 'Unknown')
        last_name = customer['attributes'].get('lastName', 'Unknown')
        name = f"{first_name} {last_name}"
        user_id = customer.get('relationships', {}).get('modifiedBy', {}).get('data', {}).get('id', "Unknown")

        logging.info(f"Processing customer {customer_counter}/{total_pages * 100}: Customer ID: {customer_id}")

        # Initialize variables for message pagination
        message_current_page = 1
        message_total_pages = 1

        # Fetch the first page of messages to get the total number of pages
        first_message_response = requests.get(f"{api_url}/{customer_id}/messages?page=1&pageSize=100", headers=headers)
        first_message_data = first_message_response.json()
        message_total_pages = first_message_data.get('meta', {}).get('totalPages', 1)

        # Loop through all pages of messages for the current customer
        while message_current_page <= message_total_pages:
            message_response = requests.get(f"{api_url}/{customer_id}/messages?page={message_current_page}&pageSize=100", headers=headers)
            message_data = message_response.json()

            for message in message_data['data']:
                sent_at = datetime.strptime(message['attributes']['sentAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.utc).astimezone(timezone)
                sent_at_str = sent_at.strftime("%m/%d/%Y %H:%M:%S")
                direction = message['attributes']['direction']
                preview = message['attributes']['preview']

                user_index = user_ids.index(user_id) if user_id in user_ids else -1
                user_name = user_names[user_index] if user_index != -1 else "Unknown"

                # Append a row to the DataFrame
                new_row = pd.DataFrame([{
                    'Sent At': sent_at_str,
                    'Preview': preview,
                    'User Name': user_name,
                    'Customer Name': name,
                    'Direction': direction
                }])
                df = pd.concat([df, new_row], ignore_index=True)

            # Increment the message page number after processing all messages on the current page
            message_current_page += 1

        logging.info(f"Finished processing messages for customer ID: {customer_id}")

    # Increment the customer page number for the next iteration
    current_page += 1

logging.info("All customer data and messages fetched and processed.")

# After processing all data and before appending it to Google Sheet
for customer_name, group in df.groupby('Customer Name'):
    sanitized_name = sanitize_filename(customer_name)  # Sanitize the customer name
    file_name = f"{sanitized_name}.txt"  # Use the sanitized name for the file
    file_path = os.path.join(folder_name, file_name)  # Construct the full file path
    with open(file_path, 'w', encoding='utf-8') as file:
        for index, row in group.iterrows():
            # Convert 'Sent At' to the desired format
            sent_at = datetime.strptime(row['Sent At'], "%m/%d/%Y %H:%M:%S").strftime("%d/%m/%y, %H:%M")
            if row['Direction'] == "in":
                # Format for messages sent by the customer
                message = f"{sent_at} - {row['Customer Name']}: {row['Preview']}\n"
            else:
                # Format for messages sent by the user
                message = f"{sent_at} - {row['User Name']}: {row['Preview']}\n"
            file.write(message)

logging.info("Text files created for each customer successfully.")

