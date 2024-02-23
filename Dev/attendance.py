import nest_asyncio
nest_asyncio.apply()

import aiohttp
import asyncio
import json
import pandas as pd
from datetime import datetime 
from datetime import timedelta


student_df = None
engagement_df = None
ums_att_df = None
ums_avg_att_df = None
ums_upload_avg_att=None

edoofy_base_url = "https://edoofa-portal.bubbleapps.io/api/1.1/obj"
edoofy_bearer_token = "2cde31d8f48919a2db1467cc06a56132"
edoofy_headers = {'Authorization': f'Bearer {edoofy_bearer_token}'}

ums_base_url = "https://app.edoofa.com/version-test/api/1.1/obj"
ums_bearer_token = "786720e8eb68de7054d1149b56cc04f9"
ums_headers = {'Authorization': f'Bearer {ums_bearer_token}'}


async def fetch_table_data(session, base_url, headers, table, constraints=None):
    records = []
    cursor = 0
    total_fetched = 0

    while True:
        params = {'limit': 100, 'cursor': cursor}
        if constraints:
            params['constraints'] = json.dumps(constraints)

        api_url = f"{base_url}/{table}"
        print(f"Fetching {table} data from {base_url}... Cursor: {cursor}")

        async with session.get(api_url, headers=headers, params=params) as response:
            if response.status != 200:
                print(f"Failed to fetch data from {table}: {await response.text()}")
                break

            data = await response.json()
            new_records = data['response']['results']
            records.extend(new_records)
            total_fetched += len(new_records)

            print(f"Fetched {len(new_records)} new records, Total fetched: {total_fetched}")

            cursor += 100

            if len(new_records) < 100:
                print(f"Exiting loop, fetched less than 100 records.")
                break

    df = pd.DataFrame(records)
    print(f"Fetched {len(df)} records for {table}.")
    return df



def create_attendance_row(engagement_record):
    attendance_row = {
        'admissions-group-name': engagement_record['admissions-group-name'],  
        'attendance-type': engagement_record['engagement-type'],  
        'date': engagement_record['engagement-date'],  
        'ewyl-group-name': engagement_record['ewyl-group-name'],  
        'present': engagement_record['daily-attendance'],  
    }

    return attendance_row

async def post_attendance_record(session, url, headers, record):
    try:
        async with session.post(url, headers=headers, json=record) as response:
            if response.status == 200:
                print("Record posted successfully.")
                return await response.json()
            else:
                print(f"Failed to post record: {await response.text()}")
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


async def main():
    async with aiohttp.ClientSession() as session:
        ums_student_df = await fetch_table_data(session, ums_base_url, ums_headers, "Student")
        ums_att_df = await fetch_table_data(session, ums_base_url, ums_headers, "Attendance")  
        student_constraints = [{'key': 'indian-edoofian', 'constraint_type': 'equals', 'value': 'yes'}]
        student_df = await fetch_table_data(session, edoofy_base_url, edoofy_headers, "Student", constraints=student_constraints)
        
        engagement_df = pd.DataFrame()
        for engagement_type in ['IE Call', 'IE Chat', 'Activity', 'Lesson']:
            engagement_data = await fetch_table_data(session, edoofy_base_url, edoofy_headers, "Engagement", [{'key': 'engagement-type', 'constraint_type': 'equals', 'value': engagement_type}])
            engagement_df = pd.concat([engagement_df, engagement_data], ignore_index=True)
        
        engagement_df = pd.merge(engagement_df, student_df[['_id', 'KAM-group-name', 'EWYL-group-name']], left_on='student', right_on='_id', how='left')
        engagement_df.rename(columns={'KAM-group-name': 'admissions-group-name', 'EWYL-group-name': 'ewyl-group-name', '_id_x': '_id'}, inplace=True)
        engagement_df.drop(['_id_y'], axis=1, inplace=True)
        engagement_df.dropna(subset=['admissions-group-name'], inplace=True)
        
        # Convert dates to datetime objects and ensure they are tz-naive
        ums_att_df['date'] = pd.to_datetime(ums_att_df['date']).dt.tz_localize(None)
        engagement_df['engagement-date'] = pd.to_datetime(engagement_df['engagement-date']).dt.tz_localize(None)
        engagement_df['engagement-date'] = pd.to_datetime(engagement_df['engagement-date']) + timedelta(hours=5, minutes=30)
        
        # Create 'comparison-date' in 'ums_att_df' with just the date part
        ums_att_df['date'] = ums_att_df['date'].dt.date
        # Create 'comparison-date' in 'engagement_df' with just the date part
        engagement_df['engagement-date'] = engagement_df['engagement-date'].dt.date
        
        new_attendance_records = []
        
        for _, student in ums_student_df.iterrows():
            admissions_group_name = student['admissions-group-name']
            print(f"Processing admissions group: {admissions_group_name}")

            # Dynamically determine the latest attendance date for each admissions group
            if admissions_group_name in ums_att_df['admissions-group-name'].values:
                group_attendance = ums_att_df[ums_att_df['admissions-group-name'] == admissions_group_name]
                if not group_attendance.empty:
                    latest_attendance_date = group_attendance['date'].max()
                else:
                    latest_attendance_date = pd.Timestamp('1900-01-01')  # Default old date for new admissions groups
            else:
                latest_attendance_date = pd.Timestamp('1900-01-01')  # Default old date for new admissions groups
            print(latest_attendance_date)
            engagements = engagement_df[engagement_df['admissions-group-name'] == admissions_group_name]
            
            for _, engagement in engagements.iterrows():
                if engagement['engagement-date'] > latest_attendance_date:
                    new_row = create_attendance_row(engagement)
                    new_attendance_records.append(new_row)

        new_attendance_df = pd.DataFrame(new_attendance_records)

        # Post new attendance records if any
        if not new_attendance_df.empty:
            new_attendance_df['date'] = new_attendance_df['date'].dt.strftime('%m/%d/%Y') + " 05:30 AM"
            for _, record in new_attendance_df.iterrows():
                admissions_group_name = record['admissions-group-name']
                print(f"Posting attendance for admissions group: {admissions_group_name}")
                url = f"{ums_base_url}/Attendance"
                await post_attendance_record(session, url, ums_headers, record.to_dict())
        else:
            print("No new attendance records to process.")
        
        
        
        print(new_attendance_df.head())
        return new_attendance_df


await main()

