import nest_asyncio
nest_asyncio.apply()

import aiohttp
import asyncio
import json
import pandas as pd
from datetime import datetime 
from datetime import timedelta

# Define global variables for dataframes
student_df = None
engagement_df = None
ums_att_df = None
ums_avg_att_df = None
ums_upload_avg_att=None

# Edoofy app information
edoofy_base_url = "https://edoofa-portal.bubbleapps.io/api/1.1/obj"
edoofy_bearer_token = "2cde31d8f48919a2db1467cc06a56132"
edoofy_headers = {'Authorization': f'Bearer {edoofy_bearer_token}'}

# UMS app information
ums_base_url = "https://edoofa-ums-90164.bubbleapps.io/version-test/api/1.1/obj"
ums_bearer_token = "786720e8eb68de7054d1149b56cc04f9"
ums_headers = {'Authorization': f'Bearer {ums_bearer_token}'}

# Asynchronous function to fetch data from a table
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

def map_students_to_engagement(student_df, engagement_df):
    # Ensure '_id' and 'EWYL-group-name' are in student_df
    if '_id' in student_df.columns and 'EWYL-group-name' in student_df.columns:
        student_id_to_EWYL = dict(zip(student_df['_id'], student_df['EWYL-group-name']))
        engagement_df['ewyl'] = engagement_df['student'].map(student_id_to_EWYL)
    else:
        print("Error: '_id' or 'EWYL-group-name' not in student_df columns")
    return engagement_df



def process_engagement_data(ums_att_df, engagement_df, student_df, ewyl_group):
    new_ums_df = pd.DataFrame(columns=['admissions-group-name', 'attendance-type', 'date',
                                       'ewyl-group-name', 'present', 'webinar'])
    # Create mappings
    student_to_kam_mapping = dict(zip(student_df['_id'], student_df['KAM-group-name']))
    student_to_ewyl_mapping = dict(zip(student_df['_id'], student_df['EWYL-group-name']))

    # Add KAM-group-name and ewyl columns to the engagement dataframe
    engagement_df['KAM-group-name'] = engagement_df['student'].map(student_to_kam_mapping)
    engagement_df['ewyl'] = engagement_df['student'].map(student_to_ewyl_mapping)

    # Find the latest date for the specific student in ums_att_df
    student_latest_ums_date = pd.to_datetime(ums_att_df[ums_att_df['ewyl-group-name'] == ewyl_group]['date'].max())

    if pd.isnull(student_latest_ums_date):
        # Process all records from engagement_df for this student
        new_ums_df['ewyl-group-name'] = engagement_df['ewyl']
        new_ums_df['admissions-group-name'] = engagement_df['KAM-group-name']
        new_ums_df['attendance-type'] = engagement_df['engagement-type']
        new_ums_df['date'] = engagement_df['engagement-date']
        new_ums_df['present'] = engagement_df['daily-attendance']
        
        print("Adding all records")
    else:
        # Process only records newer than the latest in ums_att_df
        filtered_engagement_df = engagement_df[pd.to_datetime(engagement_df['engagement-date']) > student_latest_ums_date]
        new_ums_df['ewyl-group-name'] = filtered_engagement_df['ewyl']
        new_ums_df['admissions-group-name'] = filtered_engagement_df['KAM-group-name']
        new_ums_df['attendance-type'] = filtered_engagement_df['engagement-type']
        new_ums_df['date'] = filtered_engagement_df['engagement-date']
        new_ums_df['present'] = filtered_engagement_df['daily-attendance']
        
        print("Adding new records")

    return new_ums_df
    

# Function to post processed engagement data
async def post_processed_data(session, base_url, headers, processed_data):
    api_url = f"{base_url}/Attendance"
    
    for index, row in processed_data.iterrows():
        data = {
            'admissions-group-name': row['admissions-group-name'],
            'ewyl-group-name': row['ewyl-group-name'],
            'attendance-type': row['attendance-type'],
            'present': row['present'],
            'date': row['date']
        }
        
        async with session.post(api_url, headers=headers, json=data) as response:
            if response.status == 201:
                print(f"Successfully posted data for row {index}")
            else:
                print(f"Failed to post data for row {index}: {await response.text()}")



def get_percentage(number_of_present, total_sessions):
    if total_sessions == 0:
        return 0  # Avoid division by zero
    percent = (number_of_present / total_sessions) * 100
    #print(percent)
    return percent

# Function to check if a row exists for the current month for a student
async def check_if_row_exists(student, year, month, ums_avg_att_df):
    # Filtering the ums_avg_att_df to check if there's an existing record for the student
    existing_rows = ums_avg_att_df[
        (ums_avg_att_df['ewyl-group-name'] == student) &
        (ums_avg_att_df['year'] == year) &
        (ums_avg_att_df['month'] == month)
    ]

    if not existing_rows.empty:
        # Assuming '_id' is the column name for the row ID
        return existing_rows['_id'].iloc[0]
    else:
        return False

# Function to post a new attendance summary
async def post_new_attendance_summary(session, base_url, headers, data):
    api_url = f"{base_url}/Attendance-Summary"
    async with session.post(api_url, headers=headers, json=data) as response:
        if response.status == 201:
            print("Successfully created a new row in Attendance-Summary.")
        else:
            print(f"Failed to create a new row: {await response.text()}")

# Function to patch an existing attendance summary
async def patch_attendance_summary(session, base_url, headers, row_id, data):
    api_url = f"{base_url}/Attendance-Summary/{row_id}"
    async with session.patch(api_url, headers=headers, json=data) as response:
        if response.status == 200:
            print("Successfully updated the row in Attendance-Summary.")
        else:
            print(f"Failed to update the row: {await response.text()}")

            
async def process_att_summary(session, base_url, headers, combined_df, ums_avg_att_df):
    att_summary_df = pd.DataFrame(columns=['admissions-group-name', 'attendance-percentage', 'avg-att-percent-till-last-month',
                                           'ewyl-group-name', 'first-day-of-month', 'month', 'year'])
    combined_df['date'] = pd.to_datetime(combined_df['date'])    
    unique_students = combined_df['admissions-group-name'].unique()

    for student in unique_students:
        student_df = combined_df[combined_df['admissions-group-name'] == student]

        # Get the earliest date in student_df for that student and set first_date as the 1st of that month
        first_date = pd.to_datetime(student_df['date'].min())
        first_day_current_month = first_date.replace(day=1)

        while True:
            this_month_df = student_df[(student_df['date'].dt.year == first_day_current_month.year) &
                                       (student_df['date'].dt.month == first_day_current_month.month)]

            prev_month_df = student_df[student_df['date'] < first_day_current_month]
            
            # Calculate attendance percentage for this month using get_percentage
            total_sessions_this_month = len(this_month_df)
            attended_sessions_this_month = this_month_df['present'].sum()  # Sum of True values in the 'present' column
            attendance_percentage_this_month = get_percentage(attended_sessions_this_month, total_sessions_this_month)

            
            # Calculate average attendance till last month
            if prev_month_df.empty:
                avg_att_till_last_month = 0
            else:
                total_sessions_till_last_month = len(prev_month_df)
                attended_sessions_till_last_month = prev_month_df['present'].sum()  # Sum of True values in the 'present' column
                avg_att_till_last_month = get_percentage(attended_sessions_till_last_month, total_sessions_till_last_month)
   
            # Add the new row to att_summary_df
            new_row = {
                'admissions-group-name': student,
                'attendance-percentage': attendance_percentage_this_month,
                'avg-att-percent-till-last-month': avg_att_till_last_month,
                'ewyl-group-name': student_df['ewyl-group-name'].iloc[0] if not student_df['ewyl-group-name'].empty else 'N/A',
                'first-day-of-month': first_day_current_month.strftime("%Y-%m-%d"),
                'month': first_day_current_month.strftime("%B"),
                'year': first_day_current_month.year
            }
            att_summary_df = pd.concat([att_summary_df, pd.DataFrame([new_row])], ignore_index=True)
            
            #print("attendance summary")
            #print(att_summary_df)

            # Calculate the first day of the next month
            next_month_first_day = (first_day_current_month + pd.offsets.MonthBegin()).normalize()
            
            # Update first_day_current_month to the first day of the next month
            first_day_current_month = next_month_first_day
            
            # Check if there is data for the next month
            if student_df[student_df['date'] >= first_day_current_month].empty:
                break  # Exit the loop if there's no data for the next month

            

        for index, row in att_summary_df.iterrows():
                row_id = await check_if_row_exists(row['ewyl-group-name'], row['year'], row['month'], ums_avg_att_df)

                data = {
                    'admissions-group-name': row['admissions-group-name'],
                    'attendance-percentage': row['attendance-percentage'],
                    'avg-att-percent-till-last-month': row['avg-att-percent-till-last-month'],
                    'ewyl-group-name': row['ewyl-group-name'],  
                    'first-day-of-month': row['first-day-of-month'],  
                    'month': row['month'],  
                    'year': row['year']  
                }

                # Convert any Pandas Series or non-serializable types to native Python types
                for key, value in data.items():
                    if isinstance(value, pd.Series):
                        data[key] = value.iloc[0]  # Assuming you want the first element of the Series

                if row_id:
                    await patch_attendance_summary(session, base_url, headers, row_id, data)
                    print("patch function")
                else:
                    await post_new_attendance_summary(session, base_url, headers, data)
                    print("post function")

    return att_summary_df


async def main():
    global student_df, engagement_this_month_df, engagement_previous_months_df, ums_att_df, ums_avg_att_df
    
    async with aiohttp.ClientSession() as session:
        # Fetch data from 'ums_att'
        ums_att_df = await fetch_table_data(session, ums_base_url, ums_headers, "Attendance")
        
        ums_avg_att_df = await fetch_table_data(session, ums_base_url, ums_headers, "Attendance-Summary")
        
        # Determine the latest date from 'ums_att'
        ums_latest = pd.to_datetime(ums_att_df['date'].max())
        
        # Fetch Student table from Edoofy
        student_constraints = [           
            {'key': '_id', 'constraint_type': 'equals', 'value': '1695736310279x550027498072956700'}
        ]
        student_df = await fetch_table_data(session, edoofy_base_url, edoofy_headers, "Student", constraints=student_constraints)
        
        # Fetch engagement data where 'engagement-date' is greater than 'ums_latest'
        engagement_constraints = [
            #{'key': 'engagement-date', 'constraint_type': 'greater than', 'value': ums_latest.isoformat()},
            {'key': 'student', 'constraint_type': 'equals', 'value': '1695736310279x550027498072956700'}
        ]
        engagement_df = await fetch_table_data(session, edoofy_base_url, edoofy_headers, "Engagement", constraints=engagement_constraints)
        engagement_df = engagement_df[engagement_df['engagement-type'].isin(['IE Call', 'IE Chat', 'Activity', 'Lesson'])]
        
        # Calculate the start and end date for the till last month (excluding this month)
        latest_engagement_date = pd.to_datetime(engagement_df['engagement-date'].max())
        end_date = latest_engagement_date.replace(day=1) - timedelta(days=1)
       # start_date = end_date - timedelta(days=365)

        # Filter engagement data for the till last month
        previous_months_df = engagement_df[            
            (pd.to_datetime(engagement_df['engagement-date']) <= end_date)
        ]

        # Split the data into this month and previous months
        this_month_start = latest_engagement_date.replace(day=1)
        engagement_this_month_df = engagement_df[
            pd.to_datetime(engagement_df['engagement-date']) >= this_month_start
        ]
        engagement_previous_months_df = previous_months_df
        
        # Apply mapping to engagement dataframes
        engagement_this_month_df = map_students_to_engagement(student_df, engagement_this_month_df)
        engagement_previous_months_df = map_students_to_engagement(student_df, engagement_previous_months_df)
        
        all_processed_data = []

        for _, student_row in student_df.iterrows():
            ewyl_group = student_row['EWYL-group-name']
            processed_df = process_engagement_data(ums_att_df, engagement_df, student_df, ewyl_group)
            # Post processed data for each student
            if not processed_df.empty:
                await post_processed_data(session, ums_base_url, ums_headers, processed_df)
                all_processed_data.append(processed_df)
            else:
                print(f"No data to post for student {ewyl_group}")
        
        combined_df = pd.concat([ums_att_df] + all_processed_data, ignore_index=True)
        print("All Attendance Posted till this point")
        # Process average attendance
        await process_att_summary(session, ums_base_url, ums_headers, combined_df, ums_avg_att_df)


await main()
