import os

def remove_files_without_keyword(folder_path, keyword):
    # List all files in the folder
    files = os.listdir(folder_path)
    
    # Iterate through each file
    for file in files:
        # Check if the keyword is not in the filename
        if keyword not in file:
            # Construct the full file path
            file_path = os.path.join(folder_path, file)
            
            # Check if the path is a file (not a directory)
            if os.path.isfile(file_path):
                # Remove the file
                os.remove(file_path)
                print(f"Removed: {file_path}")

# Specify the folder path
folder_path = r'C:\Users\aditya\OneDrive\Documents\EdoofaUMS\ScholarshipCreditsCodes\Kustomer Chats'

# Specify the keyword
keyword = "EWYL"

# Call the function to remove files without the keyword
remove_files_without_keyword(folder_path, keyword)
    




    