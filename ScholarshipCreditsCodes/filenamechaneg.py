import os
import zipfile

def zip_folder(folder_path, output_path):
    # Create a zip file
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        # Walk through the directory
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Add each file to the zip file
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, folder_path))

# Specify the path to your folder
folder_path = r'C:\Users\aditya\OneDrive\Documents\EdoofaUMS\ScholarshipCreditsCodes\Kustomer Chats'
# Specify the output zip file path
output_path = r'C:\Users\aditya\OneDrive\Documents\EdoofaUMS\ScholarshipCreditsCodes\output.zip'

# Call the function to zip the folder
zip_folder(folder_path, output_path)

print("Folder zipped successfully!")
