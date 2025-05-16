import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# Define the scopes needed for Google Drive API access
SCOPES = ['https://www.googleapis.com/auth/drive']

class GDriveError(Exception):
    """Custom exception for Google Drive API errors."""
    pass

def initialize_gdrive_service():
    """
    Initializes and returns the Google Drive API service client
    using credentials from a service account JSON file.
    The path to the JSON file is expected in the 'GDRIVE_SA_KEY_PATH' environment variable.

    Returns:
        A Google Drive API service object.

    Raises:
        ValueError: If the environment variable is not set or the file doesn't exist.
        GDriveError: If there's an issue building the service.
    """
    key_path = os.getenv("GDRIVE_SA_KEY_PATH")
    if not key_path:
        raise ValueError("GDRIVE_SA_KEY_PATH environment variable not set.")
    if not os.path.exists(key_path):
        raise ValueError(f"Service account key file not found at: {key_path}")

    try:
        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        return service
    except Exception as e:
        raise GDriveError(f"Failed to initialize Google Drive service: {e}") from e

def upload_file_to_gdrive(service, file_path: str, folder_id: str, file_name: str | None = None) -> str | None:
    """
    Uploads a file to a specified Google Drive folder.

    Args:
        service: Authorized Google Drive API service instance.
        file_path: The local path to the file to be uploaded.
        folder_id: The ID of the Google Drive folder where the file will be uploaded.
        file_name: Optional. The name the file should have on Google Drive. 
                   If None, uses the local file's name.

    Returns:
        The file ID of the uploaded file, or None if upload fails.
    
    Raises:
        FileNotFoundError: If the local file_path does not exist.
        GDriveError: If the API request fails.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File to upload not found: {file_path}")

    if file_name is None:
        file_name = os.path.basename(file_path)

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, resumable=True)
    
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        return file.get('id')
    except HttpError as error:
        raise GDriveError(f"An API error occurred during file upload: {error}") from error
    except Exception as e:
        raise GDriveError(f"An unexpected error occurred during file upload: {e}") from e

def create_shareable_link(service, file_id: str, role: str = 'reader', type: str = 'anyone') -> str | None:
    """
    Creates a shareable link for a file in Google Drive, making it publicly readable.

    Args:
        service: Authorized Google Drive API service instance.
        file_id: The ID of the file in Google Drive.
        role: The role to grant. Defaults to 'reader'. Other options: 'writer', 'commenter'.
        type: The type of permission. Defaults to 'anyone'. Other options: 'user', 'group', 'domain'.

    Returns:
        The webViewLink (shareable URL) of the file, or None if it fails.

    Raises:
        GDriveError: If the API request fails.
    """
    try:
        permission = {
            'type': type,
            'role': role
        }
        service.permissions().create(fileId=file_id, body=permission).execute()
        
        # Retrieve the file metadata to get the webViewLink
        file_metadata = service.files().get(fileId=file_id, fields='webViewLink').execute()
        return file_metadata.get('webViewLink')
    except HttpError as error:
        raise GDriveError(f"An API error occurred while creating shareable link: {error}") from error
    except Exception as e:
        raise GDriveError(f"An unexpected error occurred while creating shareable link: {e}") from e

if __name__ == '__main__':
    # Example Usage: python -m utils.gdrive_utils
    # Ensure .env file has GDRIVE_SA_KEY_PATH="/path/to/your/service_account.json"
    # And replace 'your_gdrive_folder_id_here' with an actual folder ID.
    
    # from dotenv import load_dotenv
    # load_dotenv()

    print("Attempting to initialize Google Drive service...")
    try:
        drive_service = initialize_gdrive_service()
        print("Google Drive service initialized successfully.")
    except (ValueError, GDriveError) as e:
        print(f"Error initializing GDrive service: {e}")
        exit(1)

    # --- Test upload_file_to_gdrive and create_shareable_link ---
    # TEST_FOLDER_ID = "your_gdrive_folder_id_here" # Replace with your folder ID
    # TEST_FILE_NAME = "test_upload_gdrive_utils.txt"

    # if TEST_FOLDER_ID != "your_gdrive_folder_id_here":
    #     # Create a dummy file for testing upload
    #     dummy_file_path = "temp_gdrive_upload_test.txt"
    #     with open(dummy_file_path, "w") as f:
    #         f.write("This is a test file for Google Drive upload from gdrive_utils.py.")
        
    #     print(f"\nAttempting to upload '{dummy_file_path}' to folder ID: {TEST_FOLDER_ID} as '{TEST_FILE_NAME}'")
    #     uploaded_file_id = None
    #     try:
    #         uploaded_file_id = upload_file_to_gdrive(
    #             service=drive_service, 
    #             file_path=dummy_file_path, 
    #             folder_id=TEST_FOLDER_ID,
    #             file_name=TEST_FILE_NAME
    #         )
    #         if uploaded_file_id:
    #             print(f"File uploaded successfully. File ID: {uploaded_file_id}")
                
    #             print(f"\nAttempting to create a shareable link for file ID: {uploaded_file_id}")
    #             shareable_link = create_shareable_link(drive_service, uploaded_file_id)
    #             if shareable_link:
    #                 print(f"Shareable link created: {shareable_link}")
    #             else:
    #                 print("Failed to create shareable link.")
    #         else:
    #             print("File upload failed.")

    #     except FileNotFoundError as e:
    #         print(f"File Error: {e}")
    #     except GDriveError as e:
    #         print(f"Google Drive API Error: {e}")
    #     except Exception as e:
    #         print(f"An unexpected error occurred: {e}")
    #     finally:
    #         # Clean up the dummy file
    #         if os.path.exists(dummy_file_path):
    #             os.remove(dummy_file_path)
    #             print(f"\nCleaned up temporary file: {dummy_file_path}")
    # else:
    #     print("\nSkipping GDrive upload/sharing tests: TEST_FOLDER_ID not set.")

    print("\nFinished gdrive_utils.py example execution.")

