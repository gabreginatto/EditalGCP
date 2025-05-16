import os
import requests
import json
from datetime import datetime

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

class NotionAPIError(Exception):
    """Custom exception for Notion API errors."""
    pass

def initialize_notion_client():
    """Initializes and returns the Notion API token from environment variables."""
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise ValueError("NOTION_TOKEN environment variable not set.")
    return token

def _get_headers(token):
    """Returns the standard headers for Notion API requests."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }

def get_existing_tender_ids(database_id: str, tender_id_property_name: str = "ID Licitação") -> set[str]:
    """
    Retrieves existing tender IDs from a Notion database to avoid duplicates.

    Args:
        database_id: The ID of the Notion database.
        tender_id_property_name: The name of the property in Notion that stores the tender ID.
                                Defaults to 'ID Licitação'.

    Returns:
        A set of existing tender IDs.

    Raises:
        NotionAPIError: If the API request fails.
    """
    token = initialize_notion_client()
    headers = _get_headers(token)
    url = f"{NOTION_API_URL}/databases/{database_id}/query"
    
    existing_ids = set()
    has_more = True
    start_cursor = None

    while has_more:
        payload = {}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
            data = response.json()
        except requests.exceptions.RequestException as e:
            raise NotionAPIError(f"Failed to query Notion database {database_id}: {e}") from e
        except json.JSONDecodeError as e:
             raise NotionAPIError(f"Failed to parse JSON response from Notion API for database {database_id}: {e}. Response text: {response.text}") from e

        for page in data.get("results", []):
            properties = page.get("properties", {})
            tender_id_obj = properties.get(tender_id_property_name, {})
            
            # Handle different property types for tender ID (rich_text or title)
            if tender_id_obj:
                if tender_id_obj.get("type") == "rich_text" and tender_id_obj.get("rich_text"):
                    tender_text = tender_id_obj["rich_text"][0].get("plain_text")
                    if tender_text:
                         existing_ids.add(tender_text)
                elif tender_id_obj.get("type") == "title" and tender_id_obj.get("title"):
                    tender_text = tender_id_obj["title"][0].get("plain_text")
                    if tender_text:
                        existing_ids.add(tender_text)
        
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return existing_ids

def create_tender_page(
    database_id: str,
    tender_id: str,
    title: str,
    source_url: str,
    downloaded_zip_path: str | None = None, # To be used later for GDrive link
    ai_summary: str | None = None,
    status: str = "Novo", # Default status
    company_id: str | None = None,
    gdrive_link: str | None = None, 
    additional_properties: dict | None = None
) -> dict:
    """
    Creates a new page in a Notion database for a tender.

    Args:
        database_id: The ID of the Notion database where the page will be created.
        tender_id: The unique identifier for the tender (e.g., 'SANEAGO-123/2024').
        title: The title or object of the tender.
        source_url: The URL where the tender was found.
        downloaded_zip_path: Local path to the downloaded ZIP file (kept for potential future use, not directly sent to Notion).
        ai_summary: AI-generated summary of the tender document.
        status: The current status of the tender (e.g., 'Novo', 'Em Análise', 'Arquivado').
        company_id: The ID of the company associated with the tender.
        gdrive_link: The shareable Google Drive link for the tender documents.
        additional_properties: A dictionary of other properties to set, with keys as Notion property names
                               and values as their corresponding Notion property objects.
                               Example: {"Número Processo": {"rich_text": [{"text": {"content": "12345"}}]}}

    Returns:
        The JSON response from the Notion API after creating the page.

    Raises:
        NotionAPIError: If the API request fails.
    """
    token = initialize_notion_client()
    headers = _get_headers(token)
    url = f"{NOTION_API_URL}/pages"

    properties = {
        "ID Licitação": {"title": [{"text": {"content": tender_id}}]}, 
        "Título (Objeto)": {"rich_text": [{"text": {"content": title}}]}, 
        "URL Fonte": {"url": source_url},
        "Status": {"select": {"name": status}},
        "Data Descoberta": {"date": {"start": datetime.utcnow().isoformat()}},
    }

    if company_id:
        properties["Empresa (ID)"] = {"rich_text": [{"text": {"content": company_id}}]} 
    
    if ai_summary:
        summary_content = ai_summary[:2000] # Notion API limit
        properties["Resumo AI"] = {"rich_text": [{"text": {"content": summary_content}}]} 

    if gdrive_link:
        properties["Link Drive"] = {"url": gdrive_link} 

    if additional_properties:
        properties.update(additional_properties)

    page_data = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }

    try:
        response = requests.post(url, headers=headers, json=page_data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_content = response.text if response else "No response content"
        try:
            error_details = response.json() if response else {}
        except json.JSONDecodeError:
            error_details = {}
        raise NotionAPIError(
            f"Failed to create Notion page for tender {tender_id} in database {database_id}: {e}. "
            f"Status: {response.status_code if response else 'N/A'}. Response: {error_content}. Details: {error_details}"
        ) from e
    except json.JSONDecodeError as e:
        raise NotionAPIError(f"Failed to parse JSON response when creating Notion page for tender {tender_id}. Response text: {response.text}") from e


if __name__ == '__main__':
    # Example Usage: python -m utils.notion_utils
    # Ensure .env file has NOTION_TOKEN="secret_..."
    # from dotenv import load_dotenv
    # load_dotenv()

    print("Attempting to initialize Notion client...")
    try:
        token = initialize_notion_client()
        print(f"Notion client initialized. Token: {token[:5]}...")
    except ValueError as e:
        print(f"Error: {e}")
        exit(1)

    # Example DB ID for testing - replace with your actual DB ID
    # TEST_DB_ID = "your_actual_notion_database_id_here" 
    # TENDER_ID_PROP_NAME = "ID Licitação" # Or whatever your ID property is named

    # if TEST_DB_ID != "your_actual_notion_database_id_here":
    #     print(f"\n--- Testing get_existing_tender_ids from {TEST_DB_ID} ---")
    #     try:
    #         existing_ids = get_existing_tender_ids(TEST_DB_ID, tender_id_property_name=TENDER_ID_PROP_NAME)
    #         print(f"Found {len(existing_ids)} IDs: {existing_ids if existing_ids else 'None found or DB empty/misconfigured.'}")
    #     except NotionAPIError as e:
    #         print(f"Notion API Error: {e}")
    #     except Exception as e:
    #         print(f"An unexpected error occurred: {e}")

    #     print(f"\n--- Testing create_tender_page in {TEST_DB_ID} ---")
    #     test_tender_id = f"TEST-UTIL-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    #     try:
    #         page = create_tender_page(
    #             database_id=TEST_DB_ID,
    #             tender_id=test_tender_id,
    #             title="Test Page via Script @ " + datetime.now().strftime('%H:%M'),
    #             source_url="http://example.com/source",
    #             ai_summary="This is an AI summary for the test page.",
    #             company_id="TESTCO",
    #             gdrive_link="http://example.com/gdrive",
    #             status="Em Análise"
    #         )
    #         print(f"Page created successfully! ID: {page.get('id')}, URL: {page.get('url')}")
    #     except NotionAPIError as e:
    #         print(f"Notion API Error: {e}")
    #     except Exception as e:
    #         print(f"An unexpected error occurred: {e}")
    # else:
    #     print("\nSkipping Notion API tests: TEST_DB_ID not set in script.")
    
    print("\nNotion utils script finished.")

