import pytest
import json
import os
from unittest import mock

# Set environment variables for testing BEFORE importing the app
# Patch os.getenv used by the app module for FLASK_APP_SECURITY_TOKEN
TEST_SECURITY_TOKEN = "test-super-secret-token"

@mock.patch.dict(os.environ, {
    "FLASK_APP_SECURITY_TOKEN": TEST_SECURITY_TOKEN,
    "NOTION_TOKEN": "fake_notion_token", # For notion_utils via app
    "GDRIVE_SA_KEY_PATH": "fake_gdrive_key_path.json" # For gdrive_utils via app
})
def app_client():
    """Fixture to create and configure a Flask test client."""
    # Import app here after environment is patched
    from app.main import app as flask_app 
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as client:
        yield client

# Mock external dependencies used by app.main
@pytest.fixture
def mock_common_utils():
    with mock.patch('app.main.create_temp_run_dir') as mock_create,
         mock.patch('app.main.cleanup_temp_dir') as mock_cleanup:
        mock_create.return_value = "/tmp/fake_temp_dir_123"
        yield mock_create, mock_cleanup

@pytest.fixture
def mock_gdrive_utils():
    with mock.patch('app.main.initialize_gdrive_service') as mock_init,
         mock.patch('app.main.upload_file_to_gdrive') as mock_upload,
         mock.patch('app.main.create_shareable_link') as mock_link:
        mock_init.return_value = mock.Mock() # Mocked service object
        mock_upload.return_value = "fake_gdrive_file_id_123"
        mock_link.return_value = "http://fake.gdrive.link/file123"
        yield mock_init, mock_upload, mock_link

@pytest.fixture
def mock_notion_utils():
    with mock.patch('app.main.create_tender_page') as mock_create:
        mock_create.return_value = {"id": "fake_notion_page_id_123", "url": "http://fake.notion.page/page123"}
        yield mock_create

@pytest.fixture
def mock_analyze_zip():
    # If analyze_zip is in app.main's namespace (due to try/except import)
    with mock.patch('app.main.analyze_zip') as mock_analyze:
        mock_analyze.return_value = "Mocked AI Summary."
        yield mock_analyze

@pytest.fixture
def mock_subprocess_run():
    with mock.patch('app.main.subprocess.run') as mock_run:
        # Default successful run
        mock_run.return_value = mock.Mock(
            returncode=0, 
            stdout=json.dumps({
                "success": True, 
                "company_id": "TEST_COMPANY", 
                "new_tenders_processed": [
                    {
                        "tender_id": "TENDER_001", 
                        "title": "Test Tender 1", 
                        "downloaded_zip_path": "tender1.zip", 
                        "source_url": "http://example.com/tender1"
                    }
                ]
            }), 
            stderr=''
        )
        yield mock_run

@pytest.fixture
def mock_config():
    with mock.patch('app.main.get_company_config') as mock_get_cfg:
        mock_get_cfg.return_value = {
            "target_url": "http://fake-target.com",
            "gdrive_folder_id": "fake_gdrive_folder_for_company",
            "notion_database_id": "fake_notion_db_for_company",
            "handler_keywords": ["test", "keyword"]
        }
        yield mock_get_cfg

# Test cases
def test_trigger_handler_unauthorized_no_token(app_client):
    response = app_client.post('/webhook/trigger-handler', json={})
    assert response.status_code == 401
    assert b"Unauthorized" in response.data

def test_trigger_handler_unauthorized_wrong_token(app_client):
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': 'wrong-token'},
                               json={})
    assert response.status_code == 401
    assert b"Unauthorized" in response.data

def test_trigger_handler_missing_company_id(app_client):
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json={'notion_database_id': 'some_db'})
    assert response.status_code == 400
    assert b"Missing required parameter: company_id" in response.data

def test_trigger_handler_company_config_not_found(app_client, mock_config):
    mock_config.return_value = None # Simulate config not found
    payload = {"company_id": "UNKNOWN_COMPANY", "notion_database_id": "some_db"}
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    assert response.status_code == 400
    assert b"Configuration not found for company_id: UNKNOWN_COMPANY" in response.data

def test_trigger_handler_missing_notion_db_id(app_client, mock_config):
    # Ensure mock_config returns a config *without* notion_database_id
    mock_config.return_value = {
        "target_url": "http://fake-target.com",
        "gdrive_folder_id": "fake_gdrive_folder_for_company",
        # "notion_database_id": "fake_notion_db_for_company", <--- Missing
        "handler_keywords": ["test", "keyword"]
    }
    payload = {"company_id": "TEST_COMPANY"} # No notion_database_id in payload either
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    assert response.status_code == 400
    assert b"Missing notion_database_id for company TEST_COMPANY" in response.data

@mock.patch('os.path.exists', return_value=True) # Assume zip file exists
def test_trigger_handler_success_flow(
    app_client, mock_common_utils, mock_gdrive_utils, 
    mock_notion_utils, mock_analyze_zip, mock_subprocess_run, mock_config, _mock_os_path_exists
):
    mock_create_temp, mock_cleanup_temp = mock_common_utils
    _mock_gdrive_init, mock_gdrive_upload, mock_gdrive_link = mock_gdrive_utils
    mock_notion_create_page = mock_notion_utils

    payload = {
        "company_id": "TEST_COMPANY",
        "notion_database_id": "req_notion_db", # Can be from request
        "keywords": ["req_keyword"]
    }
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    
    assert response.status_code == 200
    json_response = response.get_json()
    assert json_response["success"] is True
    assert len(json_response["processed_tenders"]) == 1
    assert json_response["processed_tenders"][0]["tender_id"] == "TENDER_001"
    assert json_response["processed_tenders"][0]["notion_page_id"] == "fake_notion_page_id_123"
    assert json_response["processed_tenders"][0]["gdrive_file_id"] == "fake_gdrive_file_id_123"
    assert not json_response["errors"]

    mock_create_temp.assert_called_once_with(prefix="TEST_COMPANY_")
    mock_subprocess_run.assert_called_once()
    # Check if subprocess was called with correct output_dir and keywords from request
    args, _kwargs = mock_subprocess_run.call_args
    assert '--output-dir' in args[0]
    assert args[0][args[0].index('--output-dir') + 1] == "/tmp/fake_temp_dir_123"
    assert '--keywords' in args[0]
    assert args[0][args[0].index('--keywords') + 1] == "req_keyword"
    assert '--notion-db-id' in args[0]
    assert args[0][args[0].index('--notion-db-id') + 1] == "req_notion_db"

    mock_analyze_zip.assert_called_once_with(os.path.join("/tmp/fake_temp_dir_123", "tender1.zip"), company_id="TEST_COMPANY")
    mock_gdrive_upload.assert_called_once_with(mock.ANY, os.path.join("/tmp/fake_temp_dir_123", "tender1.zip"), "fake_gdrive_folder_for_company", file_name="TEST_COMPANY_TENDER_001.zip")
    mock_gdrive_link.assert_called_once_with(mock.ANY, "fake_gdrive_file_id_123")
    mock_notion_create_page.assert_called_once_with(
        database_id="req_notion_db",
        tender_id="TENDER_001",
        title="Test Tender 1",
        source_url="http://example.com/tender1",
        ai_summary="Mocked AI Summary.",
        status="Em Análise",
        company_id="TEST_COMPANY",
        gdrive_link="http://fake.gdrive.link/file123"
    )
    mock_cleanup_temp.assert_called_once_with("/tmp/fake_temp_dir_123")

def test_trigger_handler_subprocess_failure(app_client, mock_subprocess_run, mock_config):
    mock_subprocess_run.return_value = mock.Mock(
        returncode=1, 
        stdout=json.dumps({"success": False, "error_message": "Handler script crashed"}), 
        stderr='Traceback...'
    )
    payload = {"company_id": "FAIL_COMPANY", "notion_database_id": "db123"}
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    assert response.status_code == 500 # Overall success is False
    json_response = response.get_json()
    assert json_response["success"] is False
    assert "Handler subprocess for FAIL_COMPANY failed" in json_response["errors"][0]
    assert "Handler script crashed" in json_response["errors"][1] # Error from handler JSON

def test_trigger_handler_json_decode_error(app_client, mock_subprocess_run, mock_config):
    mock_subprocess_run.return_value = mock.Mock(returncode=0, stdout="not valid json", stderr='')
    payload = {"company_id": "BAD_JSON_COMPANY", "notion_database_id": "db123"}
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    assert response.status_code == 500
    json_response = response.get_json()
    assert json_response["success"] is False
    assert "Failed to parse JSON output from handler" in json_response["errors"][0]

@mock.patch('os.path.exists', return_value=False) # Simulate zip file NOT existing
def test_trigger_handler_zip_file_not_found(
    app_client, mock_common_utils, mock_subprocess_run, mock_config, _mock_os_path_exists
):
    payload = {"company_id": "NO_ZIP_COMPANY", "notion_database_id": "db123"}
    response = app_client.post('/webhook/trigger-handler', 
                               headers={'X-Auth-Token': TEST_SECURITY_TOKEN},
                               json=payload)
    assert response.status_code == 500 # Still results in an error for that tender
    json_response = response.get_json()
    assert json_response["success"] is False # Because an error occurred for a tender
    assert "ZIP file missing for tender TENDER_001" in json_response["errors"][0]

# Add more tests: e.g., GDrive init fails, GDrive upload fails, Notion create fails, analyze_zip throws exception
# Test for when keywords are from config vs request

