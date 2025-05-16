import os
import subprocess
import json
import logging
from flask import Flask, request, jsonify

# Assuming utils are in the parent directory and PYTHONPATH is set appropriately or project is structured as a package
# If running Flask dev server from project root, imports should work.
from utils.common_utils import create_temp_run_dir, cleanup_temp_dir, CommonUtilsError
from utils.gdrive_utils import initialize_gdrive_service, upload_file_to_gdrive, create_shareable_link, GDriveError
from utils.notion_utils import create_tender_page, NotionAPIError # initialize_notion_client is called within these
from config.app_config import get_company_config

# Placeholder for analyze_zip, which will be in analyzers/__init__.py (workflow step 9)
# This allows the Flask app to be developed somewhat independently.
try:
    from analyzers import analyze_zip
except ImportError:
    def analyze_zip(zip_path: str, company_id: str | None = None) -> str:
        logging.warning(f"analyze_zip function not found or 'analyzers' module missing. Using placeholder for {zip_path}.")
        return f"Placeholder AI summary for {os.path.basename(zip_path)} (Company: {company_id}). Full analysis pending analyzer integration."

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables (ensure these are set in your environment)
FLASK_APP_SECURITY_TOKEN = os.getenv("FLASK_APP_SECURITY_TOKEN")
# NOTION_TOKEN and GDRIVE_SA_KEY_PATH are used by the utility modules

@app.route('/webhook/trigger-handler', methods=['POST'])
def trigger_handler_webhook():
    logger.info("Received request on /webhook/trigger-handler")

    # 1. Validate token
    auth_token = request.headers.get('X-Auth-Token')
    if not FLASK_APP_SECURITY_TOKEN:
        logger.error("CRITICAL: FLASK_APP_SECURITY_TOKEN is not set in the environment.")
        return jsonify({"success": False, "error": "Server configuration error: missing security token."}), 500
    
    if auth_token != FLASK_APP_SECURITY_TOKEN:
        logger.warning(f"Invalid or missing token. Received: {auth_token}")
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    # Extract parameters from request
    try:
        data = request.get_json()
        if data is None:
            logger.warning("Request payload is not JSON or is empty.")
            return jsonify({"success": False, "error": "Invalid request: payload must be JSON."}), 400
        
        company_id = data.get("company_id")
        if not company_id:
            logger.warning(f"Missing company_id in payload. Data: {data}")
            return jsonify({"success": False, "error": "Missing required parameter: company_id"}), 400

        company_cfg = get_company_config(company_id)
        if not company_cfg:
            logger.warning(f"No configuration found for company_id: {company_id}")
            return jsonify({"success": False, "error": f"Configuration not found for company_id: {company_id}"}), 400

        # Prioritize request payload, fallback to company config, then to defaults
        notion_database_id = data.get("notion_database_id") or company_cfg.get("notion_database_id")
        keywords = data.get("keywords") or company_cfg.get("handler_keywords", [])
        target_gdrive_folder_id = company_cfg.get("gdrive_folder_id") # This comes from config primarily

        if not notion_database_id:
            logger.warning(f"Missing notion_database_id for company {company_id} in payload and config.")
            return jsonify({"success": False, "error": f"Missing notion_database_id for company {company_id}"}), 400
        
        if not target_gdrive_folder_id:
            logger.warning(f"Missing gdrive_folder_id in configuration for company {company_id}.")
            # Decide if this is fatal or just a warning for upload skip
            # For now, this will cause uploads to be skipped later, which is handled.

        logger.info(f"Processing request for Company ID: {company_id}, Notion DB ID: {notion_database_id}, GDrive Folder ID: {target_gdrive_folder_id}")

    except Exception as e:
        logger.error(f"Error parsing request JSON: {e}", exc_info=True)
        return jsonify({"success": False, "error": f"Invalid JSON payload: {e}"}), 400

    temp_dir = None
    gdrive_service = None
    results = {"success": True, "processed_tenders": [], "errors": []}

    try:
        # 2. Create temp dir
        logger.info("Creating temporary run directory...")
        temp_dir = create_temp_run_dir(prefix=f"{company_id}_")
        logger.info(f"Temporary directory created: {temp_dir}")

        # Initialize Google Drive Service (do this once)
        try:
            gdrive_service = initialize_gdrive_service()
            logger.info("Google Drive service initialized.")
        except (ValueError, GDriveError) as e:
            logger.error(f"Failed to initialize Google Drive service: {e}", exc_info=True)
            results["errors"].append(f"GDrive Service Init Error: {e}")
            # Depending on requirements, you might choose to abort or continue without GDrive
            # For now, we'll let it proceed and fail on upload if gdrive_service is None

        # 3. Subprocess.run(['python','-m','handlers', ...])
        handler_command = [
            'python',
            '-m', 'handlers', # Assumes handlers/__main__.py exists and is executable
            '--company-id', company_id,
            '--notion-db-id', notion_database_id,
            '--output-dir', temp_dir
        ]
        if keywords and isinstance(keywords, list):
            handler_command.extend(['--keywords'] + keywords)
        
        logger.info(f"Running handler subprocess: {' '.join(handler_command)}")
        process = subprocess.run(handler_command, capture_output=True, text=True, check=False)

        if process.returncode != 0:
            error_msg = f"Handler subprocess for {company_id} failed. Return code: {process.returncode}. Stderr: {process.stderr.strip()}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            # Even if handler fails, we might have partial results or want to report failure clearly.
            # The workflow implies parsing JSON even on failure, so let's try.

        logger.info(f"Handler subprocess stdout for {company_id}: {process.stdout.strip()}")
        logger.info(f"Handler subprocess stderr for {company_id}: {process.stderr.strip()}")

        # 4. Parse JSON from handler output
        try:
            handler_output = json.loads(process.stdout)
            logger.info(f"Parsed handler output for {company_id}: {handler_output}")
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON output from handler for {company_id}: {e}. Raw output: {process.stdout.strip()}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            # If JSON parsing fails, we cannot proceed with tender processing
            raise CommonUtilsError(error_msg) # Propagate to main try-except for cleanup

        if not handler_output.get("success", False) and not handler_output.get("new_tenders_processed"):
            msg = f"Handler for {company_id} reported failure or no new tenders processed. Message: {handler_output.get('error_message', 'N/A')}"
            logger.warning(msg)
            if handler_output.get('error_message'):
                 results["errors"].append(handler_output.get('error_message'))
            # No new tenders to process, but not necessarily a fatal error for the webhook itself

        # 5. For each tender invoke analyze_zip()
        # 6. Upload ZIP to Drive (gdrive_utils) get link
        # 7. Build Notion properties and create_tender_page()
        processed_tenders_info = []
        for tender in handler_output.get("new_tenders_processed", []):
            tender_id = tender.get("tender_id")
            tender_title = tender.get("title")
            downloaded_zip_path = tender.get("downloaded_zip_path") # Should be absolute path or relative to temp_dir
            source_url = tender.get("source_url")

            if not all([tender_id, tender_title, downloaded_zip_path, source_url]):
                logger.warning(f"Skipping tender due to missing data: {tender}")
                results["errors"].append(f"Incomplete tender data from handler: {tender_id or 'Unknown ID'}")
                continue
            
            # Ensure downloaded_zip_path is absolute
            if not os.path.isabs(downloaded_zip_path) and temp_dir:
                current_zip_path = os.path.join(temp_dir, downloaded_zip_path)
            else:
                current_zip_path = downloaded_zip_path

            if not os.path.exists(current_zip_path):
                logger.error(f"ZIP file not found for tender {tender_id} at path: {current_zip_path}")
                results["errors"].append(f"ZIP file missing for tender {tender_id}: {current_zip_path}")
                continue
            
            logger.info(f"Processing tender: {tender_id} - {tender_title}")
            tender_data_for_notion = {
                "tender_id": tender_id,
                "title": tender_title,
                "source_url": source_url,
                "company_id": company_id,
                "status": "Em Análise" # Initial status after processing
            }

            # Invoke analyze_zip()
            try:
                logger.info(f"Analyzing ZIP for tender {tender_id}: {current_zip_path}")
                # Pass company_id to analyze_zip if it can use it for context
                ai_summary = analyze_zip(current_zip_path, company_id=company_id)
                tender_data_for_notion["ai_summary"] = ai_summary
                logger.info(f"AI Summary for {tender_id}: {ai_summary[:100]}...") # Log snippet
            except Exception as e:
                logger.error(f"Error analyzing ZIP for tender {tender_id}: {e}", exc_info=True)
                results["errors"].append(f"Analysis Error ({tender_id}): {e}")
                tender_data_for_notion["ai_summary"] = f"Error during AI analysis: {e}"
            
            # Upload ZIP to Drive and get link
            gdrive_file_id = None
            if gdrive_service:
                try:
                    logger.info(f"Uploading ZIP to Google Drive for tender {tender_id}...")
                    # TODO: Need gdrive_folder_id, likely from config (Step 12)
                    # For now, let's assume it's passed in request or hardcoded (bad)
                    # We'll need to fetch this from app_config.py based on company_id (step 12)
                    # target_gdrive_folder_id is now fetched from company_cfg earlier
                    if not target_gdrive_folder_id:
                         logger.warning(f"No gdrive_folder_id configured for company {company_id}, cannot upload {tender_id}")
                         results["errors"].append(f"Missing GDrive folder ID in config for {company_id}, upload skipped for {tender_id}.")
                    else:
                        gdrive_file_name = f"{company_id}_{tender_id.replace('/', '-')}.zip"
                        gdrive_file_id = upload_file_to_gdrive(gdrive_service, current_zip_path, target_gdrive_folder_id, file_name=gdrive_file_name)
                        if gdrive_file_id:
                            logger.info(f"ZIP uploaded to GDrive for tender {tender_id}. File ID: {gdrive_file_id}")
                            shareable_link = create_shareable_link(gdrive_service, gdrive_file_id)
                            if shareable_link:
                                tender_data_for_notion["gdrive_link"] = shareable_link
                                logger.info(f"GDrive shareable link for {tender_id}: {shareable_link}")
                            else:
                                results["errors"].append(f"Failed to create GDrive shareable link for {tender_id} (File ID: {gdrive_file_id})")
                        else:
                            results["errors"].append(f"Failed to upload ZIP to GDrive for tender {tender_id}")
                except (GDriveError, FileNotFoundError) as e:
                    logger.error(f"Error with GDrive for tender {tender_id}: {e}", exc_info=True)
                    results["errors"].append(f"GDrive Error ({tender_id}): {e}")
            else:
                 logger.warning("GDrive service not available, skipping upload.")
                 results["errors"].append(f"GDrive service not available, upload skipped for {tender_id}.")

            # Build Notion properties and create_tender_page()
            try:
                logger.info(f"Creating Notion page for tender {tender_id} in DB {notion_database_id}...")
                # Additional properties can be passed if needed
                notion_page = create_tender_page(
                    database_id=notion_database_id,
                    tender_id=tender_data_for_notion["tender_id"],
                    title=tender_data_for_notion["title"],
                    source_url=tender_data_for_notion["source_url"],
                    ai_summary=tender_data_for_notion.get("ai_summary"),
                    status=tender_data_for_notion["status"],
                    company_id=tender_data_for_notion["company_id"],
                    gdrive_link=tender_data_for_notion.get("gdrive_link")
                )
                logger.info(f"Notion page created for {tender_id}. Page ID: {notion_page.get('id')}")
                processed_tenders_info.append({
                    "tender_id": tender_id,
                    "notion_page_id": notion_page.get('id'),
                    "notion_page_url": notion_page.get('url'),
                    "gdrive_file_id": gdrive_file_id
                })
            except NotionAPIError as e:
                logger.error(f"Error creating Notion page for tender {tender_id}: {e}", exc_info=True)
                results["errors"].append(f"Notion Error ({tender_id}): {e}")
        
        results["processed_tenders"] = processed_tenders_info
        if not results["errors"]:
            results["message"] = f"Successfully processed request for company {company_id}. {len(processed_tenders_info)} new tenders handled."
        else:
            results["message"] = f"Request for company {company_id} processed with {len(results['errors'])} errors. {len(processed_tenders_info)} tenders handled."
            results["success"] = False # Mark overall success as False if any errors occurred during processing

        logger.info(f"Finished processing for {company_id}. Results: {results}")
        return jsonify(results), 200 if results["success"] else 500

    except CommonUtilsError as e:
        logger.critical(f"A critical CommonUtilsError occurred: {e}", exc_info=True)
        results["errors"].append(f"Critical processing error: {e}")
        results["success"] = False
        return jsonify(results), 500
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        results["errors"].append(f"Unexpected critical error: {e}")
        results["success"] = False
        return jsonify(results), 500
    finally:
        # 8. Cleanup temp dir
        if temp_dir:
            logger.info(f"Cleaning up temporary directory: {temp_dir}")
            try:
                cleanup_temp_dir(temp_dir)
            except CommonUtilsError as e:
                logger.error(f"Failed to cleanup temp_dir {temp_dir}: {e}", exc_info=True)
                # This error during cleanup shouldn't fail the main response if processing was otherwise ok
                if results.get("success", True): # if not already marked as failed
                    results["errors"].append(f"Cleanup Error: {e}") # Add to errors but don't change status

if __name__ == '__main__':
    # For local development: flask run --host=0.0.0.0 --port=8080
    # Ensure FLASK_APP=app/main.py is set in your environment, along with other required env vars.
    logger.info("Starting Flask application for local development.")
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)), debug=True)
