import argparse
import importlib
import json
import os
import sys
import logging

# Assuming handlers/__init__.py defines COMPANY_HANDLER_MAP
# Need to adjust sys.path if running this script directly for testing, 
# but when run as 'python -m handlers', the project root should be in sys.path.
try:
    from . import COMPANY_HANDLER_MAP
except ImportError:
    # This might happen if run directly and 'handlers' is not in a recognized path.
    # For 'python -m handlers', this import should work.
    logging.error("Could not import COMPANY_HANDLER_MAP from .__init__. Ensure handlers is a package and __init__.py is correct.")
    # Fallback for direct execution or if Python path issues persist in some environments
    if 'COMPANY_HANDLER_MAP' not in globals():
        try:
            # Attempt to load from parent if EditalGCP is the project root
            from handlers import COMPANY_HANDLER_MAP
        except ImportError:
            logging.critical("Failed to import COMPANY_HANDLER_MAP. Cannot proceed.")
            sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Main handler dispatcher for tender processing.")
    parser.add_argument("--company-id", required=True, help="Identifier for the company/handler to run.")
    parser.add_argument("--output-dir", required=True, help="Directory to save downloaded files and output.")
    parser.add_argument("--notion-db-id", required=False, help="Notion Database ID (optional, for context to handler if needed).")
    parser.add_argument("--keywords", nargs='*', default=[], help="Optional keywords for the handler.")

    args = parser.parse_args()

    logging.info(f"Dispatcher invoked for company: {args.company_id} with output to {args.output_dir}")

    if args.company_id not in COMPANY_HANDLER_MAP:
        logging.error(f"No handler configured for company_id: {args.company_id}")
        print(json.dumps({"success": False, "error_message": f"No handler for {args.company_id}"}), file=sys.stdout)
        sys.exit(1)

    handler_module_name = COMPANY_HANDLER_MAP[args.company_id]

    try:
        logging.info(f"Attempting to import handler module: {handler_module_name}")
        handler_module = importlib.import_module(handler_module_name)
        logging.info(f"Successfully imported {handler_module_name}")
    except ImportError as e:
        logging.error(f"Failed to import handler module {handler_module_name}: {e}", exc_info=True)
        print(json.dumps({"success": False, "error_message": f"Could not import {handler_module_name}: {e}"}), file=sys.stdout)
        sys.exit(1)

    if not hasattr(handler_module, 'run_handler'):
        logging.error(f"Handler module {handler_module_name} does not have a 'run_handler' function.")
        print(json.dumps({"success": False, "error_message": f"'{handler_module_name}' is missing 'run_handler' function."}), file=sys.stdout)
        sys.exit(1)

    try:
        # The 'run_handler' function is expected to take output_dir, keywords, and potentially other args.
        # It should return a list of dictionaries, each representing a processed tender.
        logging.info(f"Executing run_handler for {args.company_id} with output_dir: {args.output_dir}, keywords: {args.keywords}")
        
        # We will need to inspect individual handlers to see what arguments they expect.
        # For now, let's assume a common interface.
        # Pass company_id and notion_db_id as well, as they might be useful for the handler context.
        processed_tenders = handler_module.run_handler(
            company_id=args.company_id,
            output_dir=args.output_dir, 
            keywords=args.keywords,
            notion_database_id=args.notion_db_id
        )
        
        # Ensure downloaded_zip_path is relative to output_dir if it's not already
        # and that all required fields are present.
        validated_tenders = []
        for tender in processed_tenders:
            if not all(k in tender for k in ['tender_id', 'title', 'downloaded_zip_path', 'source_url']):
                logging.warning(f"Skipping tender due to missing fields: {tender.get('tender_id', 'Unknown')}. Data: {tender}")
                continue
            
            # Make downloaded_zip_path relative if it's absolute within output_dir
            zip_path = tender['downloaded_zip_path']
            if os.path.isabs(zip_path) and zip_path.startswith(os.path.abspath(args.output_dir)):
                tender['downloaded_zip_path'] = os.path.relpath(zip_path, start=os.path.abspath(args.output_dir))
            elif os.path.isabs(zip_path):
                 logging.warning(f"Tender {tender['tender_id']} has absolute path '{zip_path}' not within output_dir '{args.output_dir}'. Keeping as is, but this might be an issue.")
            
            validated_tenders.append(tender)
            
        logging.info(f"Handler for {args.company_id} processed {len(validated_tenders)} tenders.")
        output = {
            "success": True, 
            "company_id": args.company_id,
            "new_tenders_processed": validated_tenders,
            "error_message": None
        }

    except Exception as e:
        logging.error(f"Error executing handler for {args.company_id}: {e}", exc_info=True)
        output = {
            "success": False, 
            "company_id": args.company_id, 
            "new_tenders_processed": [], 
            "error_message": str(e)
        }
        print(json.dumps(output), file=sys.stdout)
        sys.exit(1) # Indicate failure to the calling process

    print(json.dumps(output), file=sys.stdout)

if __name__ == "__main__":
    # This allows direct execution for testing individual handlers via this dispatcher.
    # Example: python -m handlers --company-id CAGECE --output-dir /tmp/cagece_output --keywords "aviso" "licitacao"
    main()
