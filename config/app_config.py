# Application Configuration

# COMPANY_CONFIG maps a company_id to its specific settings.
# Each company's settings should be a dictionary including:
#   - 'target_url': The base URL or specific endpoint the handler should target for this company.
#                   (This might not be directly used by app/main.py but by the handler invoked via subprocess)
#   - 'gdrive_folder_id': The Google Drive Folder ID where documents for this company should be stored.
#   - 'notion_database_id': The Notion Database ID for this company's tenders (alternative to passing in request).
#                           It's often better to have this configurable per company.
#   - 'handler_keywords': Default keywords to pass to the handler for this company if not provided in the request.
#   - Any other company-specific parameters.

COMPANY_CONFIG = {
    "EXAMPLE_COMPANY_1": {
        "target_url": "https://www.example-company1.com/tenders",
        "gdrive_folder_id": "your_gdrive_folder_id_for_example_company_1",
        "notion_database_id": "your_notion_db_id_for_example_company_1", 
        "handler_keywords": ["public bid", "licitação"],
        "contact_email": "procurement@example-company1.com"
    },
    "EXAMPLE_COMPANY_2": {
        "target_url": "https://bids.example-company2.org/current",
        "gdrive_folder_id": "your_gdrive_folder_id_for_example_company_2",
        "notion_database_id": "your_notion_db_id_for_example_company_2",
        "handler_keywords": ["RFP", "RFQ"],
        "requires_special_auth": False 
    },
    # Add more company configurations here as needed
    # "SANEAGO": {
    #     "target_url": "https://www.saneago.com.br/licitacoes/", 
    #     "gdrive_folder_id": "your_saneago_gdrive_folder_id",
    #     "notion_database_id": "your_saneago_notion_db_id",
    #     "handler_keywords": ["edital", "contratação", "disputa"],
    # },
}

def get_company_config(company_id: str) -> dict | None:
    """Retrieves the configuration for a given company_id."""
    return COMPANY_CONFIG.get(company_id)

if __name__ == '__main__':
    print("--- Testing app_config.py ---")
    test_ids = ["EXAMPLE_COMPANY_1", "NON_EXISTENT_COMPANY", "SANEAGO"]
    for cid in test_ids:
        config = get_company_config(cid)
        if config:
            print(f"Config for {cid}:")
            for key, value in config.items():
                print(f"  {key}: {value}")
        else:
            print(f"No config found for {cid}.")
    print("--- Finished app_config.py tests ---")
