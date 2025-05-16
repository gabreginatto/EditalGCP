---
description: 
---

/title tender-tracking
/description End‑to‑end build plan: scaffold aux utils → refactor handlers → integrate analyzer → run E2E tests → containerize → deploy.

/steps
# ─────────────────────────────────────────────
# 0. Setup context & index
# ─────────────────────────────────────────────
1. /chat "Create utils/notion_utils.py with initialize_notion_client(), get_existing_tender_ids(), create_tender_page() per spec (token via env)"
2. /chat "Create utils/gdrive_utils.py with initialize_gdrive_service(), upload_file_to_gdrive(), create_shareable_link() (service‑account JSON via env)"
3. /chat "Create utils/common_utils.py with create_temp_run_dir() and cleanup_temp_dir()"
4. /chat "Write unit tests under tests/unit/ for the three utils using pytest + unittest.mock (target ≥85 % coverage)"

# ─────────────────────────────────────────────
# 1. Standardize handler interface
# ─────────────────────────────────────────────
5. /chat "Add handlers/__main__.py: argparse wrapper (--company-id …) that imports correct dynamic_handler_* module and returns JSON to stdout"
6. /chat "Refactor every script in handlers/*.py so each exposes a run(notion_database_id, output_dir, keywords) function that returns JSON schema: {success:bool, company_id:str, new_tenders_processed:[{tender_id:str,title:str,downloaded_zip_path:str,source_url:str}], error_message:str?}"
7. /chat "Inside each handler, call utils/notion_utils.get_existing_tender_ids() to skip duplicates and save ZIPs into output_dir"
8. /chat "Commit all modified handlers and run pytest tests/unit/test_handler_wrapper.py to ensure CLI invocation works"

# ─────────────────────────────────────────────
# 2. Integrate edital_analyzer
# ─────────────────────────────────────────────
9. /chat "Create analyzers/__init__.py exposing analyze_zip(path) that imports edital_analyzer.EditalAnalyzer and returns summary text"
10. /chat "Write tests/unit/test_analyzer.py with a sample ZIP fixture (empty ok) calling analyze_zip()"

# ─────────────────────────────────────────────
# 3. Orchestration (Flask)
# ─────────────────────────────────────────────
11. /chat "Implement app/main.py Flask route /webhook/trigger-handler:
   • validate token
   • create temp dir (common_utils)
   • subprocess.run(['python','-m','handlers', …])
   • parse JSON
   • for each tender invoke analyze_zip()
   • upload ZIP to Drive (gdrive_utils) get link
   • build Notion properties and create_tender_page()
   • cleanup temp dir"
12. /chat "Add config/app_config.py with COMPANY_CONFIG mapping company_id → target_url, gdrive_folder_id"
13. /chat "Unit‑test main.py with Flask test client + monkeypatch subprocess + mocks"

# ─────────────────────────────────────────────
# 4. Local end‑to‑end test
# ─────────────────────────────────────────────
14. /terminal "pytest -q tests/unit"

# ─────────────────────────────────────────────
# 5. Containerize only after code passes
# ─────────────────────────────────────────────
15. /terminal "docker build -t tender-tracker:local ."
16. /terminal "docker run --rm -p 8080:8080 -e NOTION_TOKEN=stub -e FLASK_APP_SECURITY_TOKEN=stub -e GDRIVE_SA_KEY=stub tender-tracker:local &"  # background
17. /chat "POST http://localhost:8080/webhook/trigger-handler with sample payload; assert 200"
18. /terminal "docker stop $(docker ps -q --filter ancestor=tender-tracker:local)"

# ─────────────────────────────────────────────
# 6. Deploy
# ─────────────────────────────────────────────
19. /terminal "gcloud builds submit --tag gcr.io/$PROJECT/tender-tracker"
20. /terminal "gcloud run deploy tender-tracker --image gcr.io/$PROJECT/tender-tracker --set-secrets=NOTION_TOKEN=projects/$PROJECT/secrets/NOTION_TOKEN:latest,FLASK_APP_SECURITY_TOKEN=projects/$PROJECT/secrets/FLASK_APP_SECURITY_TOKEN:latest,GDRIVE_SA_KEY=projects/$PROJECT/secrets/GDRIVE_SA_KEY:latest,GEMINI_API_KEY=projects/$PROJECT/secrets/GEMINI_API_KEY:latest"
21. /terminal "gcloud functions deploy triggerTender --runtime=python312 --source=gcf/ --entry-point=entry --set-secrets=FLASK_APP_SECURITY_TOKEN=projects/$PROJECT/secrets/FLASK_APP_SECURITY_TOKEN:latest --allow-unauthenticated"

# ─────────────────────────────────────────────
# 7. Production smoke‑test
# ─────────────────────────────────────────────
22. /chat "POST {{GCF_URL}}?company_id=SANEAGO&notion_database_id={{STAGING_DB}}"
23. /chat "Assert: Notion page appears with AI summary & Drive link within 90 s"