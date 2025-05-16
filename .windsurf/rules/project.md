<!-- Always On -->
# Tender-Tracking Build Rules

- **Environment contract**  
  - NOTION_TOKEN, FLASK_APP_SECURITY_TOKEN, GDRIVE_SA_KEY, GEMINI_API_KEY → Secret Manager.  
  - PYTHONUNBUFFERED=1 at container runtime.

- **Handler CLI schema**  
  - Adapter wrapper must call handlers with:  
    `--url <target_url> --output-dir <temp_dir> --timeout 1800`  
  - Wrapper itself accepts:  
    `--company-id` `--notion-database-id` `--output-dir` `--keywords*`

- **Definition of Done**  
  1. Notion trigger for SANEAGO creates a new page in <90 s.  
  2. Page has Tender ID, Title, AI Summary, Drive link, Source URL.  
  3. No leftover files in `/tmp` on Cloud Run.

- **Coding conventions**  
  - Use `python-json-logger` for structured logs.  
  - ≥85 % test coverage on utils & orchestration.
