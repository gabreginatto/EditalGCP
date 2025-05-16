# Tender-Tracking End-to-End

Build, deploy, smoke-test the whole pipeline.

## Steps

1. Build the Docker image locally:
   ```bash
   docker build -t tender-tracker:local .
   ```

2. Run unit tests:
   ```bash
   pytest -q tests/unit
   ```

3. Build and push the container image to Google Container Registry:
   ```bash
   gcloud builds submit --tag gcr.io/$PROJECT/tender-tracker
   ```

4. Deploy to Cloud Run:
   ```bash
   gcloud run deploy tender-tracker --image gcr.io/$PROJECT/tender-tracker \
   --set-secrets=NOTION_TOKEN=projects/$PROJECT/secrets/NOTION_TOKEN:latest,\
FLASK_APP_SECURITY_TOKEN=projects/$PROJECT/secrets/FLASK_APP_SECURITY_TOKEN:latest,\
GDRIVE_SA_KEY=projects/$PROJECT/secrets/GDRIVE_SA_KEY:latest,\
GEMINI_API_KEY=projects/$PROJECT/secrets/GEMINI_API_KEY:latest
   ```

5. Deploy the Cloud Function:
   ```bash
   gcloud functions deploy triggerTender --runtime=python312 \
   --source=gcf/ --entry-point=entry \
   --set-secrets=FLASK_APP_SECURITY_TOKEN=projects/$PROJECT/secrets/FLASK_APP_SECURITY_TOKEN:latest \
   --allow-unauthenticated
   ```

6. Trigger the tender tracking for SANEAGO:
   ```
   POST {{GCF_URL}}?company_id=SANEAGO&notion_database_id={{STAGING_DB}}
   ```

7. Verify the deployment:
   ```
   Assert: New Notion page with Drive link exists within 90 s.
   ```
