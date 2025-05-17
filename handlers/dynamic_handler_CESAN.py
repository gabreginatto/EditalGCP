#!/usr/bin/env python3
# handler_cesan.py - Refactored to use Playwright exclusively in Python

import os
import json
import time
import asyncio
import argparse
import zipfile
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, ElementHandle, Download, Locator, BrowserContext

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the current script directory
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent.parent

# Load environment variables from .env file
ENV_PATH = ROOT_DIR / '.env'
load_dotenv(dotenv_path=ENV_PATH)
logger.info(f"Loading environment from: {ENV_PATH}")

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='CESAN procurement downloader')
    parser.add_argument('--output-dir', type=str, default=os.getcwd(),
                        help='Directory to save downloads (default: current working directory)')
    return parser.parse_args()

def load_processed_list(file_path: str) -> List[str]:
    """Load the list of already processed items."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)  # Should be a list of strings
        else:
            return []
    except Exception as error:
        logger.error(f"Error loading processed list from {file_path}: {error}")
        return []  # Start fresh if error reading

def save_processed_list(file_path: str, items_list: List[str]):
    """Save the list of processed items."""
    try:
        dir_path = os.path.dirname(file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(items_list, f, indent=2, ensure_ascii=False)
    except Exception as error:
        logger.error(f"Error saving processed list to {file_path}: {error}")

def sanitize_processo(processo: str) -> str:
    """Safely format Processo Number for filenames."""
    return processo.replace(' ', '-').replace('/', '-').replace('\\', '-').replace('?', '-').replace('%', '-')\
        .replace('*', '-').replace(':', '-').replace('|', '-').replace('"', '-').replace('<', '-').replace('>', '-') \
        or 'UNKNOWN_PROCESSO'

async def main():
    try:
        # Parse command-line arguments
        args = parse_args()
        # Define fixed base directories relative to the script or workspace root if possible,
        # otherwise use absolute paths carefully.
        # Assuming ROOT_DIR is correctly defined as the project root.
        downloads_base_dir = ROOT_DIR / 'downloads' # /Users/gabrielreginatto/Desktop/Code/DownloadEditalAnalise/downloads
        
        # --- Configuration ---
        temp_download_dir = downloads_base_dir / 'temp_cesan' # Specific temp folder for this handler
        archive_dir = downloads_base_dir / 'archives'       # Final zip location
        processed_json_path = downloads_base_dir / 'processed_cesan.json' # Correct processed file location

        logger.info(f"[handler_cesan.py] Using processed file: {processed_json_path}")
        logger.info(f"[handler_cesan.py] Temporary download dir: {temp_download_dir}")
        logger.info(f"[handler_cesan.py] Final archive dir: {archive_dir}")

        keywords = ['tubo', 'polietileno', 'PEAD', 'polimero', 'PAM', 'hidrômetro', 'medidor']
        required_year = "2025" # Year to filter by
        # ---------------------

        processed_list = load_processed_list(str(processed_json_path))
        logger.info(f"Loaded {len(processed_list)} previously processed items.")
        
        # Ensure temporary download and archive directories exist
        if not temp_download_dir.exists():
            logger.info(f"Creating temporary download directory: {temp_download_dir}")
            temp_download_dir.mkdir(parents=True, exist_ok=True)
        if not archive_dir.exists():
            logger.info(f"Creating archive directory: {archive_dir}")
            archive_dir.mkdir(parents=True, exist_ok=True)

        async with async_playwright() as playwright:
            browser = None
            
            try:
                logger.info("Initializing browser...")
                browser = await playwright.chromium.launch(
                    headless=False,
                    args=['--start-maximized']
                )
                context = await browser.new_context(
                    viewport=None,  # Full viewport
                    accept_downloads=True  # Allow file downloads
                )
                page = await context.new_page()
                logger.info("Browser initialized.")

                # 1. Navigation
                search_url = "https://compras.cesan.com.br/consultarLicitacao.php"
                logger.info(f"Step 1: Navigating to {search_url}...")
                await page.goto(search_url, wait_until='networkidle')
                await page.wait_for_timeout(2000) # Allow dynamic content
                logger.info("Step 1 Complete: Page loaded.")

                # 3. Wait for initial table to potentially load (if dynamic)
                logger.info("Step 3: Waiting for potential dynamic table load...")
                # Adjust selector if needed, ensure table is targetted
                results_table_body_selector = 'fieldset.formulario div.content3 table.rTableLicitacao tbody'
                try:
                    await page.locator(results_table_body_selector).first.wait_for(state='attached', timeout=15000)
                    await page.wait_for_load_state('networkidle', timeout=15000) # Extra wait
                    await page.wait_for_timeout(3000) # Buffer
                except Exception as wait_error:
                    logger.warning(f"Warning during initial wait for table: {wait_error}")
                logger.info("Step 3 Complete: Initial wait finished.")

                # 4. Collect Results Programmatically directly from the table
                logger.info("Step 4: Collecting and filtering results from the table...")
                licitacoes_to_process = []
                results_row_selector = f"{results_table_body_selector} tr"

                try:
                    table_body_count = await page.locator(results_table_body_selector).count()
                    if table_body_count > 0:
                        result_rows = await page.locator(results_row_selector).element_handles()
                        logger.info(f"   Found {len(result_rows)} total rows in the table.")

                        for row_handle in result_rows:
                            try:
                                # Get all text content from the row
                                row_text_content = (await row_handle.text_content() or '').lower() # Lowercase for case-insensitive keyword check
                                # *** CORRECTED: Selector targets the strong tag inside the specific label ***
                                date_element = await row_handle.query_selector('label.custom-label strong')
                                date_text = (await date_element.text_content() or '').strip() if date_element else ""

                                # Check for keywords AND year
                                keyword_found = any(keyword.lower() in row_text_content for keyword in keywords)
                                year_found = required_year in date_text

                                if keyword_found and year_found:
                                    # Determine which keywords matched
                                    matching_keywords = [kw for kw in keywords if kw.lower() in row_text_content]
                                    logger.info(f"    MATCH FOUND: Row contains keyword(s) {matching_keywords} and year {required_year}. Date: '{date_text}'")
                                    link_element = await row_handle.query_selector('a[href*="viewLicitacao.php?idLicitacao="]')
                                    if link_element:
                                        relative_url = await link_element.get_attribute('href')
                                        strong_element = await link_element.query_selector('strong') # Get Processo from link text
                                        
                                        if strong_element:
                                            processo_text = (await strong_element.text_content() or '').strip()
                                            logger.debug(f"    Extracted processo_text from strong tag: '{processo_text}'") # Log the text
                                        else:
                                            processo_text = 'UNKNOWN_PROCESSO_IN_LINK' # Fallback
                                            
                                        # Match the process code pattern (keep this)
                                        import re
                                        # Updated Regex: More flexible for multiple words and hyphens
                                        logger.debug(f"    Attempting regex match on: '{processo_text}'") # Log before regex
                                        match = re.search(r'([A-Z\sÇÃÔ]+?\s*-\s*[A-Z]+\s+\d+\/\d{4})', processo_text)

                                        # Determine final processo_code based on match result
                                        if match:
                                            processo_code = match.group(0).replace(" - ", " ") # Cleaned matched code
                                        elif processo_text != 'UNKNOWN_PROCESSO_IN_LINK':
                                            processo_code = processo_text # Use original text if no match but text exists
                                            logger.warning(f"    Regex did not match '{processo_text}'. Using full text as processo_code.")
                                        else:
                                            processo_code = 'UNKNOWN_PROCESSO_IN_LINK' # Keep the marker if it started as unknown

                                        logger.debug(f"    Final processo_code after regex/fallback: '{processo_code}'") # Log after regex

                                        # Check if we have a valid URL AND a valid extracted/fallback code
                                        if relative_url and processo_code != 'UNKNOWN_PROCESSO_IN_LINK':
                                            # Create absolute URL
                                            base_url = '/'.join(search_url.split('/')[:3])
                                            absolute_url = f"{base_url}/{relative_url}"
                                            licitacoes_to_process.append({
                                                'processo': processo_code,
                                                'url': absolute_url
                                            })
                                            logger.info(f"       Collected: Processo '{processo_code}', URL: {absolute_url}")
                                        else:
                                            # Log detailed failure reason
                                            failure_reason = []
                                            if not relative_url: failure_reason.append("relative_url is missing")
                                            if processo_code == 'UNKNOWN_PROCESSO_IN_LINK': failure_reason.append("processo_code is unknown/missing")
                                            logger.warning(f"       Could not collect row. Reason(s): {', '.join(failure_reason)}.")
                                            logger.warning(f"         Details: relative_url='{relative_url}', processo_text='{processo_text}', final_processo_code='{processo_code}'")
                                    else:
                                        logger.warning("       Matched row, but could not find the details link element (<a>).")
                                # else:
                                    # Optional: Log rows that didn't match
                                    # if not keyword_found: logger.debug(f"    Skipping row: Keyword not found.")
                                    # if not year_found: logger.debug(f"    Skipping row: Year '{required_year}' not found in date '{date_text}'.")

                            except Exception as extract_error:
                                logger.warning(f"     Error processing a row: {extract_error}")
                    else:
                        logger.info("   Results table body not found or empty. No licitações to process.")

                except Exception as programmatic_error:
                    logger.error(f"   Error during result collection: {programmatic_error}")

                logger.info(f"Step 4 Complete: Collected {len(licitacoes_to_process)} licitações matching criteria.")

                # 5. Process Each Collected Licitacao (This part remains largely the same)
                logger.info(f"Step 5: Processing {len(licitacoes_to_process)} collected licitações...")
                newly_processed_count = 0
                for i, licitacao in enumerate(licitacoes_to_process):
                    processo = licitacao['processo']
                    url = licitacao['url']
                    safe_processo_number = sanitize_processo(processo)
                    licitacao_success = True

                    logger.info(f"\n  Processing Licitacao {i + 1}/{len(licitacoes_to_process)}: Processo {processo} at {url}")

                    if processo in processed_list:
                        logger.info(f"    Skipping already processed Processo: {processo}")
                        continue

                    downloaded_file_paths = []
                    try:
                        logger.info("    Navigating to licitação details page...")
                        await page.goto(url, wait_until='networkidle')
                        await page.wait_for_timeout(3000) # Allow page to settle

                        # Download logic (find links ending in .pdf, .zip, etc.)
                        logger.info("    Locating 'Lista de Documentos' table and searching for download links...")
                        # Example selectors (adjust as needed based on actual page structure)
                        link_elements = [] # Initialize
                        try:
                            # 1. Find the header element containing the specific text
                            header_element = page.locator("strong:has-text('Lista de Documentos')").first
                            await header_element.wait_for(state='visible', timeout=10000)

                            # 2. Find the parent table of that header
                            # Using XPath to find the ancestor table
                            documents_table = header_element.locator("xpath=ancestor::table[1]")
                            await documents_table.wait_for(state='attached', timeout=5000)

                            # 3. Find links with specific extensions *within* that table
                            download_links_selector = 'a[href$=".pdf"], a[href$=".zip"], a[href$=".rar"], a[href$=".doc"], a[href$=".docx"], a[href$=".xls"], a[href$=".xlsx"]'
                            link_elements = await documents_table.locator(download_links_selector).element_handles()
                            logger.info(f"    Found table and located {len(link_elements)} potential download links within it.")

                        except Exception as find_links_error:
                            logger.warning(f"    Could not reliably locate the 'Lista de Documentos' table or links within it: {find_links_error}")
                            # Optional: Fallback to searching the whole page? Or just skip? For now, we skip.
                            link_elements = [] # Ensure it's empty if search failed

                        if not link_elements:
                            logger.info("    No download links found on the details page.")
                        else:
                            logger.info(f"    Found {len(link_elements)} potential download links.")
                            for link_index, link_element in enumerate(link_elements):
                                try:
                                    href = await link_element.get_attribute('href')
                                    link_text = (await link_element.text_content() or f"download_{link_index}").strip()

                                    if href:
                                        # Ensure it's a full URL
                                        if not href.startswith(('http://', 'https://')):
                                            # Attempt to construct full URL relative to the current page
                                            current_page_url = page.url
                                            from urllib.parse import urljoin
                                            download_url = urljoin(current_page_url, href)
                                        else:
                                            download_url = href

                                        # --- Modify link using JavaScript to force download ---                                            
                                        logger.debug(f"      Modifying link via JS: Remove target, add download='{link_text}'")
                                        try:
                                            await page.evaluate("""(args) => {
                                                const link = document.querySelector(`a[href="${args.href}"]`);
                                                if (link) {
                                                    link.removeAttribute('target'); // Remove target=\"_blank\"
                                                    link.download = args.filename;     // Add download attribute
                                                }
                                            }""", {"href": href, "filename": link_text})
                                        except Exception as js_err:
                                            logger.warning(f"      Failed to modify link via JS: {js_err}")
                                        # --- End link modification ---

                                        logger.info(f"      Attempting download {link_index + 1}/{len(link_elements)} from: {download_url}")

                                        # Initiate download (should now work correctly)
                                        async with page.expect_download(timeout=120000) as download_info: # 2 min timeout
                                            # Sometimes a direct click works, sometimes navigating is needed
                                            try:
                                                await link_element.click(timeout=15000) # Click to trigger download
                                            except Exception as click_err:
                                                logger.warning(f"        Click failed ({click_err}), trying navigation for {download_url}")
                                                await page.goto(download_url, wait_until='domcontentloaded') # Navigate if click fails

                                        download = await download_info.value
                                        suggested_filename = download.suggested_filename or f"downloaded_file_{link_index}"

                                        # Create a unique path within the processo folder (using temp_download_dir)
                                        processo_folder = temp_download_dir / safe_processo_number
                                        processo_folder.mkdir(parents=True, exist_ok=True)
                                        save_path = processo_folder / suggested_filename

                                        # Ensure unique filename if exists
                                        counter = 1
                                        original_save_path = save_path
                                        while save_path.exists():
                                            save_path = original_save_path.with_stem(f"{original_save_path.stem}_{counter}")
                                            counter += 1
                                            
                                        await download.save_as(save_path)
                                        logger.info(f"        Successfully downloaded and saved to: {save_path}")
                                        downloaded_file_paths.append(str(save_path))

                                        # Handle zip extraction (optional)
                                        if save_path.suffix.lower() == '.zip':
                                            logger.info(f"        Extracting zip file: {save_path}")
                                            try:
                                                with zipfile.ZipFile(save_path, 'r') as zip_ref:
                                                    zip_ref.extractall(processo_folder)
                                                logger.info(f"        Successfully extracted zip to: {processo_folder}")
                                                # Optionally remove zip after extraction: os.remove(save_path)
                                            except zipfile.BadZipFile:
                                                logger.error(f"        Error: Bad zip file encountered: {save_path}")
                                            except Exception as zip_err:
                                                logger.error(f"        Error extracting zip file {save_path}: {zip_err}")

                                except Exception as download_error:
                                    # Use download_url in log for clarity, href might be relative
                                    logger.error(f"      Error downloading/saving file from link {download_url}: {download_error}")
                                    licitacao_success = False # Mark as partially failed if one download fails

                        # --- Zipping and Cleanup ---                            
                        if licitacao_success and downloaded_file_paths:
                            logger.info(f"    Zipping {len(downloaded_file_paths)} downloaded files for {processo}...")
                            zip_file_name = f"CESAN_{safe_processo_number}.zip"
                            zip_file_path = archive_dir / zip_file_name
                            zip_error = False
                            try:
                                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                    for file_path in downloaded_file_paths:
                                        if os.path.exists(file_path):
                                            # Add file to zip using just the filename
                                            zip_file.write(file_path, os.path.basename(file_path))
                                            logger.debug(f"        Added {os.path.basename(file_path)} to zip.")
                                        else:
                                            logger.warning(f"        File not found for zipping: {file_path}")
                                logger.info(f"      Zip created successfully: {zip_file_path}")

                                # Delete original files AFTER successful zipping
                                logger.info("      Deleting original downloaded files...")
                                for file_path in downloaded_file_paths:
                                    try:
                                        if os.path.exists(file_path):
                                            os.unlink(file_path)
                                            logger.debug(f"        Deleted: {file_path}")
                                    except Exception as delete_err:
                                        logger.warning(f"        Error deleting original file {os.path.basename(file_path)}: {delete_err}")
                                # Optionally delete the temporary process folder if empty
                                try:
                                    if processo_folder.exists() and not any(processo_folder.iterdir()):
                                        processo_folder.rmdir()
                                        logger.debug(f"        Removed empty temp folder: {processo_folder}")
                                except Exception as rmdir_err:
                                     logger.warning(f"        Could not remove temp folder {processo_folder}: {rmdir_err}")
                                     
                            except Exception as zip_err:
                                logger.error(f"      ERROR creating zip file {zip_file_path}: {zip_err}")
                                zip_error = True
                                licitacao_success = False # Mark as failed if zipping fails

                        elif not downloaded_file_paths and licitacao_success:
                             logger.info(f"    No files were downloaded for {processo}, skipping zip.")

                        # Add to processed list only if all downloads (if any) were successful AND zipping worked
                        if licitacao_success:
                            processed_list.append(processo)
                            newly_processed_count += 1
                            logger.info(f"    Successfully processed and downloaded files for Processo: {processo}")
                        else:
                             logger.warning(f"    Processo {processo} processed with download errors. Not adding to processed list yet.")

                    except Exception as detail_page_error:
                        logger.error(f"    Error processing details page {url} for Processo {processo}: {detail_page_error}")
                        licitacao_success = False # Mark as failed

                    finally:
                         # Save processed list after each attempt (success or partial failure)
                        save_processed_list(str(processed_json_path), processed_list)
                        logger.info(f"    Saved processed list ({len(processed_list)} items).")
                        # Click the 'Voltar' button to go back to the results page
                        try:
                            logger.info("    Clicking 'Voltar' button...")
                            voltar_button_selector = "button.btn.blue:has-text('Voltar')"
                            await page.locator(voltar_button_selector).click(timeout=10000)
                            logger.info("    Waiting for navigation after clicking 'Voltar'...")
                            await page.wait_for_load_state('networkidle', timeout=20000) # Wait for page load
                            await page.wait_for_timeout(2000) # Extra buffer
                            logger.info("    Successfully navigated back.")
                        except Exception as back_error:
                            logger.error(f"    Error clicking 'Voltar' button or waiting for navigation: {back_error}")
                            # Decide how to handle this: maybe break, or try to re-navigate to search_url?
                            # For now, it will just log the error and the main loop will continue to the next URL.


                logger.info(f"\n===== Processing Complete. Processed {newly_processed_count} new licitações. =====")


            except Exception as e:
                logger.error(f"An error occurred during the main processing loop: {e}")
                # Optionally re-raise the error if needed: raise e
            finally:
                if browser:
                    logger.info("Closing browser.")
                    await browser.close()

    except Exception as outer_error:
        logger.error(f"A critical error occurred outside the main loop: {outer_error}")
        # Optionally re-raise the error: raise outer_error

if __name__ == "__main__":
    # Ensure the script runs within an asyncio event loop
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "Cannot run the event loop while another loop is running" in str(e):
            logger.warning("Event loop already running. Attempting to get existing loop.")
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
        else:
            raise e
