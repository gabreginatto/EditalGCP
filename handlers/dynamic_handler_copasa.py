#!/usr/bin/env python3
"""
COPASA Download Handler for Procurement Download System

This specialized handler:
1. Processes search result URLs from the COPASA procurement portal.
2. Identifies active procurement processes (Estágio != 'Encerrado').
3. Navigates to the detail page for each active process.
4. Clicks the 'Anexos' tab.
5. Extracts download links for all attachments.
6. Downloads files using requests with session cookies.
7. Creates a ZIP archive for each process containing all its attachments.
8. Manages state to avoid re-downloading already processed items.

Usage:
    python dynamic_handler_COPASA.py --url URL --output-dir DIR [--timeout SECONDS]
"""

import os
import sys
import json
import time
import argparse
import logging
import re
import traceback
import asyncio
import shutil
import zipfile
import requests
from pathlib import Path
from urllib.parse import urlparse, unquote, urljoin
from datetime import datetime
from typing import Set, Dict, Optional, Tuple, List

# Import Playwright components
try:
    from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
except ImportError:
    # If playwright is not installed, return error JSON and exit
    print(json.dumps({
        "success": False,
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)

# --- Logging Configuration ---
# Use absolute path for downloads base directory
script_dir = os.path.dirname(os.path.abspath(__file__))
# Navigate up two levels from handlers/ to the main project dir, then into downloads
base_download_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'downloads'))

log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = f"dynamic_handler_copasa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("COPASAHandler")

# --- Configuration ---
STATE_FILE_NAME = "processed_copasa.json"

# --- Helper Functions ---

def setup_output_dirs(base_dir):
    """Ensure all necessary output directories exist relative to the base download dir."""
    # Ensure base_dir is absolute
    base_dir_abs = os.path.abspath(base_dir)
    logger.info(f"Setting up directories relative to: {base_dir_abs}")

    output_dirs = {
        'pdf': os.path.join(base_dir_abs, 'pdfs'),
        'archive': os.path.join(base_dir_abs, 'archives'),
        'debug': os.path.join(base_dir_abs, 'debug'),
        'logs': os.path.join(base_dir_abs, 'logs'), # Log dir is already created, but ensure it's here
        'screenshots': os.path.join(base_dir_abs, 'debug', 'screenshots'),
        'temp': os.path.join(base_dir_abs, 'temp'),
        'state': base_dir_abs # For the state file
    }

    for key, dir_path in output_dirs.items():
        # Only create directories, not the state file path itself
        if key != 'state':
            try:
                os.makedirs(dir_path, exist_ok=True)
                # logger.debug(f"Directory ensured: {dir_path}")
            except OSError as e:
                logger.error(f"Failed to create directory {dir_path}: {e}")
                raise # Reraise if directory creation fails

    return output_dirs

def clean_filename(filename: str, max_length: int = 100) -> str:
    """Clean a filename to make it safe for the filesystem."""
    if not filename:
        return f"unknown_file_{int(time.time())}"
    # Remove path components just in case
    filename = os.path.basename(filename)
    # Replace problematic characters
    filename = re.sub(r'[\\/*?:"<>|]', '_', filename)
    # Replace multiple spaces/underscores with a single underscore
    filename = re.sub(r'[\s_]+', '_', filename)
    # Remove leading/trailing underscores/spaces
    filename = filename.strip('_ ')
    # Limit length
    if len(filename) > max_length:
        name, ext = os.path.splitext(filename)
        ext = ext[:max_length] # Ensure extension doesn't exceed max length either
        name = name[:max_length - len(ext)]
        filename = name + ext
    # Handle case where filename becomes empty after cleaning
    if not filename:
        return f"cleaned_empty_{int(time.time())}"
    return filename

def create_zip_archive(file_paths: list[str], output_zip_path: str) -> bool:
    """Create a ZIP archive containing multiple files."""
    if not file_paths:
        logger.warning(f"No files provided to create zip: {output_zip_path}")
        return False

    try:
        logger.info(f"Creating ZIP archive: {output_zip_path} with {len(file_paths)} file(s)")
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                if os.path.exists(file_path):
                    # Add file to zip using just its base name
                    zipf.write(file_path, os.path.basename(file_path))
                    logger.debug(f"Added {os.path.basename(file_path)} to {os.path.basename(output_zip_path)}")
                else:
                    logger.warning(f"File not found, cannot add to zip: {file_path}")
        logger.info(f"Successfully created ZIP archive: {output_zip_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating ZIP archive {output_zip_path}: {e}", exc_info=True)
        # Attempt to remove partially created zip file
        if os.path.exists(output_zip_path):
            try:
                os.remove(output_zip_path)
            except Exception as rm_e:
                logger.error(f"Failed to remove partial zip {output_zip_path}: {rm_e}")
        return False

def load_processed_state(output_dirs: Dict[str, str]) -> Set[str]:
    """Load the set of processed process numbers from the state file."""
    state_file_path = os.path.join(output_dirs['state'], STATE_FILE_NAME)
    try:
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                processed_set = set(data.get('processed_copasa_processos', []))
                logger.info(f"Loaded {len(processed_set)} processed IDs from {state_file_path}")
                return processed_set
        else:
            logger.info(f"State file not found ({state_file_path}). Starting fresh.")
            return set()
    except (json.JSONDecodeError, IOError, Exception) as e:
        logger.error(f"Error loading state file {state_file_path}: {e}. Starting fresh.", exc_info=True)
        return set()

def save_processed_state(output_dirs: Dict[str, str], processed_set: Set[str]):
    """Save the set of processed process numbers to the state file."""
    state_file_path = os.path.join(output_dirs['state'], STATE_FILE_NAME)
    try:
        data = {'processed_copasa_processos': sorted(list(processed_set))}
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(processed_set)} processed IDs to {state_file_path}")
    except (IOError, Exception) as e:
        logger.error(f"Error saving state file {state_file_path}: {e}", exc_info=True)

async def download_file_requests(url: str, target_path: str, cookies: List[Dict], referer: str) -> bool:
    """Download a single file using requests with session context."""
    try:
        logger.info(f"Attempting download via requests: {url}")
        logger.debug(f"Initial target path: {target_path}")

        session = requests.Session()
        # Load cookies into session
        for cookie in cookies:
            # Only add cookies that have necessary fields
            if 'name' in cookie and 'value' in cookie and 'domain' in cookie and 'path' in cookie:
                 session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'], path=cookie['path'])
            else:
                 logger.warning(f"Skipping invalid cookie: {cookie}")


        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': referer
        }

        # Disable insecure request warnings for verify=False
        requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

        response = session.get(url, headers=headers, stream=True, timeout=60, verify=False)
        response.raise_for_status() # Raise exception for bad status codes

        # Handle potential filename issues from headers
        filename_from_header = None
        if 'Content-Disposition' in response.headers:
            cd = response.headers['Content-Disposition']
            # Improved regex to handle different quoting and encoding prefixes
            fname_match = re.search(r'filename\*?=(?:(?:UTF-8|utf-8)\'\')?["\']?([^"\';]+)["\']?', cd, re.IGNORECASE)
            if fname_match:
                filename_from_header = unquote(fname_match.group(1))
                logger.info(f"Filename from Content-Disposition: {filename_from_header}")
                # Update target_path using the potentially more accurate filename
                target_path = os.path.join(os.path.dirname(target_path), clean_filename(filename_from_header))
                logger.info(f"Using filename from header for saving: {target_path}")

        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        logger.info(f"Saving downloaded file to: {target_path}")
        with open(target_path, 'wb') as f:
            downloaded_bytes = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_bytes += len(chunk)

        if downloaded_bytes > 0:
            logger.info(f"Successfully downloaded {downloaded_bytes} bytes: {target_path}")
            return True
        else:
            logger.warning(f"Download completed but file is empty: {target_path}")
            # Attempt to remove empty file
            try:
                os.remove(target_path)
            except Exception:
                pass
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"Requests error downloading {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}", exc_info=True)
        return False

async def process_detail_page(page: Page, context, objeto_clean: str, processo_num_clean: str, output_dirs: Dict[str, str]) -> bool:
    """Handles finding and downloading attachments and the Relacao Materiais PDF."""
    anexos_processed = False
    relacao_processed = False
    relacao_downloaded_this_step = False
    final_zip_success = False
    temp_process_dir = os.path.join(output_dirs['temp'], processo_num_clean)
    os.makedirs(temp_process_dir, exist_ok=True)
    downloaded_file_paths = []

    # Define selectors
    anexos_tab_selector = 'div.sapMITBFilter[id$="__filter2"]'
    relacao_tab_selector = 'div.sapMITBFilter[id*="tabRelMat"]'
    content_area_selector = 'div[id$="--idIconTabBarMulti-content"]'
    anexos_link_selector = f'{content_area_selector} div.sapMFlexBox >> a.sapMLnk[href]'
    anexos_no_data_selector = f'{content_area_selector} span.sapMText:has-text("Nenhum anexo encontrado"), {content_area_selector} div.sapUiTableCtrlEmpty'
    # We won't need iframe/button selectors if interception works

    try:
        logger.info(f"Processing detail page for {processo_num_clean}")
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_1_initial.png"))

        # ---------------------------------
        # Step 1: Process 'Anexos' Tab
        # ---------------------------------
        logger.info("--- Processing 'Anexos' Tab ---")
        try:
            anexos_tab = page.locator(anexos_tab_selector).first
            await anexos_tab.wait_for(state="visible", timeout=20000)
            if await anexos_tab.get_attribute("aria-selected") != "true":
                anexos_click_success = False
                for attempt in range(3):
                    # ... click retry loop ...
                    try:
                        if attempt == 0: await anexos_tab.click(timeout=10000)
                        elif attempt == 1: await anexos_tab.click(force=True, timeout=10000)
                        else: await anexos_tab.evaluate("element => element.click()")
                        await page.wait_for_timeout(1500)
                        if await anexos_tab.get_attribute("aria-selected") == "true":
                            logger.info("Anexos tab selected.")
                            anexos_click_success = True; break
                        else: logger.warning(f"Anexos aria-selected still false after attempt {attempt + 1}.")
                    except Exception as click_err: logger.error(f"Anexos click attempt {attempt + 1} error: {click_err}")
                if not anexos_click_success: raise Exception("Failed to select Anexos tab.")
            else: logger.info("Anexos tab already selected.")

            await page.wait_for_timeout(5000) # Wait for content
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_3_anexos_tab_selected.png"))

            no_anexos_files = False
            try:
                 if await page.locator(anexos_no_data_selector).first.is_visible(timeout=2000):
                     logger.info(f"No Anexos files found for {processo_num_clean}.")
                     no_anexos_files = True
            except (PlaywrightTimeoutError, PlaywrightError): pass

            if not no_anexos_files:
                logger.info(f"Looking for Anexos download links...")
                try:
                    await page.locator(anexos_link_selector).first.wait_for(state="visible", timeout=20000)
                    download_links = await page.locator(anexos_link_selector).all()
                    logger.info(f"Found {len(download_links)} potential Anexos links.")
                    cookies = await context.cookies()
                    page_url = page.url
                    for i, link_locator in enumerate(download_links):
                        try:
                            href = await link_locator.get_attribute('href'); download_url = urljoin(page_url, href)
                            displayed_filename = f"anexo_{i+1}"; temp_target_path = os.path.join(temp_process_dir, clean_filename(displayed_filename))
                            if await download_file_requests(download_url, temp_target_path, cookies, page_url):
                                actual_file_path = temp_target_path if os.path.exists(temp_target_path) else None
                                if not actual_file_path: files = list(Path(temp_process_dir).glob('*')); actual_file_path = str(max(files, key=os.path.getctime)) if files else None
                                if actual_file_path and os.path.exists(actual_file_path): downloaded_file_paths.append(actual_file_path)
                                else: logger.error(f"Anexo DL success but file missing: {temp_target_path}")
                            else: logger.error(f"Failed Anexo DL: {download_url}")
                            await asyncio.sleep(0.5)
                        except Exception as dl_err: logger.error(f"Error processing Anexo link {i+1}: {dl_err}", exc_info=True)
                except (PlaywrightTimeoutError, PlaywrightError) as e: logger.warning(f"Timeout/Error finding Anexos links: {e}")

            anexos_processed = True

        except Exception as anexos_err:
             logger.error(f"Error during 'Anexos' processing stage: {anexos_err}", exc_info=True)
             anexos_processed = False

        # -----------------------------------------
        # Step 2: Process 'Relação Materiais' Tab
        # -----------------------------------------
        logger.info("--- Processing 'Relação Materiais' Tab ---")
        pdf_request_url = None
        request_handler_finished = asyncio.Event()

        # --- Define async handler for network requests ---
        async def handle_route(route):
            nonlocal pdf_request_url
            request = route.request
            url = request.url
            resource_type = request.resource_type

            # --- Log ALL intercepted requests for debugging ---
            logger.debug(f"Intercepted Route: URL={url}, Type={resource_type}")
            # --- End Logging ---

            # Only intercept if we haven't found the PDF URL yet
            if pdf_request_url is None:
                is_pdf_likely = False
                # Check for URLs likely containing the PDF
                if ('docserver' in url.lower() or url.lower().endswith('.pdf')) and not url.lower().endswith(('.js', '.css', '.woff', '.woff2', '.png', '.jpg', '.jpeg', '.gif')):
                     is_pdf_likely = True
                     logger.debug(f"MATCHED PDF heuristic: URL={url}")
                # Add another check: maybe the PDF is served via a generic endpoint with specific params?
                elif 'sap/opu/odata' in url.lower() and '$value' in url.lower() : # Common SAP pattern for files
                     is_pdf_likely = True
                     logger.debug(f"MATCHED SAP OData heuristic: URL={url}")
                # Special case for COPASA's zsrm_viewpdf endpoint (Relação Materiais)
                elif 'sap/bc/pagina/zsrm_viewpdf' in url.lower():
                     is_pdf_likely = True
                     logger.debug(f"MATCHED COPASA's zsrm_viewpdf heuristic: URL={url}")


                if is_pdf_likely:
                     logger.info(f"Intercepted CANDIDATE PDF request: {url}")
                     pdf_request_url = url # Capture the URL
                     request_handler_finished.set() # Signal found
                     try:
                         logger.debug(f"Aborting request for captured PDF URL: {url}")
                         await route.abort()
                         return # Stop processing this route
                     except PlaywrightError as abort_err:
                         if "Request is already handled" not in str(abort_err):
                              logger.warning(f"Error aborting route for {url}: {abort_err}")
                         if not request_handler_finished.is_set(): request_handler_finished.set()
                         return
            # --- Corrected Continue Logic ---
            try:
                # Simply attempt to continue
                await route.continue_()
            except PlaywrightError as cont_err:
                # Log only unexpected errors
                if "Request context disposed" not in str(cont_err) and \
                   "Request is already handled" not in str(cont_err):
                    logger.warning(f"Error continuing route for {url}: {cont_err}")
            except Exception as gen_cont_err:
                 logger.error(f"Generic error continuing route for {url}: {gen_cont_err}", exc_info=True)

        # --- Setup Interception BEFORE Clicking ---
        route_handler = lambda route: asyncio.create_task(handle_route(route))
        try:
            logger.info("Setting up network interception for Relacao Materiais PDF URL...")
            await page.route("**/*", route_handler)

            relacao_tab = page.locator(relacao_tab_selector).first
            await relacao_tab.wait_for(state="visible", timeout=15000)

            if await relacao_tab.get_attribute("aria-selected") != "true":
                 relacao_click_success = False
                 for attempt in range(3):
                     logger.info(f"Attempting Relacao Materiais click (Attempt {attempt + 1})...")
                     try:
                         if attempt == 0: await relacao_tab.click(timeout=10000)
                         elif attempt == 1: await relacao_tab.click(force=True, timeout=10000)
                         else: await relacao_tab.evaluate("element => element.click()")
                         await page.wait_for_timeout(500) # Shorter wait, network handler is active
                         if await relacao_tab.get_attribute("aria-selected") == "true":
                             logger.info("Relacao Materiais tab selected.")
                             relacao_click_success = True; break
                         else: logger.warning(f"Relacao aria-selected still false after attempt {attempt + 1}.")
                     except Exception as click_err: logger.error(f"Relacao click attempt {attempt + 1} error: {click_err}")
                 if not relacao_click_success: raise Exception("Failed to select Relacao Materiais tab.")
            else:
                 logger.info("Relacao Materiais tab already selected.")
                 relacao_click_success = True

            # --- Wait for PDF Request to be Intercepted ---
            if relacao_click_success:
                try:
                    logger.debug("Waiting up to 30s for PDF URL interception...")
                    await asyncio.wait_for(request_handler_finished.wait(), timeout=30.0)
                    logger.info("PDF URL interception signal received.")
                except asyncio.TimeoutError:
                    logger.warning("Timed out waiting for PDF request interception for Relacao Materiais.")
                    await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_7_relacao_intercept_timeout.png"))

            # --- Download if URL was captured ---
            if pdf_request_url:
                logger.info(f"Proceeding to download captured PDF URL: {pdf_request_url}")
                target_filename = f"Relacao_Materiais_{processo_num_clean}.pdf"
                target_path = os.path.join(temp_process_dir, clean_filename(target_filename))
                cookies = await context.cookies()
                if await download_file_requests(pdf_request_url, target_path, cookies, page.url):
                     # Check if the target path exists, but also look for other recently created PDF files
                     # in case the filename was changed by Content-Disposition header
                     actual_file_path = target_path if os.path.exists(target_path) else None
                     if not actual_file_path:
                         # Look for any recently created PDF files in the temp directory
                         try:
                             pdf_files = list(Path(temp_process_dir).glob('*.pdf'))
                             if pdf_files:
                                 # Get the most recently created PDF file
                                 actual_file_path = str(max(pdf_files, key=os.path.getctime))
                                 logger.info(f"Using actual downloaded PDF path: {actual_file_path}")
                         except Exception as find_err:
                             logger.error(f"Error finding actual PDF file: {find_err}")
                     
                     if actual_file_path and os.path.exists(actual_file_path): 
                         downloaded_file_paths.append(actual_file_path)
                         relacao_downloaded_this_step = True
                     else:
                         logger.error(f"Relacao PDF download reported success but file is missing")
                else:
                     logger.error(f"Failed to download captured Relacao Materiais PDF URL: {pdf_request_url}")
            else:
                 logger.warning("No PDF URL was captured via network interception for Relacao Materiais.")

            relacao_processed = True # Mark Relacao stage as attempted

        except Exception as relacao_err:
             logger.error(f"Error during 'Relação Materiais' processing stage: {relacao_err}", exc_info=True)
             relacao_processed = False # Mark stage itself as failed
        finally:
             # --- ALWAYS Unroute ---
             try:
                  logger.debug("Removing network interception.")
                  await page.unroute("**/*", handler=route_handler)
             except Exception as unroute_err:
                  logger.error(f"Error trying to unroute: {unroute_err}")


        # ---------------------------------
        # Step 3: Create Final ZIP Archive
        # ---------------------------------
        if downloaded_file_paths:
            zip_filename = f"{objeto_clean}_{processo_num_clean}.zip"
            zip_filepath = os.path.join(output_dirs['archive'], zip_filename)
            if create_zip_archive(downloaded_file_paths, zip_filepath):
                final_zip_success = True
                if not relacao_downloaded_this_step and relacao_processed:
                     logger.warning(f"ZIP created for {processo_num_clean}, but 'Relação Materiais' PDF might be missing (download failed).")
                elif not relacao_processed:
                     logger.warning(f"ZIP created for {processo_num_clean}, but 'Relação Materiais' stage failed.")
            else:
                logger.error(f"Failed to create final ZIP for {processo_num_clean}")
        elif anexos_processed and relacao_processed:
             logger.info(f"Processed detail page for {processo_num_clean}, but no files were downloaded or zipped.")
             final_zip_success = True
        else:
             logger.error(f"Did not successfully process all required stages for {processo_num_clean}. No ZIP created.")
             final_zip_success = False

    except Exception as e:
        logger.error(f"Critical error processing detail page for {processo_num_clean}: {e}", exc_info=True)
        final_zip_success = False
    finally:
        # Clean up temp directory
        try:
            if os.path.exists(temp_process_dir): shutil.rmtree(temp_process_dir)
        except Exception as cleanup_e: logger.error(f"Error cleaning temp dir {temp_process_dir}: {cleanup_e}")

    return final_zip_success

async def process_search_page(page: Page, context, search_url: str, output_dirs: Dict[str, str], processed_state: Set[str]) -> Set[str]:
    newly_processed_in_this_run = set()
    try:
        logger.info(f"Processing search results page: {search_url}")
        await page.goto(search_url, wait_until="networkidle", timeout=60000)
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_{clean_filename(urlparse(search_url).fragment)}.png"))

        table_selector = 'div[id$="ViewContentSearchList--table-tableCCnt"]'
        row_selector = f'{table_selector} tr.sapUiTableContentRow:not(.sapUiTableRowHidden)'

        try:
            logger.debug(f"Waiting for table container: {table_selector}")
            await page.locator(table_selector).wait_for(state="visible", timeout=30000)
            await page.wait_for_timeout(3000)
            rows = await page.locator(row_selector).all()
            logger.info(f"Found {len(rows)} visible rows.")
        except Exception as e:
            logger.error(f"Could not find table/rows on {search_url}: {e}")
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_fail_table_{clean_filename(urlparse(search_url).fragment)}.png"))
            return newly_processed_in_this_run

        if not rows: logger.info(f"No rows found on {search_url}."); return newly_processed_in_this_run

        rows_data = []
        for i, row_locator in enumerate(rows):
             try:
                estagio_locator = row_locator.locator('td[data-sap-ui-colid*="estagioId"]')
                estagio_text = await estagio_locator.text_content(timeout=5000) if await estagio_locator.count() else ""
                if "Encerrado" in estagio_text: continue

                processo_link_locator = row_locator.locator('td[data-sap-ui-colid*="numeroProcessoId"] a.sapMLnk')
                objeto_locator = row_locator.locator('td[data-sap-ui-colid*="objetoId"]')

                if await processo_link_locator.count() == 0 or await objeto_locator.count() == 0: continue

                processo_num_text = await processo_link_locator.text_content(timeout=5000)
                processo_num_clean = clean_filename(processo_num_text.strip())
                if not processo_num_clean: continue

                if processo_num_clean in processed_state or processo_num_clean in newly_processed_in_this_run:
                    logger.info(f"Row {i}: Process {processo_num_clean} already handled. Skipping.")
                    continue

                objeto_text = await objeto_locator.text_content(timeout=5000) or "UnknownObjeto"
                objeto_clean = clean_filename(objeto_text.strip(), max_length=60)

                logger.info(f"Row {i}: Found active process {processo_num_clean} ('{objeto_clean}'). Queuing.")
                rows_data.append({
                    "processo_num_text": processo_num_text.strip(),
                    "processo_num": processo_num_clean,
                    "objeto": objeto_clean
                })
             except Exception as row_err: logger.error(f"Error extracting row {i} data: {row_err}", exc_info=True)

        logger.info(f"Processing details for {len(rows_data)} new active processes...")
        for data in rows_data:
            processo_num = data['processo_num']; objeto = data['objeto']; processo_num_text_original = data['processo_num_text']
            link_selector_for_click = f'td[data-sap-ui-colid*="numeroProcessoId"] a.sapMLnk:has-text("{processo_num_text_original}")'
            success = False
            try:
                logger.info(f"Locating and clicking link for process {processo_num}...")
                current_link_locator = page.locator(link_selector_for_click).first
                await current_link_locator.wait_for(state="visible", timeout=10000)
                await current_link_locator.click()
                await page.wait_for_load_state("networkidle", timeout=60000)

                success = await process_detail_page(page, context, objeto, processo_num, output_dirs)

                if success:
                    logger.info(f"Successfully processed detail page for {processo_num}")
                    newly_processed_in_this_run.add(processo_num)
                else: logger.warning(f"Failed processing detail page for {processo_num}")

                logger.info(f"Navigating back to search results from {processo_num}.")
                await page.go_back(wait_until="networkidle", timeout=60000)
                logger.debug("Waiting for search table after go_back...")
                await page.locator(table_selector).wait_for(state="visible", timeout=45000)
                await page.wait_for_timeout(2000)
                logger.debug("Search table ready after go_back.")

            except Exception as detail_e:
                logger.error(f"Error during detail processing/navigation for {processo_num}: {detail_e}", exc_info=True)
                await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_processing_error_{processo_num}.png"))
                try: # Recovery attempt
                    logger.warning(f"Attempting recovery navigation to {search_url}")
                    await page.goto(search_url, wait_until="networkidle", timeout=60000)
                    await page.locator(table_selector).wait_for(state="visible", timeout=30000)
                except Exception as recovery_e:
                    logger.error(f"Recovery failed for {search_url}: {recovery_e}. Aborting.", exc_info=True)
                    break

        logger.info(f"Finished processing search page: {search_url}")

    except PlaywrightTimeoutError as pte:
         logger.error(f"Timeout error on search page {search_url}: {pte}")
         try: await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"timeout_error_{clean_filename(urlparse(search_url).fragment)}.png"))
         except Exception: pass
    except Exception as e:
        logger.error(f"General error on search page {search_url}: {e}", exc_info=True)
        try: await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"general_error_{clean_filename(urlparse(search_url).fragment)}.png"))
        except Exception: pass

    return newly_processed_in_this_run

async def handle_copasa_download(url, output_dir, timeout=300):
    playwright = None; browser = None; context = None
    overall_success = False; processed_at_least_one = False; error_message = None
    try:
        output_dirs = setup_output_dirs(output_dir)
        processed_state = load_processed_state(output_dirs)
        playwright = await async_playwright().start()
        headless = False
        logger.info(f"Starting browser with headless={headless}")
        browser = await playwright.chromium.launch(headless=headless, args=["--disable-web-security", "--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(
            accept_downloads=True, # MUST be True for expect_download
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            viewport={"width": 1280, "height": 800}, 
            ignore_https_errors=True
        )
        context.set_default_timeout(60000)
        context.set_default_navigation_timeout(90000) # Set navigation timeout after context creation
        page = await context.new_page()
        newly_processed = await process_search_page(page, context, url, output_dirs, processed_state)
        if newly_processed:
            processed_at_least_one = True
            updated_state = processed_state.union(newly_processed)
            save_processed_state(output_dirs, updated_state)
        else: logger.info(f"No new processes downloaded from {url} in this run.")
        overall_success = True
    except Exception as e:
        logger.error(f"Critical error in handle_copasa_download for {url}: {e}", exc_info=True)
        overall_success = False; error_message = f"Critical error: {e}"
    finally:
        # Ensure context/browser close gracefully
        if context:
            try: await context.close()
            except Exception as e: logger.error(f"Ctx close err: {e}")
        if browser:
            try: await browser.close()
            except Exception as e: logger.error(f"Browser close err: {e}")
        if playwright:
            try: await playwright.stop()
            except Exception as e: logger.error(f"PW stop err: {e}")
        logger.info("Playwright resources cleaned up.")

    result = { "success": overall_success, "url": url, "file_path": None, "error_message": error_message, "processed_new_items": processed_at_least_one }
    print(json.dumps(result))
    return 0 if overall_success else 1

async def main():
    parser = argparse.ArgumentParser(description="COPASA Download Handler")
    parser.add_argument("--url", required=True, help="URL of the COPASA search results page")
    parser.add_argument("--output-dir", default=base_download_dir, help=f"Base output directory (defaults to: {base_download_dir})")
    parser.add_argument("--timeout", type=int, default=600, help="Overall timeout for the handler in seconds")
    args = parser.parse_args()
    output_dir_abs = os.path.abspath(args.output_dir)
    exit_code = await handle_copasa_download(args.url, output_dir_abs, args.timeout)
    sys.exit(exit_code)

if __name__ == "__main__":
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())