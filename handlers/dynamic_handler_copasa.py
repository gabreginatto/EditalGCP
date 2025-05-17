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
    python dynamic_handler_copasa.py --url URL --output-dir DIR [--company-id ID] [--timeout SECONDS] [--headless]
"""

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
import sys
import time
import traceback
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import unquote, urljoin, urlparse

import requests
from playwright.async_api import (
    BrowserContext,
    Error as PlaywrightError,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

# Disable insecure request warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Default timeout for browser operations (in seconds)
DEFAULT_TIMEOUT = 300

# Import Playwright components
try:
    from playwright.async_api import async_playwright, Page, Locator, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError, BrowserContext
except ImportError:
    # If playwright is not installed, return error JSON and exit
    print(json.dumps({
        "success": False,
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)

# --- Helper Functions ---
def setup_logging(output_dir: Path, company_id: str) -> Any:
    """
    Set up logging configuration for the handler.
    
    Args:
        output_dir: Base output directory path
        company_id: Company identifier for log file naming
        
    Returns:
        Logger object configured for this handler
    """
    import logging
    
    # Create logs directory
    log_dir = output_dir / company_id / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create log file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"dynamic_handler_copasa_{company_id}_{timestamp}.log"
    log_path = log_dir / log_file
    
    # Configure logger
    logger = logging.getLogger(f"COPASAHandler_{company_id}")
    logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers (important for repeated runs)
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Add file handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create formatter and add to handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized for COPASA handler for company_id: {company_id}")
    return logger

# --- Configuration ---
STATE_FILE_NAME_TEMPLATE = "processed_copasa_{}.json"

# --- Helper Functions ---

def setup_output_dirs(base_dir: Path, company_id: str, logger: Any) -> Dict[str, Path]:
    """
    Ensure all necessary output directories exist relative to the base output directory.
    Directory structure follows the pattern: base_dir/company_id/subdir
    
    Args:
        base_dir: Base output directory path
        company_id: Company identifier for directory structure
        logger: Logger instance
    
    Returns:
        Dictionary of output directory paths
    """
    # Ensure base_dir is a Path object
    base_dir_path = Path(base_dir)
    company_dir = base_dir_path / company_id
    logger.info(f"Setting up directories relative to: {company_dir}")

    output_dirs = {
        'archive': company_dir / 'archives',
        'logs': company_dir / 'logs',  # Log dir is already created, but ensure it's here
        'screenshots': company_dir / 'screenshots',
        'temp': company_dir / 'temp',
        'state': company_dir  # For the state file
    }

    for key, dir_path in output_dirs.items():
        # Only create directories, not the state file path itself
        if key != 'state':
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Directory ensured: {dir_path}")
            except OSError as e:
                logger.error(f"Failed to create directory {dir_path}: {e}")
                raise  # Reraise if directory creation fails

    return output_dirs

def clean_filename(filename: str, max_length: int = 100) -> str:
    """
    Clean a filename to make it safe for the filesystem.
    
    Args:
        filename: The filename to clean
        max_length: Maximum length for the filename
        
    Returns:
        A cleaned, filesystem-safe filename
    """
    if not filename:
        return f"unknown_file_{int(time.time())}"
    # Remove path components just in case
    filename = Path(filename).name
    # Replace problematic characters
    filename = re.sub(r'[\\/*?:"<>|]', '_', filename)
    # Replace multiple spaces/underscores with a single underscore
    filename = re.sub(r'[\s_]+', '_', filename)
    # Remove leading/trailing underscores/spaces
    filename = filename.strip('_ ')
    # Limit length
    if len(filename) > max_length:
        name, ext = Path(filename).stem, Path(filename).suffix
        ext = ext[:max_length]  # Ensure extension doesn't exceed max length either
        name = name[:max_length - len(ext)]
        filename = name + ext
    # Handle case where filename becomes empty after cleaning
    if not filename:
        return f"cleaned_empty_{int(time.time())}"
    return filename

def create_zip_archive(file_paths: List[Path], output_zip_path: Path, logger: Any) -> bool:
    """
    Create a ZIP archive containing multiple files.
    
    Args:
        file_paths: List of paths to files to include in the archive
        output_zip_path: Path where the ZIP file will be created
        logger: Logger instance
        
    Returns:
        True if successful, False otherwise
    """
    if not file_paths:
        logger.warning(f"No files provided to create zip: {output_zip_path}")
        return False

    try:
        logger.info(f"Creating ZIP archive: {output_zip_path} with {len(file_paths)} file(s)")
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                file_path_obj = Path(file_path)
                if file_path_obj.exists():
                    # Add file to zip using just its base name
                    zipf.write(file_path_obj, file_path_obj.name)
                    logger.debug(f"Added {file_path_obj.name} to {output_zip_path.name}")
                else:
                    logger.warning(f"File not found, cannot add to zip: {file_path}")
        logger.info(f"Successfully created ZIP archive: {output_zip_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating ZIP archive {output_zip_path}: {e}", exc_info=True)
        # Attempt to remove partially created zip file
        if output_zip_path.exists():
            try:
                output_zip_path.unlink()
            except Exception as rm_e:
                logger.error(f"Failed to remove partial zip {output_zip_path}: {rm_e}")
        return False

def load_processed_state(output_dirs: Dict[str, Path], company_id: str, logger: Any) -> Set[str]:
    """
    Load the set of processed process numbers from the state file.
    
    Args:
        output_dirs: Dictionary of output directories
        company_id: Company identifier for state file
        logger: Logger instance
        
    Returns:
        Set of processed process IDs
    """
    state_file_name = STATE_FILE_NAME_TEMPLATE.format(company_id)
    state_file_path = output_dirs['state'] / state_file_name
    try:
        if state_file_path.exists():
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

def save_processed_state(output_dirs: Dict[str, Path], processed_set: Set[str], company_id: str, logger: Any) -> None:
    """
    Save the set of processed process numbers to the state file.
    
    Args:
        output_dirs: Dictionary of output directories
        processed_set: Set of processed process IDs
        company_id: Company identifier for state file
        logger: Logger instance
    """
    state_file_name = STATE_FILE_NAME_TEMPLATE.format(company_id)
    state_file_path = output_dirs['state'] / state_file_name
    try:
        data = {'processed_copasa_processos': sorted(list(processed_set))}
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(processed_set)} processed IDs to {state_file_path}")
    except (IOError, Exception) as e:
        logger.error(f"Error saving state file {state_file_path}: {e}", exc_info=True)

async def download_file_requests(
    url: str, 
    target_path: Path, 
    cookies: List[Dict], 
    referer: str, 
    logger: Any
) -> Tuple[bool, Optional[Path]]:
    """
    Download a single file using requests with session context.
    
    Args:
        url: URL to download from
        target_path: Path where to save the downloaded file
        cookies: List of cookies to use in the request
        referer: Referer header value
        logger: Logger instance
        
    Returns:
        Tuple of (success: bool, downloaded_path: Optional[Path])
    """
    try:
        logger.info(f"Attempting download: {url}")
        target_path = Path(target_path)
        
        # Ensure parent directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        session = requests.Session()
        
        # Add cookies to session
        for cookie in cookies:
            if all(k in cookie for k in ['name', 'value', 'domain', 'path']):
                session.cookies.set(
                    cookie['name'], 
                    cookie['value'], 
                    domain=cookie['domain'], 
                    path=cookie['path']
                )
            else:
                logger.warning(f"Skipping invalid cookie: {cookie}")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Referer': referer
        }

        # Disable insecure request warnings
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning
        )

        # Make the request
        response = session.get(
            url, 
            headers=headers, 
            stream=True, 
            timeout=60, 
            verify=False
        )
        response.raise_for_status()

        # Handle filename from Content-Disposition if available
        if 'Content-Disposition' in response.headers:
            cd = response.headers['Content-Disposition']
            fname_match = re.search(
                r'filename\*?=(?:(?:UTF-8|utf-8)\'\')?["\']?([^"\';]+)["\']?', 
                cd, 
                re.IGNORECASE
            )
            if fname_match:
                filename = unquote(fname_match.group(1))
                target_path = target_path.parent / clean_filename(filename)
                logger.info(f"Using filename from header: {target_path.name}")

        # Stream the response to file
        with target_path.open('wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if target_path.stat().st_size > 0:
            logger.info(f"Downloaded {target_path.stat().st_size} bytes to {target_path}")
            return True, target_path
        else:
            logger.warning(f"Downloaded file is empty: {target_path}")
            target_path.unlink(missing_ok=True)
            return False, None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error downloading {url}: {e}")
        return False, None
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}", exc_info=True)
        return False, None

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
                    await page.screenshot(path=output_dirs['screenshots'] / f"detail_{processo_num_clean}_7_relacao_intercept_timeout.png")

            # --- Download if URL was captured ---
            if pdf_request_url:
                logger.info(f"Proceeding to download captured PDF URL: {pdf_request_url}")
                target_filename = f"Relacao_Materiais_{processo_num_clean}.pdf"
                target_path = output_dirs['archive'] / target_filename
                cookies = await context.cookies()
                if await download_file_requests(pdf_request_url, target_path, cookies, page.url):
                     # Check if the target path exists, but also look for other recently created PDF files
                     # in case the filename was changed by Content-Disposition header
                     actual_file_path = target_path if target_path.exists() else None
                     if not actual_file_path:
                         # Look for any recently created PDF files in the temp directory
                         try:
                             pdf_files = list(output_dirs['archive'].glob('*.pdf'))
                             if pdf_files:
                                 # Get the most recently created PDF file
                                 actual_file_path = max(pdf_files, key=os.path.getctime)
                                 logger.info(f"Using actual downloaded PDF path: {actual_file_path}")
                         except Exception as find_err:
                             logger.error(f"Error finding actual PDF file: {find_err}")
                     
                     if actual_file_path and actual_file_path.exists(): 
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
            zip_filepath = output_dirs['archive'] / zip_filename
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

async def main() -> None:
    """
    Main entry point for the script.
    
    Handles command-line arguments and executes the download process.
    """
    parser = argparse.ArgumentParser(
        description='Download attachments from COPASA procurement portal.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    parser.add_argument(
        '--url',
        required=True,
        help='URL of the COPASA search results page to start scraping from'
    )
    
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Base directory for saving downloaded files and logs'
    )
    
    # Optional arguments
    parser.add_argument(
        '--company-id',
        default='copasa',
        help='Company identifier used for organizing output files and logs'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=300,
        help='Timeout in seconds for browser operations'
    )
    
    parser.add_argument(
        '--headless',
        action='store_true',
        help='Run browser in headless mode (no GUI)'
    )
    
    # Parse command line arguments
    args = parser.parse_args()
    
    try:
        # Run the download handler
        result = asyncio.run(
            handle_copasa_download(
                url=args.url,
                output_dir=args.output_dir,
                company_id=args.company_id,
                timeout=args.timeout,
                headless=args.headless
            )
        )
        
        # Print the result as JSON
        print(json.dumps(result, indent=2))
        
        # Exit with appropriate status code
        sys.exit(0 if result.get('success', False) else 1)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {str(e)}", file=sys.stderr)
        if hasattr(e, '__traceback__'):
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())