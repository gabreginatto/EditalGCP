#!/usr/bin/env python3
"""
Sanepar Download Handler for Procurement Download System

Handles downloading documents from the Sanepar procurement portal (licitacoes.sanepar.com.br).
This handler is designed to be called by the dispatcher in handlers/__main__.py.

Workflow:
1. Navigates to the main listing page (SLI11000.aspx).
2. Filters processes based on provided keywords in the 'Objeto' column.
3. Avoids reprocessing using a state file (e.g., processed_sanepar.json) within the output directory.
4. For matching processes, navigates to the detail page (SLI11100.aspx).
5. Downloads attachments by simulating form POST requests.
6. Extracts 'Lote' table data and generates a separate PDF.
7. Creates a ZIP archive containing all downloaded files for each process.
8. Returns a list of structured data for processed tenders.
"""

import os
import sys
import json
import time
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
from typing import Set, Dict, Optional, Tuple, List, Any, Union

# Import Playwright components
try:
    from playwright.async_api import (
        async_playwright,
        Page,
        Locator,
        TimeoutError as PlaywrightTimeoutError,
        Error as PlaywrightError,
        BrowserContext
    )
except ImportError:
    # If playwright is not installed, return error JSON and exit
    print(json.dumps({
        "success": False,
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)

# Logger will be configured by setup_logging() called within run_handler.
logger = logging.getLogger("SaneparHandler") # Placeholder, will be reconfigured

def setup_logging(log_output_dir: str, company_id: str, handler_name: str = "SaneparHandler") -> logging.Logger:
    """Configures and returns a logger for the handler."""
    current_logger = logging.getLogger(handler_name)
    current_logger.handlers.clear() # Remove any existing handlers
    current_logger.propagate = False # Prevent duplicate logs in parent loggers

    log_dir_path = os.path.join(log_output_dir, "logs")
    os.makedirs(log_dir_path, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = f"dynamic_handler_{handler_name.lower()}_{company_id}_{timestamp}.log"
    log_file_path = os.path.join(log_dir_path, log_file_name)

    # Create file handler
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Create stream handler (for console output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') # Simpler format for console
    stream_handler.setFormatter(stream_formatter)

    current_logger.addHandler(file_handler)
    current_logger.addHandler(stream_handler)
    current_logger.setLevel(logging.INFO) # Default to INFO, can be made configurable

    current_logger.info(f"Logging initialized. Log file: {log_file_path}")
    return current_logger

# --- Configuration ---
ARCHIVE_SUBDIR_NAME = "archives" # Name of the archive subdirectory within output_dir
PROCESSED_FILE_NAME = "processed_sanepar.json" # Name of the state file within output_dir
BASE_URL = "https://licitacoes.sanepar.com.br/"
# --- Processed State Management ---
def load_processed(processed_file_path: str) -> Set[str]:
    """Loads the set of processed item identifiers from the state file."""
    if not os.path.exists(processed_file_path):
        try:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
            with open(processed_file_path, 'w', encoding='utf-8') as f:
                json.dump([], f)
            logger.info(f"Initialized new processed items file: {processed_file_path}")
        except OSError as e:
            logger.error(f"OSError creating processed items file {processed_file_path}: {e}")
            return set() # Return empty if directory creation fails
        except Exception as e:
            logger.error(f"Failed to create or initialize processed items file {processed_file_path}: {e}")
            return set() # Return empty if creation fails
        return set()
    try:
        with open(processed_file_path, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except FileNotFoundError:
        logger.warning(f"Processed items file not found {processed_file_path}, treating as empty. Will create on save.")
        return set()
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from processed items file {processed_file_path}: {e}. Treating as empty.")
        # Optionally, attempt to re-initialize or backup the corrupted file here
        return set()
    except Exception as e:
        logger.error(f"Unexpected error loading processed items file {processed_file_path}: {e}")
        return set()

def save_processed(processed_set: Set[str], processed_file_path: str):
    """Saves the set of processed item identifiers to the state file."""
    try:
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(processed_set)), f, indent=2)
        logger.debug(f"Saved {len(processed_set)} items to processed file: {processed_file_path}")
    except Exception as e:
        logger.error(f"Failed to save processed items to file {processed_file_path}: {e}")

# --- Helper Functions ---
def setup_output_dirs(base_dir):
    """Ensure all necessary output directories exist relative to the base download dir."""
    base_dir_abs = os.path.abspath(base_dir)
    logger.info(f"Setting up directories relative to: {base_dir_abs}")

    output_dirs = {
        'pdf': os.path.join(base_dir_abs, 'pdfs'),
        'archive': os.path.join(base_dir_abs, ARCHIVE_SUBDIR_NAME),
        'debug': os.path.join(base_dir_abs, 'debug'),
        'logs': os.path.join(base_dir_abs, 'logs'), # Log directory within the main output_dir
        'screenshots': os.path.join(base_dir_abs, 'debug', 'screenshots'),
        'temp': os.path.join(base_dir_abs, 'temp')
    }

    for key, dir_path in output_dirs.items():
        try:
            os.makedirs(dir_path, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create directory {dir_path}: {e}")
            raise

    return output_dirs

def clean_filename(filename: Optional[str], max_length: int = 100) -> str:
    """Clean a filename to make it safe for the filesystem."""
    if not filename:
        return f"unknown_file_{int(time.time())}"
    # Remove path components
    filename = os.path.basename(filename)
    # Replace problematic characters
    filename = re.sub(r'[\\/*?:"<>|\n\r]', '_', filename)
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
def create_zip_archive(file_paths: List[str], output_zip_path: str) -> bool:
    """Create a ZIP archive containing multiple files."""
    if not file_paths:
        logger.warning(f"No files provided to create zip: {output_zip_path}")
        return False

    try:
        logger.info(f"Creating ZIP archive: {output_zip_path} with {len(file_paths)} file(s)")
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                if os.path.exists(file_path):
                    zipf.write(file_path, os.path.basename(file_path))
                    logger.debug(f"Added {os.path.basename(file_path)} to {os.path.basename(output_zip_path)}")
                else:
                    logger.warning(f"File not found, cannot add to zip: {file_path}")
        logger.info(f"Successfully created ZIP archive: {output_zip_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating ZIP archive {output_zip_path}: {e}", exc_info=True)
        if os.path.exists(output_zip_path):
            try:
                os.remove(output_zip_path)
            except Exception as rm_e:
                logger.error(f"Failed to remove partial zip {output_zip_path}: {rm_e}")
        return False

async def generate_lot_pdf(page: Page, context: BrowserContext, pdf_path: str, 
                          logger: logging.Logger, keywords: Optional[List[str]] = None) -> bool:
    """Extracts Lot data and generates a PDF report."""
    logger.info("Extracting Lot data for PDF report...")
    lot_data = {}
    current_lot = "Desconhecido"

    try:
        lotes_table_locator = page.locator('#ItensDoProcesso')
        all_rows = await lotes_table_locator.locator('tr').all()

        # Get the process number from the page if available for the logs
        processo_num = "unknown_process"
        try:
            processo_title = await page.locator('title').text_content()
            process_match = re.search(r'Processo\s*(\d+)', processo_title)
            if process_match:
                processo_num = process_match.group(1)
        except:
            pass

        for row in all_rows:
            # Check for Lot Header Row
            is_lot_header = await row.locator('td.cabecalho_gridView[colspan="4"]').count() > 0
            if is_lot_header:
                lot_text = await row.locator('td').first.text_content() or ""
                match = re.search(r'LOTE\s*(\d+)', lot_text, re.IGNORECASE)
                current_lot = f"Lote {match.group(1)}" if match else lot_text.strip()
                lot_data[current_lot] = []
                logger.debug(f"PDF Gen - Found header for: {current_lot}")
                continue

            # Check if it's an Item Row (excluding the small font sub-rows)
            is_data_row = await row.locator('td').count() == 4 # Item rows have 4 cells
            is_small_font = "font-size:8px" in (await row.get_attribute("style") or "")
            if is_data_row and not is_small_font:
                cells = await row.locator('td').all()
                if len(cells) == 4:
                    item_num = (await cells[0].text_content() or "").strip()
                    descricao = (await cells[1].text_content() or "").strip()
                    qtde = (await cells[2].text_content() or "").strip()
                    objeto_text = qtde  # Just use qtde for filtering if needed
                    un = (await cells[3].text_content() or "").strip()

                    # Keyword Filtering
                    if keywords and qtde: # Only filter if keywords are provided
                        objeto_text_lower = objeto_text.lower()
                        if not any(keyword.lower() in objeto_text_lower for keyword in keywords):
                            logger.debug(f"Processo {processo_num} | Objeto '{objeto_text[:100]}...' does not match keywords. Skipping.")
                            continue

                    if item_num: # Only add if item number is present
                        lot_data.setdefault(current_lot, []).append({
                            "Item": item_num,
                            "Descrição": descricao,
                            "Qtde": qtde,
                            "UN": un
                        })
                        logger.debug(f"PDF Gen - Item Data for Lot '{current_lot}': {item_num} - {descricao} - {qtde} - {un}")

        if not lot_data or all(not items for items in lot_data.values()):
            logger.warning("PDF Gen - No Lot data found or extracted.")
            return False

        # Generate HTML for PDF
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Relatório de Lotes</title>
            <style>
                body { font-family: sans-serif; font-size: 10px; }
                table { width: 100%; border-collapse: collapse; margin-bottom: 15px; }
                th, td { border: 1px solid #ddd; padding: 4px; text-align: left; }
                th { background-color: #f2f2f2; font-weight: bold; }
                .lot-header { background-color: #e0e0e0; font-weight: bold; text-align: center; }
                .item-num { text-align: right; width: 5%; }
                .qtde { text-align: right; width: 10%; }
                .un { text-align: center; width: 5%; }
            </style>
        </head>
        <body>
            <h1>Relatório de Itens por Lote</h1>
        """
        for lot_name, items in lot_data.items():
            if not items: continue # Skip lots with no items
            html_content += f"<h2>{lot_name}</h2>\n"
            html_content += """
                <table>
                    <thead>
                        <tr>
                            <th class="item-num">Item</th>
                            <th>Descrição</th>
                            <th class="qtde">Qtde</th>
                            <th class="un">UN</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for item in items:
                html_content += f"""
                        <tr>
                            <td class="item-num">{item['Item']}</td>
                            <td>{item['Descrição']}</td>
                            <td class="qtde">{item['Qtde']}</td>
                            <td class="un">{item['UN']}</td>
                        </tr>
                """
            html_content += "</tbody></table>\n"

        html_content += "</body></html>"

        # Generate PDF using Playwright
        temp_pdf_page = None
        try:
            logger.info(f"PDF Gen - Generating Lot PDF at: {pdf_path}")
            temp_pdf_page = await context.new_page()
            await temp_pdf_page.set_content(html_content, wait_until="domcontentloaded")
            await temp_pdf_page.pdf(path=pdf_path, format='A4',
                                    margin={'top': '20px', 'bottom': '20px', 'left': '20px', 'right': '20px'})
            logger.info(f"PDF Gen - Successfully generated Lot PDF: {pdf_path}")
            return True
        except Exception as pdf_err:
            logger.error(f"Failed to generate Lot PDF: {pdf_err}", exc_info=True)
            return False
        finally:
            if temp_pdf_page:
                await temp_pdf_page.close()

    except Exception as e:
        logger.error(f"PDF Gen - Error generating Lot PDF: {e}", exc_info=True)
        return False

async def process_detail_page(page: Page, context: BrowserContext, tender_data_from_search: Dict, 
                              output_dirs: Dict[str, str], keywords: List[str]) -> Optional[Dict]:
    """Handles downloading attachments and generating Lot PDF for a specific process."""
    processo_num_id = tender_data_from_search.get('processo_num_id', 'unknown_id')
    processo_num_clean = tender_data_from_search.get('processo_num_clean', 'unknown_process')
    objeto_clean = tender_data_from_search.get('objeto_clean', 'unknown_objeto')

    logger.info(f"--- Processing Detail Page for Process: {processo_num_clean} (ID: {processo_num_id}) ---")
    
    processed_tender_data = tender_data_from_search.copy()
    processed_tender_data.update({
        "status": "Failed", # Default to Failed, update on success
        "processing_timestamp_detail_start": datetime.now().isoformat(),
        "downloaded_files_paths": [],
        "zip_archive_path": None,
        "lot_pdf_path": None,
        "error_message_detail": None
    })

    temp_process_dir = os.path.join(output_dirs['temp'], processo_num_clean) # Use cleaned name for temp dir
    os.makedirs(temp_process_dir, exist_ok=True)

    try:
        await page.wait_for_load_state("networkidle", timeout=60000)
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_1_initial.png"))

        # --- Step 1: Download Attachments ---
        logger.info("Extracting attachment information...")
        cookies_list = await context.cookies()
        referer_url = page.url
        action_url = page.url # Form posts back to itself

        # Extract ASP.NET state fields carefully
        try:
            viewstate = await page.locator('#__VIEWSTATE').input_value(timeout=10000)
            viewstategenerator = await page.locator('#__VIEWSTATEGENERATOR').input_value(timeout=5000)
            eventvalidation = await page.locator('#__EVENTVALIDATION').input_value(timeout=5000)
        except Exception as state_err:
            logger.error(f"Could not extract ASP.NET state for {processo_num_clean}: {state_err}")
            processed_tender_data['error_message_detail'] = f"Could not extract ASP.NET state: {state_err}"
            return processed_tender_data

        # Extract document IDs and labels
        documents_info = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('#wrdb_anexos input[type="radio"]')).map(radio => ({
                id: radio.value,
                label: radio.nextElementSibling ? radio.nextElementSibling.textContent.trim() : radio.id
            }));
        }""")
        if not documents_info:
            logger.warning(f"No attachments found for {processo_num_clean} or data extraction failed.")
            logger.warning(f"No attachment radio buttons found for process {processo_num_clean}.")
        else:
            logger.info(f"Found {len(documents_info)} attachments to download for process {processo_num_clean}.")

            # Initialize requests Session
            session = requests.Session()
            requests_cookies = {c['name']: c['value'] for c in cookies_list}
            session.cookies.update(requests_cookies)
            session.headers.update({
                'User-Agent': await page.evaluate("() => navigator.userAgent"), # Use browser's UA
                'Referer': referer_url
            })

            # Download each document
            for doc_info in documents_info:
                doc_id = doc_info['id']
                base_filename = clean_filename(doc_info['label'])

                form_data = {
                    '__EVENTTARGET': 'lbdownload',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATE': viewstate,
                    '__VIEWSTATEGENERATOR': viewstategenerator,
                    '__EVENTVALIDATION': eventvalidation,
                    'wrdb_anexos': doc_id,
                }

                logger.info(f"Requesting document: ID={doc_id}, Label={doc_info['label']}")
                try:
                    response = session.post(
                        action_url,
                        data=form_data,
                        stream=True,
                        timeout=60,
                        verify=False # Added for potential SSL issues
                    )
                    response.raise_for_status()

                    final_filename = base_filename
                    content_disp = response.headers.get('Content-Disposition')
                    if content_disp:
                        fname_match = re.search(r'filename="?([^"]+)"?', content_disp)
                        if fname_match:
                            final_filename = clean_filename(unquote(fname_match.group(1)))
                            logger.info(f"Using filename from Content-Disposition: {final_filename}")

                    if '.' not in final_filename:
                        content_type = response.headers.get('Content-Type', '').lower()
                        ext = '.pdf' if 'pdf' in content_type else \
                              '.zip' if 'zip' in content_type else \
                              '.doc' if 'doc' in content_type else \
                              '.docx' if 'officedocument' in content_type else \
                              '.dat' # Default
                        final_filename += ext
                        logger.info(f"Guessed extension '{ext}' based on Content-Type or default.")

                    target_path = os.path.join(temp_process_dir, final_filename)
                    with open(target_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192): f.write(chunk)

                    if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
                        logger.info(f"Successfully saved: {target_path}")
                        processed_tender_data['downloaded_files_paths'].append(target_path)
                    else:
                        logger.warning(f"Downloaded file is empty or missing: {target_path}")
                        if os.path.exists(target_path): os.remove(target_path)
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to download document ID {doc_id}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error downloading document ID {doc_id}: {e}", exc_info=True)

                await asyncio.sleep(0.2) # Small delay

        # --- Step 2: Generate Lot PDF ---
        lot_pdf_filename = f"Lotes_{processo_num_clean}.pdf"
        # This is the path for the PDF specific to Lotes, correctly named lot_pdf_path.
        lot_pdf_path = os.path.join(temp_process_dir, lot_pdf_filename) 
        processed_tender_data['lot_pdf_path'] = lot_pdf_path # Store potential path for Lot PDF

        if await generate_lot_pdf(page, context, lot_pdf_path, logger, keywords):
            if os.path.exists(lot_pdf_path) and os.path.getsize(lot_pdf_path) > 0:
                processed_tender_data['downloaded_files_paths'].append(lot_pdf_path)
            else:
                logger.warning(f"Lot PDF generation reported success but file is missing or empty: {lot_pdf_path}")
                processed_tender_data['lot_pdf_path'] = None # Clear if failed despite report
        else:
            logger.error(f"Failed to generate Lot PDF for {processo_num_clean}.")
            processed_tender_data['lot_pdf_path'] = None # Clear path on failure

        # --- Step 3: Create Final ZIP Archive ---
        if processed_tender_data['downloaded_files_paths']:
            # Construct filename using cleaned process number and object description from search page data
            objeto_for_zip = clean_filename(tender_data_from_search.get('objeto_clean', 'objeto'))
            zip_filename = f"SANEPAR_{processo_num_clean}_{objeto_for_zip}.zip"
            zip_filepath = os.path.join(output_dirs['archive'], zip_filename)
            
            # Attempt to create the archive
            if create_zip_archive(processed_tender_data['downloaded_files_paths'], zip_filepath):
                logger.info(f"Successfully created ZIP archive: {zip_filepath} for {processo_num_clean}")
                processed_tender_data['zip_archive_path'] = zip_filepath # Confirm path on success
            else:
                logger.error(f"Failed to create ZIP archive for {processo_num_clean} at {zip_filepath}")
                processed_tender_data['zip_archive_path'] = None # Clear path on failure
                processed_tender_data['error_message_detail'] = (processed_tender_data.get('error_message_detail') or "") + "Failed to create ZIP. "
        else:
            logger.warning(f"No files downloaded for {processo_num_clean}, skipping ZIP creation.")
            processed_tender_data['zip_archive_path'] = None # Ensure no ZIP path if no files to ZIP
    except Exception as main_exception:
        logger.error(f"Main error processing detail page for {processo_num_clean}: {main_exception}", exc_info=True)
        processed_tender_data['error_message_detail'] = (processed_tender_data.get('error_message_detail') or "") + f"Main exception: {main_exception}"
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_9_critical_error.png"))
    finally:
        # Clean up temp directory for this process
        try:
            if os.path.exists(temp_process_dir):
                shutil.rmtree(temp_process_dir)
                logger.debug(f"Cleaned up temp directory: {temp_process_dir} for {processo_num_clean}")
        except Exception as cleanup_e:
            logger.error(f"Error cleaning temp dir {temp_process_dir} for {processo_num_clean}: {cleanup_e}")

    # Determine final status
    # Success if ZIP created OR (Lot PDF generated AND no files were expected/downloaded)
    # This logic might need refinement based on what's considered a 'successful' partial processing
    if processed_tender_data['zip_archive_path'] or (processed_tender_data['lot_pdf_path'] and not processed_tender_data['downloaded_files_paths']):
        processed_tender_data['status'] = "Processed"
    else:
        processed_tender_data['status'] = "Failed"
        if not processed_tender_data['error_message_detail']:
            processed_tender_data['error_message_detail'] = "Processing completed but no archive or lot PDF was generated."

    processed_tender_data['processing_timestamp_detail_end'] = datetime.now().isoformat()
    logger.info(f"--- Finished Processing Detail Page for Process: {processo_num_clean} | Status: {processed_tender_data['status']} ---")
    return processed_tender_data

async def process_search_page(page: Page, context: BrowserContext, search_url: str, 
                              output_dirs: Dict[str, str], processed_state: Set[str], 
                              keywords: List[str]) -> Tuple[Set[str], List[Dict]]:
    """Processes the main listing page, filters by keywords, and triggers detail page processing."""
    newly_processed_in_this_run: Set[str] = set()
    processed_tenders_on_page: List[Dict] = []
    try:
        logger.info(f"Navigating to search results page: {search_url}")
        await page.goto(search_url, wait_until="networkidle", timeout=60000)
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_page_initial.png"))

        # --- Locate Table and Rows ---
        table_selector = '#GridView1' # Main table for upcoming openings
        # Combine selectors for alternating row styles
        row_selector = f'{table_selector} tr.tabPar, {table_selector} tr.tabImpar'
        logger.debug(f"Using row selector: {row_selector}")

        try:
            await page.locator(table_selector).wait_for(state="visible", timeout=30000)
            # Add a small static wait for rows to render after table is visible
            await page.wait_for_timeout(2000)
            rows = await page.locator(row_selector).all()
            logger.info(f"Found {len(rows)} potential process rows.")
        except Exception as e:
            logger.error(f"Could not find table or rows on {search_url}: {e}")
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_fail_table.png"))
            return newly_processed_in_this_run, processed_tenders_on_page # Cannot proceed

        if not rows:
            logger.info(f"No process rows found on {search_url}.")
            return newly_processed_in_this_run, processed_tenders_on_page

        # --- Extract Data and Filter ---
        rows_to_process: List[Dict] = []
        for i, row_locator in enumerate(rows):
            try:
                cells = await row_locator.locator('td').all()
                if len(cells) < 4: # Expecting 4 columns: Processo, Objeto, Publicação, Abertura
                    logger.warning(f"Row {i} has fewer than 4 cells, skipping.")
                    continue

                objeto_text = await cells[1].text_content(timeout=5000) or ""
                processo_link_locator = cells[0].locator('a').first # Link is in the first cell

                if not await processo_link_locator.count():
                    logger.warning(f"Row {i}: Could not find link in 'Processo' cell.")
                    continue

                href = await processo_link_locator.get_attribute('href')
                if not href:
                    logger.warning(f"Row {i}: Link in 'Processo' cell has no href.")
                    continue

                # Extract numpro ID
                match = re.search(r'numpro=(\d+)', href)
                if not match:
                    logger.warning(f"Row {i}: Could not extract 'numpro' ID from href: {href}")
                    continue
                processo_num_id = match.group(1)

                # Keyword Filtering
                # objeto_text was extracted from cells[1] earlier
                should_process_item = True
                if keywords: # Only filter if keywords are provided
                    objeto_text_lower = objeto_text.lower()
                    if not any(keyword.lower() in objeto_text_lower for keyword in keywords):
                        logger.debug(f"Row {i} | Processo ID {processo_num_id} | Objeto '{objeto_text[:70]}...' does not match keywords. Skipping.")
                        should_process_item = False
                    if should_process_item:
                        # If item passed keyword filter (or no keywords), proceed with state check and adding to rows_to_process
                        processo_num_text = await processo_link_locator.text_content() or f"UnknownProcess_{processo_num_id}"
                        processo_num_clean = clean_filename(processo_num_text.strip())
                    # Use the already extracted objeto_text for cleaning, ensure it's the full description
                    objeto_clean = clean_filename(objeto_text.strip(), max_length=150) # Increased max_length for more context
                    
                    # State Check
                    if processo_num_id in processed_state:
                        logger.info(f"Row {i}: Process {processo_num_clean} (ID: {processo_num_id}) already processed (from state file). Skipping.")
                        continue # Skip to next row in the main loop
                    if processo_num_id in newly_processed_in_this_run:
                        logger.info(f"Row {i}: Process {processo_num_clean} (ID: {processo_num_id}) already processed (this run). Skipping.")
                        continue # Skip to next row in the main loop
                    
                    # Attempt to get Publicação and Abertura dates from cells[2] and cells[3]
                    # Assuming table structure: Processo (0), Objeto (1), Publicação (2), Abertura (3)
                    publication_date_str = (await cells[2].text_content() or "").strip() if len(cells) > 2 else "N/A"
                    opening_date_str = (await cells[3].text_content() or "").strip() if len(cells) > 3 else "N/A"

                    detail_page_url = urljoin(BASE_URL, href)
                    logger.info(f"Row {i}: Adding to processing queue. Process: {processo_num_clean} (ID: {processo_num_id}), Objeto: '{objeto_text[:70]}...'. Link: {detail_page_url}")
                    rows_to_process.append({
                        "detail_page_url": detail_page_url,
                        "processo_num_id": processo_num_id,
                        "processo_num_clean": processo_num_clean,
                        "objeto_clean": objeto_clean, 
                        "original_objeto": objeto_text, 
                        "publication_date_str": publication_date_str,
                        "opening_date_str": opening_date_str,
                        "link_row_text": processo_num_text,
                        "source_url": detail_page_url  # Store source URL for the requirement
                    })

            except Exception as row_err:
                logger.error(f"Error extracting data from row {i}: {row_err}", exc_info=True)

        # --- Process Filtered Rows ---
        logger.info(f"Processing details for {len(rows_to_process)} new processes...")
        for data in rows_to_process:
            processo_num_id = data['processo_num_id']
            detail_url = data['detail_page_url']
            objeto = data['objeto_clean']
            processo_num_clean = data['processo_num_clean']

            detail_page = None # Use a new page for isolation? Or reuse? Reuse for session state.
            try:
                logger.info(f"Navigating to detail page for process {processo_num_clean}: {detail_url}")
                # Reuse the existing page to maintain session/cookies
                await page.goto(detail_url, wait_until="networkidle", timeout=90000)

                detailed_tender_info = await process_detail_page(
                    page, context, data, output_dirs, keywords
                )

                if detailed_tender_info and detailed_tender_info.get('status') == "Processed":
                    logger.info(f"Successfully processed detail page for {detailed_tender_info.get('processo_num_clean', processo_num_clean)}")
                    newly_processed_in_this_run.add(detailed_tender_info.get('processo_num_id', processo_num_id))
                    processed_tenders_on_page.append(detailed_tender_info) # Add to the list
                elif detailed_tender_info: # It ran, but status might be 'Failed'
                    logger.warning(f"Failed or partially processed detail page for {detailed_tender_info.get('processo_num_clean', processo_num_clean)}: {detailed_tender_info.get('error_message_detail', 'Unknown error')}")
                    # Add to processed to avoid retrying immediately
                    newly_processed_in_this_run.add(detailed_tender_info.get('processo_num_id', processo_num_id))
                else:
                    logger.error(f"Critical failure in process_detail_page for {processo_num_clean}, no data returned.")
                    # Add to processed to avoid retrying this problematic page
                    newly_processed_in_this_run.add(processo_num_id)

                # Navigate back robustly
                logger.info(f"Navigating back to search results from {processo_num_clean}...")
                try:
                    await page.go_back(wait_until="networkidle", timeout=60000)
                    # Verify we are back by checking for the table
                    await page.locator(table_selector).wait_for(state="visible", timeout=30000)
                    await page.wait_for_timeout(1000) # Small wait after table visible
                    logger.debug("Successfully navigated back to search results.")
                except Exception as back_err:
                    logger.error(f"Error navigating back for {processo_num_clean}, attempting direct navigation: {back_err}")
                    await page.goto(search_url, wait_until="networkidle", timeout=60000)
                    await page.locator(table_selector).wait_for(state="visible", timeout=30000) # Re-verify

            except Exception as detail_e:
                logger.error(f"Error during detail processing/navigation for {processo_num_clean}: {detail_e}", exc_info=True)
                await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_processing_error_{processo_num_clean}.png"))
                # Attempt recovery by going back to the main search URL
                try:
                    logger.warning(f"Attempting recovery navigation to {search_url}")
                    await page.goto(search_url, wait_until="networkidle", timeout=60000)
                    await page.locator(table_selector).wait_for(state="visible", timeout=30000)
                except Exception as recovery_e:
                    logger.critical(f"Recovery navigation failed for {search_url}: {recovery_e}. Aborting search page processing.", exc_info=True)
                    break # Stop processing this search page

        logger.info(f"Finished processing search page: {search_url}")

    except PlaywrightTimeoutError as pte:
        logger.error(f"Timeout error on search page {search_url}: {pte}")
        try: await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_timeout_error.png"))
        except Exception: pass
    except Exception as e:
        logger.error(f"General error processing search page {search_url}: {e}", exc_info=True)
        try: await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_general_error.png"))
        except Exception: pass

    return newly_processed_in_this_run, processed_tenders_on_page

async def _run_handler_async(company_id: str, output_dir: str, keywords: List[str], notion_database_id: Optional[str] = None) -> Dict[str, Any]:
    """Async implementation of the handler function."""
    global logger # Use global logger
    logger = setup_logging(output_dir, company_id, handler_name="SaneparHandler")

    playwright = None; browser = None; context = None
    overall_success = False; processed_new_items_count = 0; error_message = None
    all_processed_tenders = [] # List to store standardized tender info for framework

    try:
        # Ensure output_dir is absolute
        output_dir_abs = os.path.abspath(output_dir)
        output_dirs = setup_output_dirs(output_dir_abs)
        
        processed_file_path = os.path.join(output_dir_abs, PROCESSED_FILE_NAME)
        processed_state = load_processed(processed_file_path)

        playwright = await async_playwright().start()
        
        # Always run non-headless for this ASP.NET site initially for easier debugging
        headless = False
        logger.info(f"Starting browser with headless={headless}")
        browser = await playwright.chromium.launch(
            headless=headless,
            args=["--disable-web-security", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            ignore_https_errors=True,
        )
        # Set longer default timeouts
        context.set_default_timeout(60000) # 60 seconds for actions
        page = await context.new_page()

        # Determine the search URL
        search_url_to_use = urljoin(BASE_URL, "SLI11000.aspx")
        logger.info(f"Navigating to Sanepar search page: {search_url_to_use} with keywords: {keywords}")

        # Process the search page for relevant items
        newly_processed_ids, detailed_tenders = await process_search_page(
            page, context, search_url_to_use, output_dirs, processed_state, keywords
        )
        
        # Convert the detailed tender data to the format expected by the framework
        for tender in detailed_tenders:
            if tender.get('status') == "Processed" and tender.get('zip_archive_path'):
                # Map our internal structure to the framework's expected format
                standardized_tender = {
                    "tender_id": tender.get('processo_num_id') or tender.get('processo_num_clean', 'unknown'),
                    "title": tender.get('original_objeto', 'Unknown Object'),
                    "downloaded_zip_path": tender.get('zip_archive_path', ''),
                    "source_url": tender.get('source_url', 'https://licitacoes.sanepar.com.br/'),
                }
                all_processed_tenders.append(standardized_tender)

        processed_new_items_count = len(newly_processed_ids)
        if newly_processed_ids:
            updated_state = processed_state.union(newly_processed_ids)
            save_processed(updated_state, processed_file_path)
            overall_success = True # Considered success if we processed *something* new
        elif not newly_processed_ids and processed_state:
             logger.info(f"No new processes matching keywords found on {search_url_to_use} in this run.")
             overall_success = True # Still success, just nothing new to do
        else:
             logger.warning(f"No processes found or processed on {search_url_to_use}.")
             # If the initial page load failed inside process_search_page, overall_success might be false
             overall_success = False # Mark as fail if nothing was processed AND state is empty
             error_message = "Failed to find or process any relevant rows on the search page."

    except Exception as e:
        logger.critical(f"Critical error in run_handler for {company_id}: {e}", exc_info=True)
        overall_success = False
        error_message = f"Critical error: {e}"
    finally:
        # Graceful cleanup
        if context:
            try: await context.close()
            except Exception as e: logger.error(f"Error closing browser context: {e}")
        if browser:
            try: await browser.close()
            except Exception as e: logger.error(f"Error closing browser: {e}")
        if playwright:
            try: await playwright.stop()
            except Exception as e: logger.error(f"Error stopping Playwright: {e}")
        logger.info("Playwright resources cleaned up.")

    # Final result structure for the dispatcher in the expected format
    return {
        "success": overall_success,
        "company_id": company_id,
        "new_tenders_processed": all_processed_tenders,
        "error_message": error_message
    }

def run_handler(company_id: str, output_dir: str, keywords: List[str], notion_database_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Main entry point for the handler. Runs the async handler in an event loop.
    
    Args:
        company_id: Identifier for the company
        output_dir: Base directory for all outputs
        keywords: List of keywords to filter tenders
        notion_database_id: Optional Notion database ID for context
        
    Returns:
        Dictionary with standardized result structure
    """
    # Run the async handler in an event loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # Create a new event loop if one doesn't exist
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        return loop.run_until_complete(_run_handler_async(company_id, output_dir, keywords, notion_database_id))
    except Exception as e:
        # If something goes wrong, log it and return a standardized error response
        error_msg = f"Error running handler for {company_id}: {str(e)}"
        if logger:
            logger.critical(error_msg, exc_info=True)
        else:
            print(error_msg, file=sys.stderr)
        
        return {
            "success": False,
            "company_id": company_id,
            "new_tenders_processed": [],
            "error_message": error_msg
        }


if __name__ == "__main__":
    # For direct CLI testing
    import argparse
    parser = argparse.ArgumentParser(description="Sanepar Handler CLI")
    parser.add_argument("--company-id", default="SANEPAR", help="Company ID")
    parser.add_argument("--output-dir", required=True, help="Base output directory")
    parser.add_argument("--keywords", nargs="+", default=["tubo", "PEAD", "PAM"], help="Keywords to filter tenders")
    parser.add_argument("--notion-db-id", help="Notion Database ID (optional)")
    
    args = parser.parse_args()
    
    result = run_handler(
        company_id=args.company_id,
        output_dir=args.output_dir,
        keywords=args.keywords,
        notion_database_id=args.notion_db_id
    )
    
    print(json.dumps(result, indent=2)) 


    