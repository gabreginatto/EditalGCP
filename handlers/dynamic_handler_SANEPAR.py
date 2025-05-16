#!/usr/bin/env python3
"""
Sanepar Download Handler for Procurement Download System

Handles downloading documents from the Sanepar procurement portal (licitacoes.sanepar.com.br).

Workflow:
1. Navigates to the main listing page (SLI11000.aspx).
2. Filters processes based on keywords in the 'Objeto' column.
3. Avoids reprocessing using a state file (processed_sanepar.json).
4. For matching processes, navigates to the detail page (SLI11100.aspx).
5. Downloads attachments by simulating form POST requests.
6. Extracts 'Lote' table data and generates a separate PDF.
7. Creates a ZIP archive containing all downloaded files for each process.

Usage:
    python dynamic_handler_sanepar.py --url URL --output-dir DIR [--timeout SECONDS]
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

# --- Logging Configuration ---
# Use absolute path for downloads base directory
script_dir = os.path.dirname(os.path.abspath(__file__))
# Navigate up two levels from handlers/ to the main project dir, then into downloads
base_download_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'downloads'))

log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = f"dynamic_handler_sanepar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    level=logging.INFO, # Changed to INFO for production, DEBUG for development
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SaneparHandler")

# --- Configuration ---
KEYWORDS = ["tubo", "polietileno", "PEAD", "polimero", "PAM", "hidrômetro", "medidor"]
PROCESSED_FILE = "/Users/gabrielreginatto/Desktop/Code/DownloadEditalAnalise/downloads/processed_sanepar.json"
ARCHIVE_DIR = os.path.join(base_download_dir, "archives")
BASE_URL = "https://licitacoes.sanepar.com.br/"

# --- Processed State Management ---
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
            json.dump([], f)
        return set()
    try:
        with open(PROCESSED_FILE, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_processed(processed_set):
    with open(PROCESSED_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted(list(processed_set)), f, indent=2)

# --- Helper Functions ---

def setup_output_dirs(base_dir):
    """Ensure all necessary output directories exist relative to the base download dir."""
    base_dir_abs = os.path.abspath(base_dir)
    logger.info(f"Setting up directories relative to: {base_dir_abs}")

    output_dirs = {
        'pdf': os.path.join(base_dir_abs, 'pdfs'),
        'archive': os.path.join(base_dir_abs, 'archives'),
        'debug': os.path.join(base_dir_abs, 'debug'),
        'logs': log_dir, # Use pre-defined absolute log_dir
        'screenshots': os.path.join(base_dir_abs, 'debug', 'screenshots'),
        'temp': os.path.join(base_dir_abs, 'temp'),
        'state': base_dir_abs # For the state file
    }

    for key, dir_path in output_dirs.items():
        if key != 'state': # Don't create the state file itself as a directory
            try:
                os.makedirs(dir_path, exist_ok=True)
                # logger.debug(f"Directory ensured: {dir_path}")
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

async def generate_lot_pdf(page: Page, context: BrowserContext, target_pdf_path: str):
    """Extracts Lot data and generates a PDF report."""
    logger.info("Extracting Lot data...")
    lot_data = {}
    current_lot = "Desconhecido"

    try:
        lotes_table_locator = page.locator('#ItensDoProcesso')
        all_rows = await lotes_table_locator.locator('tr').all()

        for row in all_rows:
            # Check for Lot Header Row
            is_lot_header = await row.locator('td.cabecalho_gridView[colspan="4"]').count() > 0
            if is_lot_header:
                lot_text = await row.locator('td').first.text_content() or ""
                match = re.search(r'LOTE\s*(\d+)', lot_text, re.IGNORECASE)
                current_lot = f"Lote {match.group(1)}" if match else lot_text.strip()
                lot_data[current_lot] = []
                logger.debug(f"Found header for: {current_lot}")
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
                    un = (await cells[3].text_content() or "").strip()
                    if item_num: # Only add if item number is present
                         lot_data.setdefault(current_lot, []).append({
                            "Item": item_num,
                            "Descrição": descricao,
                            "Qtde": qtde,
                            "UN": un
                         })
                         # logger.debug(f" Extracted Item: {item_num} for {current_lot}")


        if not lot_data or all(not items for items in lot_data.values()):
            logger.warning("No valid Lot data extracted.")
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
            logger.info(f"Generating Lot PDF: {target_pdf_path}")
            temp_pdf_page = await context.new_page()
            await temp_pdf_page.set_content(html_content, wait_until="domcontentloaded")
            await temp_pdf_page.pdf(path=target_pdf_path, format='A4',
                                    margin={'top': '20px', 'bottom': '20px', 'left': '20px', 'right': '20px'})
            logger.info(f"Successfully generated Lot PDF: {target_pdf_path}")
            return True
        except Exception as pdf_err:
            logger.error(f"Failed to generate Lot PDF: {pdf_err}", exc_info=True)
            return False
        finally:
            if temp_pdf_page:
                await temp_pdf_page.close()

    except Exception as e:
        logger.error(f"Error extracting Lot data: {e}", exc_info=True)
        return False


async def process_detail_page(page: Page, context: BrowserContext, objeto_clean: str, processo_num_clean: str, processo_num_id: str, output_dirs: Dict[str, str]) -> bool:
    """Handles downloading attachments and generating Lot PDF for a specific process."""
    logger.info(f"--- Processing Detail Page for Process: {processo_num_clean} (ID: {processo_num_id}) ---")
    temp_process_dir = os.path.join(output_dirs['temp'], processo_num_clean)
    os.makedirs(temp_process_dir, exist_ok=True)
    downloaded_file_paths = []
    final_zip_success = False

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
            logger.error(f"Failed to extract ASP.NET state fields: {state_err}")
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_2_state_error.png"))
            return False # Cannot proceed without state

        # Extract document IDs and labels
        documents_info = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('#wrdb_anexos input[type="radio"]')).map(radio => ({
                id: radio.value,
                label: radio.nextElementSibling ? radio.nextElementSibling.textContent.trim() : radio.id
            }));
        }""")

        if not documents_info:
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
                    # Include other relevant form fields if necessary
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
                        downloaded_file_paths.append(target_path)
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
        lot_pdf_path = os.path.join(temp_process_dir, lot_pdf_filename)
        if await generate_lot_pdf(page, context, lot_pdf_path):
            if os.path.exists(lot_pdf_path) and os.path.getsize(lot_pdf_path) > 0:
                 downloaded_file_paths.append(lot_pdf_path)
            else:
                 logger.warning(f"Lot PDF generation reported success but file is missing or empty: {lot_pdf_path}")
        else:
            logger.error("Failed to generate Lot PDF.")

        # --- Step 3: Create Final ZIP Archive ---
        if downloaded_file_paths:
            zip_filename = f"SANEPAR_{processo_num_clean}.zip"
            zip_filepath = os.path.join(output_dirs['archive'], zip_filename)
            if create_zip_archive(downloaded_file_paths, zip_filepath):
                final_zip_success = True
            else:
                logger.error(f"Failed to create final ZIP for {processo_num_clean}")
        else:
            logger.warning(f"No files were downloaded or generated for {processo_num_clean}, no ZIP created.")
            # Consider success=True if the goal was just to check and there were no files?
            # For now, require at least one file downloaded/generated for overall success.
            final_zip_success = False

    except Exception as e:
        logger.error(f"Critical error processing detail page for {processo_num_clean}: {e}", exc_info=True)
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"detail_{processo_num_clean}_9_critical_error.png"))
        final_zip_success = False
    finally:
        # Clean up temp directory for this process
        try:
            if os.path.exists(temp_process_dir):
                shutil.rmtree(temp_process_dir)
                logger.debug(f"Cleaned up temp directory: {temp_process_dir}")
        except Exception as cleanup_e:
            logger.error(f"Error cleaning temp dir {temp_process_dir}: {cleanup_e}")

    logger.info(f"--- Finished Processing Detail Page for Process: {processo_num_clean} | Success: {final_zip_success} ---")
    return final_zip_success


async def process_search_page(page: Page, context: BrowserContext, search_url: str, output_dirs: Dict[str, str], processed_state: Set[str]) -> Set[str]:
    """Processes the main listing page, filters by keywords, and triggers detail page processing."""
    newly_processed_in_this_run = set()
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
            return newly_processed_in_this_run # Cannot proceed

        if not rows:
            logger.info(f"No process rows found on {search_url}.")
            return newly_processed_in_this_run

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

                # Keyword Check
                for keyword in KEYWORDS:
                    # Use regex to match whole word only, case-insensitive
                    if re.search(rf'\\b{re.escape(keyword)}\\b', objeto_text, re.IGNORECASE):
                        processo_num_text = await processo_link_locator.text_content() or f"UnknownProcess_{processo_num_id}"
                        processo_num_clean = clean_filename(processo_num_text.strip())
                        objeto_clean = clean_filename(objeto_text.strip(), max_length=60)
                        # State Check
                        if processo_num_id in processed_state:
                            logger.info(f"Row {i}: Process {processo_num_clean} (ID: {processo_num_id}) already processed (from state file). Skipping.")
                            continue
                        if processo_num_id in newly_processed_in_this_run:
                            logger.info(f"Row {i}: Process {processo_num_clean} (ID: {processo_num_id}) already processed (this run). Skipping.")
                            continue
                        logger.info(f"Row {i}: Keyword match! Process: {processo_num_clean} (ID: {processo_num_id}), Objeto: '{objeto_text[:50]}...', Triggered by keyword: '{keyword}'")
                        rows_to_process.append({
                            "detail_page_url": urljoin(BASE_URL, href),
                            "processo_num_id": processo_num_id,
                            "processo_num_clean": processo_num_clean,
                            "objeto_clean": objeto_clean
                        })
                        break  # Only queue once per process, even if multiple keywords match

             except Exception as row_err:
                 logger.error(f"Error extracting data from row {i}: {row_err}", exc_info=True)

        # --- Process Filtered Rows ---
        logger.info(f"Processing details for {len(rows_to_process)} new processes matching keywords...")
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

                success = await process_detail_page(
                    page, context, objeto, processo_num_clean, processo_num_id, output_dirs
                )

                if success:
                    logger.info(f"Successfully processed detail page for {processo_num_clean}")
                    newly_processed_in_this_run.add(processo_num_id)
                else:
                    logger.warning(f"Failed processing detail page for {processo_num_clean}")

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

    return newly_processed_in_this_run

async def handle_sanepar_download(url, output_dir, timeout=300):
    """Main handler function for Sanepar downloads."""
    playwright = None; browser = None; context = None
    overall_success = False; processed_new_items_count = 0; error_message = None
    try:
        output_dirs = setup_output_dirs(output_dir)
        processed_state = load_processed()

        playwright = await async_playwright().start()
        # Always run non-headless for this ASP.NET site initially, easier debugging
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
            # No downloads expected via Playwright event, relying on requests
        )
        # Set longer default timeouts
        context.set_default_timeout(60000) # 60 seconds for actions
        context.set_default_navigation_timeout(90000) # 90 seconds for navigations

        page = await context.new_page()

        # Process the main search/listing page
        newly_processed = await process_search_page(page, context, url, output_dirs, processed_state)

        processed_new_items_count = len(newly_processed)
        if newly_processed:
            updated_state = processed_state.union(newly_processed)
            save_processed(updated_state)
            overall_success = True # Considered success if we processed *something* new
        elif not newly_processed and processed_state:
             logger.info(f"No new processes matching keywords found on {url} in this run.")
             overall_success = True # Still success, just nothing new to do
        else:
             logger.warning(f"No processes found or processed on {url}.")
             # If the initial page load failed inside process_search_page, overall_success might be false
             overall_success = False # Mark as fail if nothing was processed AND state is empty? debatable
             error_message = "Failed to find or process any relevant rows on the search page."

    except Exception as e:
        logger.critical(f"Critical error in handle_sanepar_download for {url}: {e}", exc_info=True)
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

    # Result for the orchestrator
    result = {
        "success": overall_success,
        "url": url,
        "file_path": None, # We output ZIPs, the orchestrator doesn't need a single path
        "error_message": error_message,
        "processed_new_items_count": processed_new_items_count
    }
    print(json.dumps(result))
    return 0 if overall_success else 1

async def main():
    """Parses arguments and runs the handler."""
    parser = argparse.ArgumentParser(description="Sanepar Download Handler")
    parser.add_argument("--url", required=True, help="URL of the Sanepar listing page (e.g., SLI11000.aspx)")
    # Default to the calculated base_download_dir
    parser.add_argument("--output-dir", default=base_download_dir, help=f"Base output directory (defaults to: {base_download_dir})")
    parser.add_argument("--timeout", type=int, default=600, help="Overall timeout for the handler in seconds (approximate)")
    args = parser.parse_args()

    # Ensure output_dir is absolute
    output_dir_abs = os.path.abspath(args.output_dir)

    # Start the main download process
    exit_code = await handle_sanepar_download(args.url, output_dir_abs, args.timeout)
    sys.exit(exit_code)

if __name__ == "__main__":
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        # Compatibility for older Python versions if needed
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())