#!/usr/bin/env python3
"""
CESAN Download Handler for Procurement Document Processing System

This handler downloads procurement documents from the CESAN procurement portal.
It searches for procurement processes matching specified keywords and year,
downloads associated documents, and creates ZIP archives.

Interface:
    run_handler(company_id, output_dir, keywords, notion_database_id)
    Returns a standardized result dictionary with success status, processed items, etc.
"""

import os
import sys
import json
import time
import asyncio
import logging
import zipfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib.parse import urljoin
import re

# Import Playwright - Using try/except for better error handling
try:
    from playwright.async_api import async_playwright, ElementHandle, Page, BrowserContext
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logging.error("Playwright libraries not installed. Install with 'pip install playwright' and 'playwright install'")

# --- Constants and Configuration ---
HANDLER_NAME = "CESANHandler"
SEARCH_URL = "https://compras.cesan.com.br/consultarLicitacao.php"
TARGET_YEAR = "2025"  # Year filter for procurement processes
STATE_FILE_NAME = "processed_cesan_{}.json"  # Format string for state file
REQUIRED_DIRS = ["archives", "logs", "temp", "screenshots"]  # Required subdirectories

# --- Helper Functions ---
def setup_logging(log_dir: str, company_id: str) -> logging.Logger:
    """Configure logger for this handler."""
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"handler_cesan_{company_id}_{timestamp}.log"
    log_filepath = log_dir_path / log_filename
    
    # Create handler logger
    logger = logging.getLogger(f"{HANDLER_NAME}_{company_id}")
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create file handler
    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def setup_directories(output_dir: str, logger: logging.Logger) -> Dict[str, Path]:
    """Create necessary directory structure based on output_dir."""
    output_path = Path(output_dir)
    
    # Create a dictionary of required directories
    dirs = {
        'base': output_path,
        'archives': output_path / "archives",
        'logs': output_path / "logs",
        'temp': output_path / "temp",
        'screenshots': output_path / "screenshots"
    }
    
    # Ensure all directories exist
    for dir_name, dir_path in dirs.items():
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Directory created/verified: {dir_path}")
        except Exception as e:
            logger.error(f"Failed to create directory {dir_path}: {e}")
            raise
    
    return dirs

def load_processed_items(state_file_path: Path, logger: logging.Logger) -> List[str]:
    """Load the list of already processed items from state file."""
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)  # Should be a list of strings
        else:
            logger.info(f"State file not found: {state_file_path}. Starting fresh.")
            return []
    except Exception as e:
        logger.error(f"Error loading state file {state_file_path}: {e}")
        return []  # Start fresh if error reading

def save_processed_items(state_file_path: Path, items_list: List[str], logger: logging.Logger) -> bool:
    """Save the list of processed items to state file."""
    try:
        state_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(items_list, f, indent=2, ensure_ascii=False)
        logger.debug(f"Saved {len(items_list)} processed items to state file")
        return True
    except Exception as e:
        logger.error(f"Error saving state file {state_file_path}: {e}")
        return False

def sanitize_processo(processo: str) -> str:
    """Clean processo ID for use in filenames."""
    return processo.replace(' ', '-').replace('/', '-').replace('\\', '-') \
        .replace('?', '-').replace('%', '-').replace('*', '-').replace(':', '-') \
        .replace('|', '-').replace('"', '-').replace('<', '-').replace('>', '-') \
        or 'UNKNOWN_PROCESSO'

async def _run_handler_async(
    company_id: str,
    output_dirs: Dict[str, Path],
    keywords: List[str],
    state_file_path: Path,
    logger: logging.Logger
) -> Tuple[bool, List[Dict[str, str]], Optional[str]]:
    """
    Internal async function to run the Playwright-based handler.
    
    Returns:
        Tuple containing:
        - Success flag (bool)
        - List of processed tenders data
        - Error message (if any)
    """
    if not HAS_PLAYWRIGHT:
        return False, [], "Playwright libraries not installed"
    
    browser = None
    processed_list = load_processed_items(state_file_path, logger)
    processed_tenders = []
    overall_success = True
    error_message = None
    
    try:
        async with async_playwright() as playwright:
            try:
                # Initialize browser
                logger.info("Initializing browser...")
                browser = await playwright.chromium.launch(
                    headless=True,  # Set to False for debugging
                    args=['--start-maximized']
                )
                context = await browser.new_context(
                    viewport=None,  # Full viewport
                    accept_downloads=True  # Allow file downloads
                )
                page = await context.new_page()
                logger.info("Browser initialized successfully.")
                
                # Navigate to search page
                logger.info(f"Navigating to: {SEARCH_URL}")
                await page.goto(SEARCH_URL, wait_until='networkidle')
                await page.wait_for_timeout(2000)  # Allow dynamic content
                
                # Wait for main table to load
                results_table_body_selector = 'fieldset.formulario div.content3 table.rTableLicitacao tbody'
                try:
                    await page.locator(results_table_body_selector).first.wait_for(state='attached', timeout=15000)
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    await page.wait_for_timeout(3000)  # Extra buffer
                except Exception as wait_error:
                    logger.warning(f"Warning during initial wait for table: {wait_error}")
                
                # Collect matching results
                logger.info("Collecting and filtering results...")
                licitacoes_to_process = []
                results_row_selector = f"{results_table_body_selector} tr"
                
                try:
                    table_body_count = await page.locator(results_table_body_selector).count()
                    if table_body_count > 0:
                        result_rows = await page.locator(results_row_selector).element_handles()
                        logger.info(f"Found {len(result_rows)} total rows in the table.")
                        
                        for row_handle in result_rows:
                            try:
                                # Get all text content from the row
                                row_text_content = (await row_handle.text_content() or '').lower()
                                
                                # Get date from the row
                                date_element = await row_handle.query_selector('label.custom-label strong')
                                date_text = (await date_element.text_content() or '').strip() if date_element else ""
                                
                                # Check for keywords AND year
                                keyword_found = any(keyword.lower() in row_text_content for keyword in keywords)
                                year_found = TARGET_YEAR in date_text
                                
                                if keyword_found and year_found:
                                    # Determine which keywords matched
                                    matching_keywords = [kw for kw in keywords if kw.lower() in row_text_content]
                                    logger.info(f"MATCH: Keywords {matching_keywords} and year {TARGET_YEAR}: {date_text}")
                                    
                                    # Extract URL and processo info
                                    link_element = await row_handle.query_selector('a[href*="viewLicitacao.php?idLicitacao="]')
                                    if link_element:
                                        relative_url = await link_element.get_attribute('href')
                                        strong_element = await link_element.query_selector('strong')
                                        
                                        if strong_element:
                                            processo_text = (await strong_element.text_content() or '').strip()
                                        else:
                                            processo_text = 'UNKNOWN_PROCESSO_IN_LINK'
                                        
                                        # Extract processo ID using regex pattern
                                        match = re.search(r'([A-Z\sÇÃÔ]+?\s*-\s*[A-Z]+\s+\d+\/\d{4})', processo_text)
                                        
                                        if match:
                                            processo_code = match.group(0).replace(" - ", " ")
                                        elif processo_text != 'UNKNOWN_PROCESSO_IN_LINK':
                                            processo_code = processo_text
                                            logger.warning(f"Regex did not match '{processo_text}'. Using full text.")
                                        else:
                                            processo_code = 'UNKNOWN_PROCESSO_IN_LINK'
                                        
                                        # Proceed if we have valid URL and processo
                                        if relative_url and processo_code != 'UNKNOWN_PROCESSO_IN_LINK':
                                            base_url = '/'.join(SEARCH_URL.split('/')[:3])
                                            absolute_url = f"{base_url}/{relative_url}"
                                            
                                            # Extract the title from the row content (if available)
                                            title = processo_text
                                            title_element = await row_handle.query_selector('label.titulo')
                                            if title_element:
                                                title = (await title_element.text_content() or '').strip() or title
                                            
                                            licitacoes_to_process.append({
                                                'processo': processo_code,
                                                'url': absolute_url,
                                                'title': title
                                            })
                                            logger.info(f"Added to queue: {processo_code}, URL: {absolute_url}")
                            except Exception as extract_error:
                                logger.warning(f"Processo {processo} processed with download errors. Not adding to processed list yet.")
                        logger.info("Results table not found or empty. No items to process.")
                
                except Exception as e:
                    logger.error(f"Error collecting results: {e}")
                
                # Process each matching tender
                logger.info(f"Processing {len(licitacoes_to_process)} matching items...")
                newly_processed_count = 0
                newly_processed_items = []
                
                for i, licitacao in enumerate(licitacoes_to_process):
                    processo = licitacao['processo']
                    url = licitacao['url']
                    title = licitacao.get('title', processo)
                    safe_processo_number = sanitize_processo(processo)
                    licitacao_success = True
                    
                    logger.info(f"Processing {i+1}/{len(licitacoes_to_process)}: {processo}")
                    
                    # Skip already processed items
                    if processo in processed_list:
                        logger.info(f"Skipping already processed: {processo}")
                        continue
                    
                    downloaded_file_paths = []
                    zip_file_path = None
                    
                    try:
                        # Navigate to detail page
                        logger.info(f"Navigating to: {url}")
                        await page.goto(url, wait_until='networkidle')
                        await page.wait_for_timeout(3000)  # Allow page to settle
                        
                        # Take screenshot for debugging if needed
                        await page.screenshot(path=str(output_dirs['screenshots'] / f"{safe_processo_number}_details.png"))
                        
                        # Download logic (find links ending in .pdf, .zip, etc.)
                        logger.info("Searching for document download links...")
                        link_elements = []
                        
                        try:
                            # Find the 'Lista de Documentos' table
                            header_element = page.locator("strong:has-text('Lista de Documentos')").first
                            await header_element.wait_for(state='visible', timeout=10000)
                            
                            # Find the parent table of the header
                            documents_table = header_element.locator("xpath=ancestor::table[1]")
                            await documents_table.wait_for(state='attached', timeout=5000)
                            
                            # Find download links within the table
                            download_links_selector = 'a[href$=".pdf"], a[href$=".zip"], a[href$=".rar"], a[href$=".doc"], a[href$=".docx"], a[href$=".xls"], a[href$=".xlsx"]'
                            link_elements = await documents_table.locator(download_links_selector).element_handles()
                            logger.info(f"Found {len(link_elements)} download links.")
                        
                        except Exception as find_links_error:
                            logger.warning(f"Could not locate document table: {find_links_error}")
                            link_elements = []
                        
                        # Download each document
                        if link_elements:
                            processo_folder = output_dirs['temp'] / safe_processo_number
                            processo_folder.mkdir(parents=True, exist_ok=True)
                            
                            for link_index, link_element in enumerate(link_elements):
                                try:
                                    href = await link_element.get_attribute('href')
                                    link_text = (await link_element.text_content() or f"download_{link_index}").strip()
                                    
                                    if href:
                                        # Ensure it's a full URL
                                        if not href.startswith(('http://', 'https://')):
                                            current_page_url = page.url
                                            download_url = urljoin(current_page_url, href)
                                        else:
                                            download_url = href
                                        
                                        # Modify link to force download if needed
                                        try:
                                            await page.evaluate("""(args) => {
                                                const link = document.querySelector(`a[href="${args.href}"]`);
                                                if (link) {
                                                    link.removeAttribute('target');
                                                    link.download = args.filename;
                                                }
                                            }""", {"href": href, "filename": link_text})
                                        except Exception as js_err:
                                            logger.warning(f"Failed to modify link via JS: {js_err}")
                                        
                                        # Download the document
                                        logger.info(f"Downloading {link_index+1}/{len(link_elements)}: {download_url}")
                                        
                                        async with page.expect_download(timeout=120000) as download_info:
                                            try:
                                                await link_element.click(timeout=15000)
                                            except Exception as click_err:
                                                logger.warning(f"Click failed, trying navigation: {click_err}")
                                                await page.goto(download_url, wait_until='domcontentloaded')
                                        
                                        download = await download_info.value
                                        suggested_filename = download.suggested_filename or f"download_{link_index}.bin"
                                        
                                        # Create save path
                                        save_path = processo_folder / suggested_filename
                                        
                                        # Ensure unique filename
                                        counter = 1
                                        original_save_path = save_path
                                        while save_path.exists():
                                            save_path = original_save_path.with_stem(f"{original_save_path.stem}_{counter}")
                                            counter += 1
                                        
                                        await download.save_as(save_path)
                                        logger.info(f"Downloaded: {save_path}")
                                        downloaded_file_paths.append(str(save_path))
                                        
                                        # Handle zip extraction if needed
                                        if save_path.suffix.lower() == '.zip':
                                            try:
                                                with zipfile.ZipFile(save_path, 'r') as zip_ref:
                                                    zip_ref.extractall(processo_folder)
                                                logger.info(f"Extracted zip: {save_path}")
                                            except Exception as zip_err:
                                                logger.error(f"Error extracting zip: {zip_err}")
                                
                                except Exception as download_error:
                                    logger.error(f"Error downloading file: {download_error}")
                                    licitacao_success = False
                        
                        # Create ZIP archive of all downloaded files
                        if licitacao_success and downloaded_file_paths:
                            logger.info(f"Creating ZIP archive for {processo}...")
                            zip_filename = f"CESAN_{safe_processo_number}.zip"
                            zip_file_path = output_dirs['archives'] / zip_filename
                            
                            try:
                                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                                    for file_path in downloaded_file_paths:
                                        if os.path.exists(file_path):
                                            zip_file.write(file_path, os.path.basename(file_path))
                                
                                logger.info(f"Successfully created: {zip_file_path}")
                                
                                # Clean up temp files
                                for file_path in downloaded_file_paths:
                                    try:
                                        if os.path.exists(file_path):
                                            os.unlink(file_path)
                                    except Exception as delete_err:
                                        logger.warning(f"Could not remove temp file {file_path}: {delete_err}")
                                
                                # Add to processed list and tracking
                                processed_list.append(processo)
                                newly_processed_count += 1
                                newly_processed_items.append(processo)
                                
                                # Add to tenders list in the required format
                                processed_tenders.append({
                                    "tender_id": processo,
                                    "title": title,
                                    "downloaded_zip_path": str(zip_file_path.relative_to(output_dirs['base'])),
                                    "source_url": url
                                })
                                
                                logger.info(f"Successfully processed: {processo}")
                            
                            except Exception as zip_err:
                                logger.error(f"Error creating ZIP: {zip_err}")
                                licitacao_success = False
                        
                        elif not downloaded_file_paths:
                            logger.info(f"No files were downloaded for {processo}, skipping zip.")
                    
                    except Exception as process_err:
                        logger.error(f"Error processing {processo}: {process_err}")
                        licitacao_success = False
                    
                    finally:
                        # Save state after each process
                        save_processed_items(state_file_path, processed_list, logger)
                        
                        # Try to return to search page
                        try:
                            logger.info("Returning to search page...")
                            voltar_button_selector = "button.btn.blue:has-text('Voltar')"
                            await page.locator(voltar_button_selector).click(timeout=10000)
                            await page.wait_for_load_state('networkidle', timeout=20000)
                            await page.wait_for_timeout(2000)
                        except Exception as back_error:
                            logger.error(f"Error returning to search page: {back_error}")
                            # Try directly navigating back to search as fallback
                            try:
                                await page.goto(SEARCH_URL, wait_until='networkidle')
                            except Exception as nav_error:
                                logger.error(f"Failed to navigate back to search: {nav_error}")
                
                logger.info(f"Processing complete. Processed {newly_processed_count} new items.")
            
            except Exception as browser_error:
                logger.error(f"Browser processing error: {browser_error}")
                logger.error(traceback.format_exc())
                overall_success = False
                error_message = f"Browser processing error: {str(browser_error)}"
            
            finally:
                if browser:
                    logger.info("Closing browser...")
                    await browser.close()
    
    except Exception as outer_error:
        logger.error(f"Critical error: {outer_error}")
        logger.error(traceback.format_exc())
        overall_success = False
        error_message = f"Critical error: {str(outer_error)}"
    
    return overall_success, processed_tenders, error_message

def run_handler(
    company_id: str,
    output_dir: str,
    keywords: List[str],
    notion_database_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Main handler function for CESAN procurement document downloads.
    
    Args:
        company_id (str): Identifier for the company
        output_dir (str): Base directory for outputs (logs, archives, etc.)
        keywords (List[str]): List of keywords to filter tender documents
        notion_database_id (str, optional): Not used by this handler but required by interface
    
    Returns:
        Dict containing:
        - success (bool): Whether the handler ran successfully
        - company_id (str): Company identifier
        - new_tenders_processed (List[Dict]): List of processed tenders with their details
        - error_message (str, optional): Error message if any
    """
    # Set up logger
    log_dir = os.path.join(output_dir, "logs")
    logger = setup_logging(log_dir, company_id)
    
    logger.info(f"=== Starting CESAN Handler (Company ID: {company_id}) ===")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Keywords: {keywords}")
    
    # Default return structure
    result = {
        "success": False,
        "company_id": company_id,
        "new_tenders_processed": [],
        "error_message": None
    }
    
    try:
        # Validate inputs
        if not output_dir:
            result["error_message"] = "Missing required output_dir parameter"
            logger.error(result["error_message"])
            return result
        
        if not keywords or not isinstance(keywords, list):
            logger.warning("No keywords provided or invalid format. Using default keywords.")
            keywords = ['tubo', 'polietileno', 'PEAD', 'polimero', 'PAM', 'hidrômetro', 'medidor']
        
        # Set up directories
        output_dirs = setup_directories(output_dir, logger)
        
        # Set up state file
        state_file_name = STATE_FILE_NAME.format(company_id.lower())
        state_file_path = output_dirs['base'] / state_file_name
        
        # Use asyncio to run the async handler
        try:
            # Run the async handler and get results
            success, processed_tenders, error_message = asyncio.run(
                _run_handler_async(
                    company_id=company_id,
                    output_dirs=output_dirs,
                    keywords=keywords,
                    state_file_path=state_file_path,
                    logger=logger
                )
            )
            
            # Update result with the async handler output
            result["success"] = success
            result["new_tenders_processed"] = processed_tenders
            result["error_message"] = error_message
            
        except RuntimeError as e:
            # Handle case where event loop is already running
            if "Cannot run the event loop while another loop is running" in str(e):
                logger.warning("Event loop already running. Getting existing loop.")
                loop = asyncio.get_event_loop()
                success, processed_tenders, error_message = loop.run_until_complete(
                    _run_handler_async(
                        company_id=company_id,
                        output_dirs=output_dirs,
                        keywords=keywords,
                        state_file_path=state_file_path,
                        logger=logger
                    )
                )
                
                # Update result with the async handler output
                result["success"] = success
                result["new_tenders_processed"] = processed_tenders
                result["error_message"] = error_message
            else:
                raise
    
    except Exception as e:
        # Catch any other exceptions
        error_msg = f"Critical error: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        result["error_message"] = error_msg
    
    # Log final status
    if result["success"]:
        logger.info(f"=== CESAN Handler completed successfully. Processed {len(result['new_tenders_processed'])} tenders ===")
    else:
        logger.error(f"=== CESAN Handler failed. Error: {result['error_message']} ===")
    
    return result

# CLI support for direct testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='CESAN procurement downloader')
    parser.add_argument('--company-id', type=str, default='CESAN', help='Company ID for the handler')
    parser.add_argument('--output-dir', type=str, required=True, help='Base output directory')
    parser.add_argument('--keywords', nargs='+', default=[], help='Keywords to filter tenders')
    parser.add_argument('--notion-database-id', type=str, help='Notion Database ID (not used)')
    
    args = parser.parse_args()
    
    # Run the handler with CLI arguments
    result = run_handler(
        company_id=args.company_id,
        output_dir=args.output_dir,
        keywords=args.keywords,
        notion_database_id=args.notion_database_id
    )
    
    # Print result as JSON
    print(json.dumps(result, indent=2))