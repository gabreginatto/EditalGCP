#!/usr/bin/env python3
"""
COMPESA ACOMPANHAMENTO Download Handler for Procurement Download System

This handler automates the search and download of bidding processes from Compesa's portal:
- Navigates to the target URL and enters the specified search keywords
- Selects the "ACOMPANHAMENTO" tab
- Filters by 2025 dates and searches specified keywords in the "OBJETO" field
- Downloads files for matching entries
- Creates ZIP archives for each downloaded file
- Tracks processed entries to avoid duplication

Usage:
    run_handler(company_id, output_dir, keywords, notion_database_id)

Returns:
    Dict with structure:
    {
        "success": bool,
        "company_id": str,
        "new_tenders_processed": [
            {
                "tender_id": str,
                "title": str,
                "downloaded_zip_path": str,
                "source_url": str
            }
        ],
        "error_message": Optional[str]
    }
"""

import os
import sys
import logging
import asyncio
import time
import json
import traceback
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Union

# Import Playwright components
try:
    from playwright.async_api import (
        async_playwright,
        Page,
        BrowserContext,
        TimeoutError as PlaywrightTimeoutError
    )
except ImportError:
    print(json.dumps({
        "success": False,
        "company_id": "COMPESA_ACOMPANHAMENTO",
        "new_tenders_processed": [],
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)

class PolimeroSearchDownloader:
    """Handles searching for keyword biddings and downloads relevant documents from 2025 entries."""

    def __init__(self, company_id: str, output_dir: str):
        """
        Initialize the downloader with company ID and output directory.
        
        Args:
            company_id: Identifier for the company
            output_dir: Base directory for all outputs
        """
        # Setup directories
        self.company_id = company_id
        self.output_base_dir = output_dir
        self.download_dir = os.path.join(output_dir, "downloads")
        self.debug_dir = os.path.join(output_dir, "debug")
        self.logs_dir = os.path.join(output_dir, "logs")
        self.archives_dir = os.path.join(output_dir, "archives")
        self.processed_file = os.path.join(output_dir, f"processed_{company_id.lower()}.json")
        self._setup_directories()

        # Set up logging
        self._setup_logging()

        # Target URL
        self.target_url = "https://portalscl.compesa.com.br:8743/webrunstudio/form.jsp?sys=SCL&action=openform&formID=7&align=0&mode=-1&goto=-1&filter=&scrolling=yes"

        logging.info(f"PolimeroSearchDownloader initialized for {company_id} with output dir: {output_dir}")
        logging.info(f"Target URL: {self.target_url}")

    def _setup_logging(self):
        """Setup logging with both file and console handlers."""
        timestamp = int(time.time())
        os.makedirs(self.logs_dir, exist_ok=True)
        log_filename = f"{self.company_id.lower()}_{timestamp}.log"
        log_filepath = os.path.join(self.logs_dir, log_filename)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)  # Changed to stdout for compatibility
        console_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)  # Changed to INFO as default

        # Remove any existing handlers to avoid duplicate logs
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        logging.info(f"Logging configured for {self.company_id}. Log file: {log_filepath}")
def _setup_directories(self):
        """Create necessary directories for downloads and debug info."""
        try:
            for dir_path in [self.download_dir, self.debug_dir, self.logs_dir, self.archives_dir]:
                os.makedirs(dir_path, exist_ok=True)
        except Exception as e:
            if 'logging' in sys.modules and logging.getLogger().handlers:
                logging.error(f"Error creating directories: {e}")
                logging.error(traceback.format_exc())
            else:
                print(f"Error creating directories: {e}", file=sys.stderr)
                traceback.print_exc()

    def _load_processed(self) -> Set[str]:
        """Loads the set of already processed licitacao codes."""
        try:
            if os.path.exists(self.processed_file):
                with open(self.processed_file, 'r') as f:
                    data = json.load(f)
                    # Add basic validation
                    processed_list = data.get("processed_compesa_processos", [])
                    if isinstance(processed_list, list):
                        logging.info(f"Loaded {len(processed_list)} processed codes from {self.processed_file}")
                        return set(processed_list)
                    else:
                        logging.warning(f"Invalid format in {self.processed_file}, expected a list. Starting fresh.")
                        return set()
            else:
                logging.info(f"Processed file not found ({self.processed_file}), starting fresh.")
                return set()
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Error loading processed file {self.processed_file}: {e}. Starting fresh.")
            return set()

    def _save_processed(self, processed_set: Set[str]) -> None:
        """Saves the set of processed licitacao codes to the JSON file."""
        try:
            os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
            with open(self.processed_file, 'w') as f:
                sorted_list = sorted(list(processed_set))
                json.dump({"processed_compesa_processos": sorted_list}, f, indent=2)
            logging.info(f"Saved {len(processed_set)} processed codes to {self.processed_file}")
        except IOError as e:
            logging.error(f"Error saving processed file {self.processed_file}: {e}")

    async def setup_playwright(self):
        """Initialize Playwright browser."""
        logging.info("Setting up Playwright...")
        try:
            # Start Playwright
            playwright = await async_playwright().start()
            logging.info("Playwright instance started successfully")

            # Launch browser
            browser = await playwright.chromium.launch(
                headless=True,  # Set to True for production, False for testing
                slow_mo=250
            )
            logging.info("Browser launched successfully")

            # Create browser context
            context = await browser.new_context(
                accept_downloads=True,
                viewport={"width": 1280, "height": 800}
            )
            context.set_default_timeout(60000)
            logging.info("Browser context created with 60s timeout")

            # Create page
            page = await context.new_page()
            logging.info("Page created successfully")

            logging.info("Playwright setup complete")
            return playwright, browser, context, page
        except Exception as e:
            logging.error(f"CRITICAL ERROR in setup_playwright: {e}")
            logging.error(traceback.format_exc())
            raise

    async def teardown_playwright(self, browser, playwright):
        """Clean up Playwright resources."""
        try:
            logging.info("Cleaning up Playwright resources...")
            if browser:
                await browser.close()
                logging.info("Browser closed")
            if playwright:
                await playwright.stop()
                logging.info("Playwright stopped")
        except Exception as e:
            logging.error(f"Error during Playwright teardown: {e}")
            logging.error(traceback.format_exc())
    async def process_keyword(self, keyword: str, context: BrowserContext, page: Page, 
                             processed_codes: Set[str]) -> List[Dict[str, Any]]:
        """
        Processes a single keyword: navigates, searches, identifies rows, and downloads files.
        
        Args:
            keyword: The keyword to search for
            context: Playwright browser context
            page: Playwright page 
            processed_codes: Set of already processed tender IDs
            
        Returns:
            List of dictionaries with processed tender information
        """
        logging.info(f"\n===== PROCESSING KEYWORD: {keyword} =====")
        processed_tenders = []
        try:
            # Step 1: Navigate to the bidding portal (for each keyword)
            logging.info(f"Step 1: Navigating to {self.target_url} for keyword '{keyword}'")
            await page.goto(self.target_url)
            await page.wait_for_load_state('networkidle')
            logging.info("Page loaded")

            # Step 2: Find the content frame...
            logging.info("Step 2: Finding the content frame...")
            content_frame = page.frame(name="mainform")
            if content_frame:
                logging.info('Found the content frame using name="mainform"')
            else:
                logging.warning('Could not find frame with name="mainform", falling back to searching by content')
                frames = page.frames
                logging.info(f"Found {len(frames)} frames")
                for frame in frames:
                    logging.debug(f"Checking frame URL: {frame.url}")
                    try:
                        # Check for a distinctive element within the frame
                        if await frame.locator('.HTMLTab:has-text("ACOMPANHAMENTO")').count() > 0:
                            content_frame = frame
                            logging.info('Found the content frame by searching for ACOMPANHAMENTO tab text')
                            break
                    except Exception as find_frame_err:
                        logging.debug(f"Error checking frame {frame.url}: {find_frame_err}")
                        continue

            if not content_frame:
                logging.error('Could not find a suitable content frame. Cannot proceed.')
                return processed_tenders

            # Step 3: Click on the ACOMPANHAMENTO tab...
            logging.info("Step 3: Clicking on ACOMPANHAMENTO tab...")
            acompanhamento_tab_selector = '.HTMLTab:has-text("ACOMPANHAMENTO")'
            try:
                await content_frame.locator(acompanhamento_tab_selector).click(timeout=10000)
                await page.wait_for_load_state('networkidle', timeout=15000)
                logging.info('Clicked on ACOMPANHAMENTO tab')
                await page.wait_for_timeout(3000)
            except Exception as e:
                logging.error(f"Failed to click ACOMPANHAMENTO tab: {e}")
                logging.error(traceback.format_exc())
                return processed_tenders

            # Step 4: Find and click the specific search field for 'OBJETO'
            logging.info("Step 4: Finding and clicking the search field for OBJETO...")
            search_field_selector = 'div#TAcomp_filtro_3 input[data-camposql="objeto"]'
            try:
                search_field = content_frame.locator(search_field_selector)
                await search_field.wait_for(state="visible", timeout=10000)
                await search_field.click(timeout=5000)
                logging.info('Search field clicked')
            except Exception as e:
                logging.error(f"Could not find or click the OBJETO search field: {e}")
                logging.error(traceback.format_exc())
                return processed_tenders

            # Step 5: Type the keyword in the search field
            logging.info(f'Step 5: Typing "{keyword}" in the search field...')
            await search_field.fill(keyword)
            await page.keyboard.press('Enter')
            logging.info('Waiting for search results to load...')
            try:
                await content_frame.locator('#TAcomp_corpo_2').wait_for(timeout=15000)
                await page.wait_for_timeout(5000)
                await page.wait_for_load_state('networkidle', timeout=10000)
                logging.info(f'Entered "{keyword}" and waited for results.')
            except PlaywrightTimeoutError:
                logging.warning(f"Timed out waiting for search results table body for keyword '{keyword}'.")
            except Exception as e:
                logging.error(f"Error waiting for search results: {e}")
                logging.error(traceback.format_exc())

            # Step 6: Locate rows and download files using selectors
            logging.info("Step 6: Locating table rows and processing entries from 2025...")
            table_body_selector = '#TAcomp_corpo_2'
            row_selector = f'{table_body_selector} div.HTMLTableBodyRow'

            try:
                await content_frame.wait_for_selector(table_body_selector, timeout=10000)
                rows = await content_frame.locator(row_selector).all()
                logging.info(f"Found {len(rows)} rows in the results table for keyword '{keyword}'.")

                if not rows:
                    logging.warning(f"No rows found in the table for keyword '{keyword}'.")
                    return processed_tenders

                for i, row in enumerate(rows):
                    licitacao_code = "N/A"
                    data_value = ""
                    objeto_value = ""
                    
                    try:
                        cells = row.locator('div.HTMLTableBodyCell')

                        # Extract licitacao code (tender ID)
                        try:
                            licitacao_code_raw = await cells.nth(1).text_content(timeout=3000)
                            licitacao_code = licitacao_code_raw.strip() if licitacao_code_raw else "N/A"
                        except Exception as cell_err:
                            logging.warning(f"Row {i+1}: Could not extract Licitacao code: {cell_err}")
                            licitacao_code = "N/A"

                        # Extract date (to filter by 2025)
                        try:
                            data_value_raw = await cells.nth(3).text_content(timeout=3000)
                            data_value = data_value_raw.strip() if data_value_raw else ""
                        except Exception as cell_err:
                             logging.warning(f"Row {i+1} ({licitacao_code}): Could not extract Data value: {cell_err}")
                             data_value = ""
                             
                        # Extract the tender title (objeto)
                        try:
                            objeto_value_raw = await cells.nth(2).text_content(timeout=3000)
                            objeto_value = objeto_value_raw.strip() if objeto_value_raw else "Unknown object"
                        except Exception as cell_err:
                             logging.warning(f"Row {i+1} ({licitacao_code}): Could not extract Objeto value: {cell_err}")
                             objeto_value = "Unknown object"

                        logging.debug(f"Row {i+1}: Licitacao='{licitacao_code}', Data='{data_value}', Objeto='{objeto_value}'")

                        # Skip if not 2025
                        if "2025" not in data_value:
                            logging.debug(f"Skipping row {i+1} ({licitacao_code}) - DATA field ('{data_value}') does not contain '2025'.")
                            continue

                        # Skip if invalid licitacao code
                        if licitacao_code == "N/A":
                             logging.warning(f"Skipping row {i+1} due to missing licitacao code after extraction attempt.")
                             continue
                             
                        # Skip if already processed
                        if licitacao_code in processed_codes:
                            logging.info(f"Skipping already processed licitacao: {licitacao_code}")
                            continue

                        logging.info(f"Processing matching 2025 entry: Licitacao={licitacao_code}, Data={data_value}, Objeto={objeto_value}")

                        # Prepare source URL (for tender info)
                        source_url = self.target_url  # Base URL as a fallback

                        # Find and click download button
                        button_locator = cells.first.locator('button')
                        
                        # Attempt download
                        logging.info(f"Attempting download for {licitacao_code}")
                        download_successful = False
                        download_path = ""
                        zip_path = ""
                        
                        try:
                            async with page.expect_download(timeout=30000) as download_info:
                                await button_locator.click(timeout=10000)
                                logging.info(f"Button clicked for {licitacao_code}")
                                
                            download = await download_info.value
                            sanitized_code = "".join(c if c.isalnum() or c in '-_' else '_' for c in licitacao_code)
                            temp_filename = f"{keyword.replace(' ', '_')}-{sanitized_code}.zip"
                            
                            # Temporary download path
                            temp_download_path = os.path.join(self.download_dir, temp_filename)
                            os.makedirs(os.path.dirname(temp_download_path), exist_ok=True)
                            
                            logging.info(f"Saving download for {licitacao_code} to: {temp_download_path}")
                            await download.save_as(temp_download_path)
                            logging.info(f"File saved: {temp_download_path}")
                            
                            # Create the final ZIP archive path
                            zip_filename = f"COMPESA_ACOMPANHAMENTO_{sanitized_code}.zip"
                            zip_path = os.path.join(self.archives_dir, zip_filename)
                            
                            # Create ZIP archive
                            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                                zipf.write(temp_download_path, os.path.basename(temp_download_path))
                                
                            logging.info(f"ZIP archive created: {zip_path}")
                            download_successful = True
                            download_path = temp_download_path
                            
                            # Add to processed list
                            processed_codes.add(licitacao_code)
                            
                            # Add tender information to results
                            processed_tenders.append({
                                "tender_id": licitacao_code,
                                "title": objeto_value,
                                "downloaded_zip_path": zip_path,
                                "source_url": source_url
                            })
                            
                        except Exception as download_err:
                            logging.error(f"Error downloading for licitacao {licitacao_code}: {download_err}")
                            logging.error(traceback.format_exc())
                    
                    except Exception as row_err:
                        logging.error(f"Error processing row {i+1} ({licitacao_code}): {row_err}")
                        logging.error(traceback.format_exc())

                logging.info(f"Finished processing {len(rows)} rows for keyword '{keyword}'. Successfully processed {len(processed_tenders)} tenders.")

            except PlaywrightTimeoutError as pte_table:
                 logging.error(f"Timed out waiting for table elements for keyword '{keyword}': {pte_table}")
            except Exception as e_table:
                logging.error(f"Error locating or iterating through table rows for keyword '{keyword}': {e_table}")
                logging.error(traceback.format_exc())

            logging.info(f"===== FINISHED PROCESSING KEYWORD: {keyword} =====")
            return processed_tenders

        except Exception as e:
            logging.error(f"CRITICAL ERROR processing keyword '{keyword}': {e}")
            logging.error(traceback.format_exc())
            return processed_tenders
            
    async def process_all_keywords(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        Process all keywords and collect tender information.
        
        Args:
            keywords: List of keywords to search for
            
        Returns:
            List of dictionaries with processed tender information
        """
        all_processed_tenders = []
        processed_codes = self._load_processed()
        
        try:
            playwright, browser, context, page = await self.setup_playwright()
            
            try:
                for keyword in keywords:
                    keyword_results = await self.process_keyword(keyword, context, page, processed_codes)
                    all_processed_tenders.extend(keyword_results)
                    
                    # Save processed state after each keyword
                    self._save_processed(processed_codes)
            finally:
                if browser:
                    await self.teardown_playwright(browser, playwright)
                
        except Exception as e:
            logging.error(f"Error in process_all_keywords: {e}")
            logging.error(traceback.format_exc())
            
        return all_processed_tenders

def run_handler(company_id: str, output_dir: str, keywords: List[str], notion_database_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Main handler function for COMPESA ACOMPANHAMENTO downloads.
    
    Args:
        company_id: Identifier for the company
        output_dir: Base directory for all outputs
        keywords: List of keywords to search for
        notion_database_id: Optional Notion database ID (not used by this handler)
        
    Returns:
        Dictionary with standardized structure:
        {
            "success": bool,
            "company_id": str,
            "new_tenders_processed": [
                {
                    "tender_id": str,
                    "title": str,
                    "downloaded_zip_path": str,
                    "source_url": str
                }
            ],
            "error_message": Optional[str]
        }
    """
    if not keywords:
        # Default keywords if none provided
        keywords = ['tubo', 'polietileno', 'PEAD', 'polimero', 'PAM', 'hidrômetro', 'medidor']
    
    error_message = None
    downloader = PolimeroSearchDownloader(company_id, output_dir)
    
    try:
        # Create event loop and run async process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        processed_tenders = loop.run_until_complete(downloader.process_all_keywords(keywords))
        loop.close()
        
        success = True
    except Exception as e:
        logging.error(f"Critical error in run_handler: {e}")
        logging.error(traceback.format_exc())
        processed_tenders = []
        success = False
        error_message = f"Critical error: {str(e)}"
    
    return {
        "success": success,
        "company_id": company_id,
        "new_tenders_processed": processed_tenders,
        "error_message": error_message
    }

# For CLI testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="COMPESA ACOMPANHAMENTO Download Handler")
    parser.add_argument("--output-dir", required=True, help="Base output directory")
    parser.add_argument("--company-id", default="COMPESA_ACOMPANHAMENTO", help="Company ID")
    parser.add_argument("--keywords", nargs="+", help="Keywords to search for")
    
    args = parser.parse_args()
    
    result = run_handler(
        company_id=args.company_id,
        output_dir=args.output_dir,
        keywords=args.keywords
    )
    
    print(json.dumps(result, indent=2))
                
