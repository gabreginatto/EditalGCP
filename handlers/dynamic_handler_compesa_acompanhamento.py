#!/usr/bin/env python3
"""
Polymer Bidding Process Search & Download Script

This script automates the search and download of bidding processes containing specific keywords on Compesa's portal:
- Uses Playwright for browser automation and direct DOM interaction
- Downloads documents for 2025 bidding processes
"""

import os
import sys
import logging
import asyncio
import time
import json
import traceback
import zipfile
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

class PolimeroSearchDownloader:
    """Handles searching for keyword biddings and downloads relevant documents from 2025 entries."""

    def __init__(self, output_dir="downloads"):
        """Initialize the downloader with output directory."""
        # Load environment variables
        load_dotenv()

        # Setup directories
        self.DOWNLOAD_DIR = output_dir
        self.DEBUG_DIR = os.path.join(output_dir, "debug")
        self.LOGS_DIR = os.path.join(output_dir, "logs")
        self.ARCHIVES_DIR = os.path.join(output_dir, "archives")
        self.PROCESSED_FILE = os.path.join(output_dir, "processed_compesa.json")
        self._setup_directories()

        # Setup logging
        self._setup_logging()

        # Log initialization
        logging.info("=" * 80)
        logging.info("KEYWORD SEARCH DOWNLOADER STARTING (SELECTOR BASED)")
        logging.info(f"Python version: {sys.version}")
        logging.info("=" * 80)

        # Target URL
        self.TARGET_URL = "https://portalscl.compesa.com.br:8743/webrunstudio/form.jsp?sys=SCL&action=openform&formID=7&align=0&mode=-1&goto=-1&filter=&scrolling=yes"

        logging.info(f"PolimeroSearchDownloader initialized with target URL: {self.TARGET_URL}")

    def _setup_logging(self):
        """Setup logging with both file and console handlers."""
        timestamp = int(time.time())
        os.makedirs(self.LOGS_DIR, exist_ok=True)
        log_filename = f"keyword_search_{timestamp}.log"
        log_filepath = os.path.join(self.LOGS_DIR, log_filename)

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Remove any existing handlers to avoid duplicate logs
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        logging.info(f"Logging configured. Log file: {log_filepath}")

    def _setup_directories(self):
        """Create necessary directories for downloads and debug info."""
        try:
            for dir_path in [self.DOWNLOAD_DIR, self.DEBUG_DIR, self.LOGS_DIR, self.ARCHIVES_DIR]:
                os.makedirs(dir_path, exist_ok=True)
        except Exception as e:
            if 'logging' in sys.modules and logging.getLogger().handlers:
                logging.error(f"Error creating directories: {e}")
                logging.error(traceback.format_exc())
            else:
                print(f"Error creating directories: {e}", file=sys.stderr)
                traceback.print_exc()

    async def setup_playwright(self):
        """Initialize Playwright browser in non-headless mode."""
        logging.info("Setting up Playwright...")
        try:
            # Start Playwright
            playwright = await async_playwright().start()
            logging.info("Playwright instance started successfully")

            # Launch browser
            browser = await playwright.chromium.launch(
                headless=False,
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

    # Add helper methods for loading/saving processed list
    def _load_processed(self):
        """Loads the set of already processed licitacao codes."""
        try:
            if os.path.exists(self.PROCESSED_FILE):
                with open(self.PROCESSED_FILE, 'r') as f:
                    data = json.load(f)
                    # Add basic validation
                    processed_list = data.get("processed_compesa_processos", [])
                    if isinstance(processed_list, list):
                        logging.info(f"Loaded {len(processed_list)} processed codes from {self.PROCESSED_FILE}")
                        return set(processed_list)
                    else:
                        logging.warning(f"Invalid format in {self.PROCESSED_FILE}, expected a list. Starting fresh.")
                        return set()
            else:
                logging.info(f"Processed file not found ({self.PROCESSED_FILE}), starting fresh.")
                return set()
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Error loading processed file {self.PROCESSED_FILE}: {e}. Starting fresh.")
            return set()

    def _save_processed(self, processed_set):
        """Saves the set of processed licitacao codes to the JSON file."""
        try:
            os.makedirs(os.path.dirname(self.PROCESSED_FILE), exist_ok=True)
            with open(self.PROCESSED_FILE, 'w') as f:
                sorted_list = sorted(list(processed_set))
                json.dump({"processed_compesa_processos": sorted_list}, f, indent=2)
            logging.info(f"Saved {len(processed_set)} processed codes to {self.PROCESSED_FILE}")
        except IOError as e:
            logging.error(f"Error saving processed file {self.PROCESSED_FILE}: {e}")

    async def process_keyword(self, keyword, context, page, processed_codes):
        """Processes a single keyword: navigates, searches, identifies rows, and downloads using selectors."""
        logging.info(f"\n===== PROCESSING KEYWORD: {keyword} =====")
        download_results = []
        try:
            # Step 1: Navigate to the bidding portal (for each keyword)
            logging.info(f"Step 1: Navigating to {self.TARGET_URL} for keyword '{keyword}'")
            await page.goto(self.TARGET_URL)
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
                download_results.append({"licitacao": "N/A", "keyword": keyword, "success": False, "error": "Could not find content frame"})
                return download_results

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
                 return download_results

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
                 return download_results

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
                    return download_results

                processed_count_for_keyword = 0
                for i, row in enumerate(rows):
                    licitacao_code = "N/A"
                    data_value = ""
                    try:
                        cells = row.locator('div.HTMLTableBodyCell')

                        try:
                            licitacao_code_raw = await cells.nth(1).text_content(timeout=3000)
                            licitacao_code = licitacao_code_raw.strip() if licitacao_code_raw else "N/A"
                        except Exception as cell_err:
                            logging.warning(f"Row {i+1}: Could not extract Licitacao code: {cell_err}")
                            licitacao_code = "N/A"

                        try:
                            data_value_raw = await cells.nth(3).text_content(timeout=3000)
                            data_value = data_value_raw.strip() if data_value_raw else ""
                        except Exception as cell_err:
                             logging.warning(f"Row {i+1} ({licitacao_code}): Could not extract Data value: {cell_err}")
                             data_value = ""

                        logging.debug(f"Row {i+1}: Licitacao='{licitacao_code}', Data='{data_value}'")

                        if "2025" not in data_value:
                            logging.debug(f"Skipping row {i+1} ({licitacao_code}) - DATA field ('{data_value}') does not contain '2025'.")
                            continue

                        if licitacao_code == "N/A":
                             logging.warning(f"Skipping row {i+1} due to missing licitacao code after extraction attempt.")
                             continue
                        if licitacao_code in processed_codes:
                            logging.info(f"Skipping already processed licitacao: {licitacao_code}")
                            continue

                        logging.info(f"Processing matching 2025 entry: Licitacao={licitacao_code}, Data={data_value}")

                        button_locator = cells.first.locator('button')

                        logging.info(f"Attempting download for {licitacao_code}")
                        async with page.expect_download(timeout=30000) as download_info:
                            try:
                                await button_locator.click(timeout=10000)
                                logging.info(f"Button clicked for {licitacao_code}")
                            except Exception as click_err:
                                logging.error(f"Failed to click download button for {licitacao_code}: {click_err}")
                                raise

                        download = await download_info.value
                        sanitized_code = "".join(c if c.isalnum() or c in '-_' else '_' for c in licitacao_code)
                        filename = f"{keyword.replace(' ', '_')}-{sanitized_code}.zip"

                        download_path = os.path.join(self.DOWNLOAD_DIR, filename)
                        os.makedirs(os.path.dirname(download_path), exist_ok=True)

                        logging.info(f"Attempting to save download for {licitacao_code} ('{keyword}') to: {download_path}")
                        await download.save_as(download_path)
                        logging.info(f"File saved successfully for {licitacao_code}: {download_path}")

                        zip_filename = f"licitacao_{sanitized_code}_{int(time.time())}.zip"
                        zip_path = os.path.join(self.ARCHIVES_DIR, zip_filename)

                        try:
                            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                                zipf.write(download_path, os.path.basename(download_path))
                            logging.info(f"ZIP archive created at: {zip_path}")
                        except Exception as zip_error:
                            logging.error(f"Error creating ZIP archive for {licitacao_code}: {zip_error}")

                        processed_codes.add(licitacao_code)
                        processed_count_for_keyword += 1

                        download_results.append({
                            "licitacao": licitacao_code,
                            "keyword": keyword,
                            "filename": filename,
                            "path": download_path,
                            "success": True
                        })

                    except PlaywrightTimeoutError as pte:
                        error_msg = f"Timeout during Playwright operation (finding/clicking/downloading) for row {i+1}"
                        logging.error(f"{error_msg} ({licitacao_code}): {pte}")
                        download_results.append({
                            "licitacao": licitacao_code,
                            "keyword": keyword,
                            "success": False,
                            "error": error_msg
                        })
                    except Exception as e:
                        error_msg = f"Error processing row {i+1} ({licitacao_code})"
                        logging.error(f"{error_msg}: {e}")
                        logging.error(traceback.format_exc())
                        download_results.append({
                            "licitacao": licitacao_code,
                            "keyword": keyword,
                            "success": False,
                            "error": f"{error_msg}: {e}"
                        })

                logging.info(f"Finished processing {len(rows)} rows for keyword '{keyword}'. Successfully downloaded {processed_count_for_keyword} new items.")

            except PlaywrightTimeoutError as pte_table:
                 logging.error(f"Timed out waiting for table elements for keyword '{keyword}': {pte_table}")
                 download_results.append({ "licitacao": "N/A", "keyword": keyword, "success": False, "error": f"Timeout waiting for table: {pte_table}"})
            except Exception as e_table:
                logging.error(f"Error locating or iterating through table rows for keyword '{keyword}': {e_table}")
                logging.error(traceback.format_exc())
                download_results.append({
                    "licitacao": "N/A",
                    "keyword": keyword,
                    "success": False,
                    "error": f"Failed to process table rows: {e_table}"
                })

            logging.info(f"===== FINISHED PROCESSING KEYWORD: {keyword} =====")
            return download_results

        except Exception as e:
            logging.error(f"CRITICAL ERROR processing keyword '{keyword}': {e}")
            logging.error(traceback.format_exc())
            if not any(r['keyword'] == keyword for r in download_results):
                 download_results.append({
                    "licitacao": "N/A",
                    "keyword": keyword,
                    "success": False,
                    "error": f"Critical error: {e}"
                })
            return download_results

async def main():
    """Main function to run the script for multiple keywords."""
    keywords = ['tubo', 'polietileno', 'PEAD', 'polimero', 'PAM', 'hidrômetro', 'medidor']
    downloader = PolimeroSearchDownloader()
    all_results = []
    playwright = None
    browser = None
    processed_codes = downloader._load_processed()

    try:
        playwright, browser, context, page = await downloader.setup_playwright()

        for keyword in keywords:
            keyword_results = await downloader.process_keyword(keyword, context, page, processed_codes)
            all_results.extend(keyword_results)

        logging.info("\n===== SCRIPT EXECUTION SUMMARY =====")
        if all_results:
            successful_downloads = [r for r in all_results if r.get('success', False)]
            logging.info(f"Processed {len(keywords)} keywords.")
            logging.info(f"Total successful downloads: {len(successful_downloads)}")
            for result in successful_downloads:
                 logging.info(f"  - Keyword: '{result.get('keyword')}', Licitacao: {result.get('licitacao')}, File: {result.get('filename')}")

            failed_downloads = [r for r in all_results if not r.get('success', False) and r.get('error') != 'Skipped (already processed)']
            if failed_downloads:
                logging.warning(f"Total actual failed download attempts: {len(failed_downloads)}")
                for result in failed_downloads:
                    logging.warning(f"  - Keyword: '{result.get('keyword')}', Licitacao: {result.get('licitacao', 'N/A')}, Error: {result.get('error')}")

            skipped_downloads = [r for r in all_results if not r.get('success', False) and r.get('error') == 'Skipped (already processed)']
            if skipped_downloads:
                logging.info(f"Total skipped (already processed): {len(skipped_downloads)}")
        else:
            logging.info("No downloads were attempted or completed across all keywords.")

        logging.info("Script completed.")
        return True

    except Exception as e:
        logging.error(f"CRITICAL ERROR in main execution: {e}")
        logging.error(traceback.format_exc())
        return False
    finally:
        if browser:
            await downloader.teardown_playwright(browser, playwright)
        downloader._save_processed(processed_codes)

if __name__ == "__main__":
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())