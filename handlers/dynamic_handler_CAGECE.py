import os
import sys
import time
import re
import json
import logging
import zipfile
import shutil
import glob
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError

# --- Configuration ---
# Define paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
base_download_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
downloads_dir = Path(base_download_dir) / "downloads"
log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

# Define keywords to search for
KEYWORDS = ["tubo", "polietileno", "PEAD", "polimero", "PAM", "hidrômetro", "medidor"]

# Define default timeout for details page processing (in seconds)
DETAILS_PAGE_TIMEOUT = 25

# Group configuration variables for better organization
CONFIG = {
    "target_url": "https://s2gpr.sefaz.ce.gov.br/licita-web/paginas/licita/PublicacaoList.seam",
    "downloads_dir": downloads_dir, # Base output directory
    "pdfs_dir": downloads_dir / "pdfs", # Directory for PDF files
    "archives_dir": downloads_dir / "archives", # Directory for final ZIP archives
    "temp_dir": downloads_dir / "temp", # Directory for temporary files per process
    "debug_dir": downloads_dir / "debug", # Debugging outputs
    "logs_dir": downloads_dir / "logs", # Directory for log files
    "screenshots_dir": downloads_dir / "debug" / "screenshots", # Screenshots directory
    "processed_log_file": downloads_dir / "processed_processos.txt", # File to track processed IDs (text format)
    "processed_ids_json": downloads_dir / "processed_CAGECE.json", # File to track processed IDs (JSON format)
    "take_screenshots": True,  # Enable taking screenshots
    "headless_mode": False, # Set True for production/headless execution
    "search_params": {
        "organization_label": "COMPANHIA DE AGUA E ESGOTO DO CEARA",
        "acquisition_nature_label": "EQUIPAMENTOS E MATERIAL PERMANENTE",
        "start_date": {
            "day": 1,
            "month": 0,  # 0-based month (0 = January)
            "year": 2025
        },
        "object_keyword": "tubo"
    },
    "selectors": {
        # Main Form & Search Fields
        "main_form": "#formularioDeCrud",
        "organization_dropdown": "#formularioDeCrud\:promotorCotacaoDecoration\:promotorLicitacao",
        "acquisition_nature_dropdown": "#formularioDeCrud\:naturezaAquisicaoDecoration\:naturezaAquisicao",
        "object_input": "#formularioDeCrud\:objetoContratacaoDecoration\:objetoContratacao",
        "object_input_fallback": '#formularioDeCrud textarea', # Fallback if input fails
        "search_button": '#formularioDeCrud\:pesquisar',
        "search_button_fallback": 'input[value="Pesquisar"]',

        # Calendar
        "start_date_input": "#formularioDeCrud\:inicioAcolhimentoDecoration\:inicioAcolhimentoPropostasInputDate",
        "start_date_button": "#formularioDeCrud\:inicioAcolhimentoDecoration\:inicioAcolhimentoPropostasPopupButton",
        "calendar_popup": 'table.rich-calendar-popup[style*="z-index"]', # Visible calendar
        "calendar_header": 'td.rich-calendar-header',
        "prev_year_button": 'div[onclick*="prevYear"]',
        "next_year_button": 'div[onclick*="nextYear"]',
        "prev_month_button": 'div[onclick*="prevMonth"]',
        "next_month_button": 'div[onclick*="nextMonth"]',
        "calendar_day_cell": 'td.rich-calendar-cell:not(.rich-calendar-boundary-dates)',
        "calendar_apply_button": 'div.rich-calendar-tool-btn:has-text("Apply")',

        # Search Results Table & Rows
        "results_table": '#formularioDeCrud\:pagedDataTable',
        "results_row_radio_img_exact": "img#formularioDeCrud\:pagedDataTable\:375975\:uncheckRadio", # Specific target
        "results_row_radio_img_generic": "table#formularioDeCrud\:pagedDataTable tbody tr td.primeiraColuna img[style*='cursor:pointer']", # Generic target
        "visualize_button": 'input[value="Visualizar"]', # General visualize button
        "visualize_button_enabled": 'input[value="Visualizar"]:not([disabled])', # Enabled visualize button

        # Details Page & Document Download
        "doc_tables": [ # List of potential table selectors
            '#formularioDeCrud\:docTermoListAction',
            '#formularioDeCrud\:arquivoProcessoTable',
            'table[id*="docTermoListAction"]',
            'table[id*="arquivoProcessoTable"]',
            'div#formularioDeCrud\:docTermoParticipacao table',
            'div#formularioDeCrud\:documentos table',
            'table[id*="formularioDeCrud"][id*="List"]',
            'div.tabelas table'
        ],
        "doc_download_button_primary_termo": '#formularioDeCrud\\:downloadButtonInf', # Primary for 'docTermo' table
        "doc_download_button_primary_arquivo": 'div#formularioDeCrud\\:download input[value="Baixar"]', # Primary for 'arquivoProcesso' table
        "doc_download_buttons_fallback": [ # Fallback selectors
            'input[id="formularioDeCrud:downloadButtonInf"]',
            'input[value="Download"]',
            'input[type="submit"][value="Download"]',
            'input[value="Baixar"]',
            'input[type="submit"][value="Baixar"]',
            'input[name="formularioDeCrud:j_id330"]',
            'div#formularioDeCrud\\:download input',
            'div#formularioDeCrud\\:grupoButtonsInf input',
            'div.actionButtons input[value="Download"]',
            'div.actionButtons input[value="Baixar"]',
            'div.actionButtons input[type="submit"]',
            'input[id*="download"][type="button"]',
            'input[id*="download"][type="submit"]',
        ],
        "return_button_selectors": [ # Selectors for the return button
             'input[id="formularioDeCrud:pesquisar"]', # This ID is reused, check context
             'input[value="Retornar para Pesquisa"]',
             'input.retornarPesquisa',
             'input.sec.retornarPesquisa',
             '#formularioDeCrud\\:pesquisar'
        ],
        # Selectors for document names and radio buttons within tables (using format strings)
        "doc_name_arquivo": '#{base_id}:{index}:j_id324', # For arquivoProcessoTable
        "doc_name_termo": 'id={base_id}:{index}:docTermo', # For docTermoListAction
        "doc_name_fallback_arquivo": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(2)',
        "doc_name_fallback_termo": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(2) span',
        "doc_radio_cell_arquivo": 'td[id="{base_id}:{index}:j_id321"]',
        "doc_radio_cell_termo": 'td[id="{base_id}:{index}:j_id203"]',
        "doc_radio_fallback_img": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(1) img',
        "doc_radio_fallback_span_img": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(1) span img',
    },
    "timeouts": { # Timeouts in milliseconds
        "navigation": 90000,
        "default_page": 60000, # Default timeout for page actions
        "default_expect": 30000, # Default for expect assertions
        "element_wait": 15000,
        "search_results": 30000,
        "button_enable": 15000,
        "download": 60000, # Increased download timeout
        "details_load": 20000, # Increased details page load timeout
        "network_idle": 20000, # Timeout for waiting for network idle
        "short_pause": 1500, # General short pause after actions
        "calendar_popup": 7000,
        "calendar_day_click": 3000,
        "calendar_apply_click": 3000,
        "radio_click_wait": 2000, # Wait after clicking a radio button
        "details_render_wait": 7000, # Extra wait for details page rendering
        "return_navigation": 20000,
        "return_element_wait": 15000,
    },
    "calendar_months_pt": [ # Portuguese months for parsing
        'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
        'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
    ]
}

# --- Logging Setup ---
# Configure logging with file and console output
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = f"dynamic_handler_cagece_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("CAGECEHandler")

# --- Helper Functions ---

def setup_output_dirs():
    """Ensure all necessary output directories exist"""
    output_dirs = {
        'pdf': CONFIG["pdfs_dir"],
        'archive': CONFIG["archives_dir"],
        'debug': CONFIG["debug_dir"],
        'logs': CONFIG["logs_dir"],
        'screenshots': CONFIG["screenshots_dir"],
        'temp': CONFIG["temp_dir"]
    }
    
    for dir_path in output_dirs.values():
        os.makedirs(dir_path, exist_ok=True)
        
    return output_dirs

def load_processed_ids_json():
    """Load processed IDs from JSON file"""
    processed_ids = []
    json_path = CONFIG["processed_ids_json"]
    
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                processed_ids = data.get("processed_cagece_processos", [])
            logger.info(f"Loaded {len(processed_ids)} processed IDs from {json_path}")
        except Exception as e:
            logger.error(f"Error loading processed IDs from {json_path}: {e}")
            # If the file exists but is corrupted, create a backup
            if json_path.exists():
                backup_path = json_path.with_suffix('.json.bak')
                try:
                    shutil.copy(json_path, backup_path)
                    logger.info(f"Created backup of corrupted JSON at {backup_path}")
                except Exception as backup_error:
                    logger.error(f"Failed to create backup of corrupted JSON: {backup_error}")
    else:
        logger.info(f"Processed IDs JSON file {json_path} not found, starting fresh.")
    
    return processed_ids

def save_processed_id_json(process_id, metadata=None):
    """Save a processed ID to the JSON file with proper structure"""
    json_path = CONFIG["processed_ids_json"]
    
    # Ensure the directory exists
    json_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Default structure
    processed_data = {
        "processed_cagece_processos": []
    }
    
    # Load existing data if file exists
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                processed_data = json.load(f)
                # Ensure the structure exists
                if "processed_cagece_processos" not in processed_data:
                    processed_data["processed_cagece_processos"] = []
        except Exception as e:
            logger.error(f"Error loading processed IDs from {json_path}: {e}")
    
    # Add the process ID if not already present
    if process_id not in processed_data["processed_cagece_processos"]:
        processed_data["processed_cagece_processos"].append(process_id)
    
    # Save back to file
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved processed ID {process_id} to {json_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving processed ID {process_id} to {json_path}: {e}")
        return False

def safe_screenshot(page, filename_prefix="screenshot"):
    """Takes a screenshot safely, ignoring errors if the page/browser is closed."""
    if not CONFIG["take_screenshots"]:
        return

    try:
        # Ensure the screenshots directory exists
        screenshots_dir = CONFIG["screenshots_dir"]
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        path = screenshots_dir / f"{filename_prefix}_{timestamp}.png"
        page.screenshot(path=path)
        logger.info(f"Screenshot saved: {path}")
    except Exception as e:
        logger.warning(f"Could not take screenshot '{filename_prefix}': {e}")

def select_dropdown_option_by_label(page, dropdown_selector, label_text):
    """Selects an option from a dropdown based on its visible text label."""
    logging.info(f"Selecting dropdown option '{label_text}' from '{dropdown_selector}'")
    try:
        dropdown = page.locator(dropdown_selector)
        expect(dropdown).to_be_enabled(timeout=CONFIG["timeouts"]["element_wait"])
        # Use Playwright's built-in label matching
        dropdown.select_option(label=re.compile(re.escape(label_text), re.IGNORECASE))
        logging.info(f"Selected option containing text: {label_text}")
        page.wait_for_timeout(CONFIG["timeouts"]["short_pause"]) # Wait for potential updates
        return True
    except PlaywrightTimeoutError:
        logging.error(f"Timeout waiting for dropdown '{dropdown_selector}' to be enabled.")
        safe_screenshot(page, f"error_dropdown_timeout_{dropdown_selector.replace(':', '_')}")
    except Exception as e:
        # Fallback if direct label match fails (less reliable)
        logging.warning(f"Direct label selection failed for '{label_text}': {e}. Trying manual iteration.")
        try:
            options = dropdown.locator('option')
            count = options.count()
            for i in range(count):
                option = options.nth(i)
                option_text = option.inner_text()
                if label_text in option_text:
                    option_value = option.get_attribute('value')
                    logging.info(f"Found matching option manually: text='{option_text}', value='{option_value}'")
                    dropdown.select_option(value=option_value)
                    page.wait_for_timeout(CONFIG["timeouts"]["short_pause"])
                    return True
            logging.error(f"Could not find any option containing text: {label_text}")
            safe_screenshot(page, f"error_dropdown_notfound_{label_text[:20]}")
            return False
        except Exception as e_fallback:
            logging.error(f"Error during manual dropdown option search for '{label_text}': {e_fallback}")
            safe_screenshot(page, f"error_dropdown_fallback_{label_text[:20]}")
            return False
    return False


def parse_calendar_header(header_text):
    """Parses the calendar header (e.g., '<< < abril 2025 > >>') to get month index and year."""
    clean_text = header_text.replace('<', '').replace('>', '').replace('x', '').strip().lower()
    logging.info(f"Parsing calendar header: '{clean_text}'")

    year_match = re.search(r'\b(20\d{2})\b', clean_text)
    current_year = int(year_match.group(1)) if year_match else None

    current_month_index = None
    for i, month_name in enumerate(CONFIG["calendar_months_pt"]):
        if month_name in clean_text:
            current_month_index = i
            break

    if current_year is None or current_month_index is None:
        logging.warning(f"Could not reliably parse year/month from header: '{header_text}'. Year: {current_year}, Month Index: {current_month_index}")
        # Add simple fallback for common cases if needed, but parsing should ideally work
        if 'abril' in clean_text: current_month_index = 3 # Example fallback
        if '2025' in clean_text: current_year = 2025

    logging.info(f"Parsed calendar: Month Index={current_month_index}, Year={current_year}")
    return current_month_index, current_year

def select_date_in_calendar(page, day, target_month_index, target_year):
    """Selects a specific date in the RichFaces calendar popup."""
    logging.info(f"Selecting date in calendar: {day}/{target_month_index + 1}/{target_year}")
    selectors = CONFIG["selectors"]
    timeouts = CONFIG["timeouts"]
    safe_screenshot(page, "calendar_before_interaction")

    try:
        calendar_popup = page.locator(selectors["calendar_popup"]).first
        expect(calendar_popup).to_be_visible(timeout=timeouts["calendar_popup"])
        logging.info("Calendar popup is visible.")

        # Navigate Year
        while True:
            header_text = calendar_popup.locator(selectors["calendar_header"]).text_content()
            current_month_idx, current_year = parse_calendar_header(header_text)

            if current_year == target_year or current_year is None:
                 logging.info(f"Reached target year {target_year} or failed to parse current year.")
                 break # Exit year loop

            year_nav_button = selectors["next_year_button"] if current_year < target_year else selectors["prev_year_button"]
            logging.info(f"Navigating year: Clicking {'next' if current_year < target_year else 'prev'} year button.")
            calendar_popup.locator(year_nav_button).click()
            page.wait_for_timeout(300) # Short pause for calendar update

        # Navigate Month (handle potential parsing failure)
        header_text = calendar_popup.locator(selectors["calendar_header"]).text_content() # Re-read header
        current_month_idx, current_year = parse_calendar_header(header_text) # Re-parse

        if current_month_idx is not None:
            month_diff = current_month_idx - target_month_index
            if month_diff != 0:
                month_nav_button = selectors["next_month_button"] if month_diff < 0 else selectors["prev_month_button"]
                clicks_needed = abs(month_diff)
                logging.info(f"Navigating month: Clicking {'next' if month_diff < 0 else 'prev'} month button {clicks_needed} times.")
                for _ in range(clicks_needed):
                    calendar_popup.locator(month_nav_button).click()
                    page.wait_for_timeout(300)
        else:
            # Fallback if month parsing failed (less reliable)
            logging.warning("Could not determine current month. Attempting fixed clicks (e.g., assuming April -> Jan = 3 prev clicks).")
            # Example: clicks_needed = 3 # Adjust based on expected default month
            # for _ in range(clicks_needed):
            #     calendar_popup.locator(selectors["prev_month_button"]).click()
            #     page.wait_for_timeout(300)
            # This part needs adjustment based on observed default calendar state if parsing fails
            logging.error("Month navigation fallback not implemented robustly. Calendar interaction might fail.")


        safe_screenshot(page, "calendar_after_navigation")

        # Select Day
        logging.info(f"Selecting day: {day}")
        # Regex to match the exact day number, potentially surrounded by whitespace
        day_regex = re.compile(f'^\\s*{day}\\s*$')
        day_cell = calendar_popup.locator(selectors["calendar_day_cell"]).filter(has_text=day_regex).first
        expect(day_cell).to_be_visible(timeout=timeouts["calendar_day_click"])
        day_cell.click()
        logging.info(f"Clicked day {day}.")
        page.wait_for_timeout(500) # Pause after clicking day
        safe_screenshot(page, f"calendar_after_day_{day}_click")

        # Click Apply
        logging.info("Clicking calendar 'Apply' button.")
        apply_button = calendar_popup.locator(selectors["calendar_apply_button"]).first
        expect(apply_button).to_be_visible(timeout=timeouts["calendar_apply_click"])
        apply_button.click()
        logging.info("Clicked 'Apply'. Calendar should close.")
        page.wait_for_timeout(timeouts["short_pause"]) # Wait for calendar to close and input update
        return True

    except Exception as e:
        logging.error(f"Error selecting date in calendar: {e}")
        safe_screenshot(page, "error_calendar_selection")
        return False

def attempt_download(page, button_selector, doc_name, save_dir):
    """Attempts to click a download button and save the file."""
    logging.info(f"Attempting download for '{doc_name}' using button: {button_selector}")
    timeouts = CONFIG["timeouts"]
    download_path = None
    suggested_filename = None # Initialize suggested_filename

    try:
        button = page.locator(button_selector).first # Assume first match is the one
        expect(button).to_be_visible(timeout=timeouts["element_wait"])
        expect(button).to_be_enabled(timeout=timeouts["button_enable"])
        logging.info(f"Download button '{button_selector}' is visible and enabled.")

        # Sanitize doc_name for filename base
        safe_filename_base = re.sub(r'[\\/*?:"<>|]', '_', doc_name)[:150]

        # Start waiting for download *before* clicking
        with page.expect_download(timeout=timeouts["download"]) as download_info:
            logging.info(f"Clicking download button: {button_selector}")
            try:
                 # Use force=True cautiously, might hide issues
                button.click(timeout=timeouts["element_wait"], force=True)
            except Exception as click_error:
                logging.warning(f"Standard click failed for {button_selector}: {click_error}. Trying JS click.")
                page.evaluate(f"document.querySelector('{button_selector}').click()")

        download = download_info.value
        suggested_filename = download.suggested_filename
        logging.info(f"Download started. Suggested filename: {suggested_filename}")

        # Determine final filename
        if suggested_filename and '.' in suggested_filename[-5:]: # Basic check for extension
            final_filename = suggested_filename
        elif '.' in safe_filename_base[-5:]:
             final_filename = safe_filename_base
        else:
            final_filename = safe_filename_base + ".pdf" # Default extension if unsure
            logging.warning(f"Could not determine file extension. Defaulting to '.pdf' for: {final_filename}")

        download_path = save_dir / final_filename
        download_path.parent.mkdir(parents=True, exist_ok=True) # Ensure dir exists

        logging.info(f"Saving download to: {download_path}")
        download.save_as(download_path)
        logging.info(f"Download saved successfully: {download_path}")
        return str(download_path) # Return path on success

    except PlaywrightTimeoutError as te:
         logging.error(f"Timeout error during download for '{doc_name}' using {button_selector}: {te}")
         if "expect_download" in str(te):
             logging.error("Timeout waiting for download event to trigger after click.")
         else:
             logging.error(f"Timeout waiting for button '{button_selector}' state.")
         safe_screenshot(page, f"error_download_timeout_{doc_name[:20]}")
    except Exception as e:
        logging.error(f"Error during download process for '{doc_name}' using {button_selector}: {e}")
        if "Target page, context or browser has been closed" in str(e):
             logging.warning("Browser closed during download. File might exist but wasn't saved via Playwright.")
             # Optionally return a placeholder path if needed downstream
             # return str(save_dir / f"possible_download_{safe_filename_base}")
        safe_screenshot(page, f"error_download_general_{doc_name[:20]}")
        # Clean up potentially partial file if path was determined
        if download_path and download_path.exists():
            try:
                download_path.unlink()
                logging.info(f"Removed potentially partial download: {download_path}")
            except OSError as unlink_e:
                logging.warning(f"Error removing partial download file {download_path}: {unlink_e}")

    return None # Return None on failure

def find_element_sequentially(page, selectors, description):
    """Tries a list of selectors sequentially and returns the first locator found."""
    logging.info(f"Trying to find {description} using selectors: {selectors}")
    for selector in selectors:
        try:
            element = page.locator(selector)
            if element.count() > 0:
                 # Perform a quick visibility check to prefer visible elements
                 if element.first.is_visible(timeout=1000):
                    logging.info(f"Found {description} with selector (visible): {selector}")
                    return element.first # Return the first locator object
                 else:
                     logging.info(f"Found {description} with selector (not visible): {selector}. Trying next.")
            # else:
            #     logging.debug(f"Selector did not match: {selector}")
        except Exception as e:
            # Ignore timeouts or other errors during the check loop for a single selector
            logging.debug(f"Check failed for selector {selector}: {e}")
            pass
    logging.warning(f"Could not find {description} using any provided selectors.")
    return None


def find_and_click_element(page, selectors, description, timeout=CONFIG["timeouts"]["element_wait"]):
    """Finds an element using multiple selectors and clicks it."""
    element = find_element_sequentially(page, selectors, description)
    if element:
        try:
            logging.info(f"Clicking {description}...")
            expect(element).to_be_visible(timeout=timeout)
            expect(element).to_be_enabled(timeout=timeout)
            element.click(timeout=timeout)
            logging.info(f"Clicked {description} successfully.")
            return True
        except Exception as e:
            logging.error(f"Error clicking {description}: {e}")
            safe_screenshot(page, f"error_click_{description}")
            # Try JS click as a fallback for stubborn elements
            try:
                logging.warning("Trying JavaScript click as fallback...")
                element.evaluate("el => el.click()")
                logging.info("JavaScript click executed.")
                return True
            except Exception as js_e:
                logging.error(f"JavaScript click also failed: {js_e}")
                return False
    return False


def get_document_details(page, index, table_selector):
    """Gets document name and determines table type and selectors for a given row index."""
    logging.info(f"Getting details for document row {index + 1} in table '{table_selector}'")
    selectors = CONFIG["selectors"]
    row_num = index + 1 # 1-based index for nth-child

    # Determine table type and base ID
    is_arquivo_processo_table = "arquivoProcessoTable" in table_selector
    base_id = table_selector.replace("#", "") # Get ID without the hash

    # --- Get Document Name ---
    doc_name = "unknown_document"
    try:
        if is_arquivo_processo_table:
            name_selector = selectors["doc_name_arquivo"].format(base_id=base_id, index=index)
            fallback_name_selector = selectors["doc_name_fallback_arquivo"].format(table_selector=table_selector, row_num=row_num)
        else:
            name_selector = selectors["doc_name_termo"].format(base_id=base_id, index=index)
            fallback_name_selector = selectors["doc_name_fallback_termo"].format(table_selector=table_selector, row_num=row_num)

        name_element = find_element_sequentially(page, [name_selector, fallback_name_selector], f"document name for row {row_num}")

        if name_element:
            doc_name = name_element.inner_text().strip()
            logging.info(f"Document name found: {doc_name}")
        else:
             logging.warning(f"Document name element not found for row {row_num}.")

    except Exception as e:
        logging.error(f"Error getting document name for row {row_num}: {e}")
        safe_screenshot(page, f"error_get_doc_name_{row_num}")
        doc_name = "error_getting_name"

    # --- Determine Radio/Cell Selector ---
    if is_arquivo_processo_table:
        radio_selector = selectors["doc_radio_cell_arquivo"].format(base_id=base_id, index=index)
    else:
        radio_selector = selectors["doc_radio_cell_termo"].format(base_id=base_id, index=index)

    fallback_radio_selectors = [
        selectors["doc_radio_fallback_img"].format(table_selector=table_selector, row_num=row_num),
        selectors["doc_radio_fallback_span_img"].format(table_selector=table_selector, row_num=row_num)
    ]
    all_radio_selectors = [radio_selector] + fallback_radio_selectors


    # --- Determine Download Button Selectors ---
    if is_arquivo_processo_table:
        primary_download_selector = selectors["doc_download_button_primary_arquivo"]
    else:
        primary_download_selector = selectors["doc_download_button_primary_termo"]

    all_download_selectors = [primary_download_selector] + selectors["doc_download_buttons_fallback"]

    return {
        "name": doc_name,
        "is_arquivo_processo": is_arquivo_processo_table,
        "radio_selectors": all_radio_selectors,
        "download_selectors": all_download_selectors
    }


def process_document_row(page, index, table_selector, process_dir, processed_doc_names):
    """Selects, downloads, and potentially deselects a document in a table row."""
    logging.info(f"--- Processing Document Row {index + 1} ---")
    timeouts = CONFIG["timeouts"]

    doc_details = get_document_details(page, index, table_selector)
    doc_name = doc_details["name"]
    is_arquivo_processo = doc_details["is_arquivo_processo"]
    radio_selectors = doc_details["radio_selectors"]
    download_selectors = doc_details["download_selectors"]

    if doc_name in processed_doc_names:
        logging.info(f"Skipping duplicate document: {doc_name}")
        return False # Indicate no download occurred
    processed_doc_names.add(doc_name)

    # --- Select Document Radio/Cell ---
    logging.info(f"Selecting document radio/cell for row {index + 1}")
    safe_screenshot(page, f"doc_{index+1}_before_select")
    # Use find_and_click_element helper
    radio_clicked = find_and_click_element(page, radio_selectors, f"document radio for row {index + 1}")

    if not radio_clicked:
        # Decide if this is critical. Maybe selection isn't needed?
        logging.warning(f"Could not click radio/cell for row {index + 1}. Proceeding to download attempt anyway.")
        # return False # Or decide to stop if selection is mandatory

    # Wait after clicking radio for potential AJAX updates
    logging.info("Waiting after radio click for potential updates...")
    page.wait_for_timeout(timeouts["short_pause"])
    try:
        # Wait for network idle briefly to catch AJAX calls
        page.wait_for_load_state('networkidle', timeout=5000)
    except PlaywrightTimeoutError:
        logging.warning("Network did not become idle after radio click, continuing...")


    # --- Attempt Download ---
    logging.info(f"Attempting download for document: {doc_name}")
    downloaded_path = None
    for btn_selector in download_selectors:
        downloaded_path = attempt_download(page, btn_selector, doc_name, process_dir)
        if downloaded_path:
            logging.info(f"Successfully downloaded '{doc_name}' using selector: {btn_selector}")
            break # Exit loop on successful download
        else:
            logging.info(f"Download attempt failed with selector: {btn_selector}. Trying next.")

    if not downloaded_path:
        logging.error(f"Failed to download document: {doc_name} after trying all selectors.")
        safe_screenshot(page, f"failed_download_{doc_name[:20]}_{index + 1}")
        # Decide whether to continue processing other documents or stop

    # --- Deselect Radio (if needed for specific table type) ---
    if is_arquivo_processo and radio_clicked: # Only deselect if it's the special table AND we successfully clicked it initially
        logging.info(f"Deselecting radio button for document {index + 1} (arquivoProcessoTable special case)")
        # We need to click the *same* element again to deselect
        deselected = find_and_click_element(page, radio_selectors, f"document radio DESELECT for row {index + 1}")
        if deselected:
            logging.info(f"Deselected radio for row {index + 1}. Waiting for updates.")
            page.wait_for_timeout(2000)  # 2-second delay after deselecting radio button
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except PlaywrightTimeoutError:
                 logging.warning("Network did not become idle after radio deselect click, continuing...")
        else:
            logging.warning(f"Failed to deselect radio for row {index + 1}. This might affect subsequent rows.")
            safe_screenshot(page, f"error_deselect_radio_{index + 1}")

    return downloaded_path is not None # Return True if download was successful


def create_zip_archive(source_dir, process_id):
    """Creates a ZIP archive from downloaded files for a process"""
    if not source_dir.exists() or not any(source_dir.iterdir()):
        logger.warning(f"No files found to zip in {source_dir}")
        return None
    
    # Create ZIP filename with CAGECE prefix and process_id
    # Use the process_id directly, not the generic identifier
    # This ensures the ZIP has a real processo number in the filename
    zip_filename = f"CAGECE_{process_id}.zip"
    archive_path = CONFIG["archives_dir"] / zip_filename
    
    try:
        # Ensure the archive directory exists
        CONFIG["archives_dir"].mkdir(parents=True, exist_ok=True)
        
        # Create the ZIP file
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in source_dir.glob('**/*'):
                if file_path.is_file():
                    # Calculate relative path for the file inside the ZIP
                    relative_path = file_path.relative_to(source_dir)
                    zipf.write(file_path, arcname=relative_path)
                    logger.info(f"Added {relative_path} to {zip_filename}")
        
        logger.info(f"Created archive {archive_path} with files from {source_dir}")
        return archive_path
    except Exception as e:
        logger.error(f"Error creating ZIP archive {archive_path}: {e}")
        return None

def return_to_search_results(page):
    """Helper function to return to search results page from anywhere."""
    logger.info("Attempting to return to search results page...")
    selectors = CONFIG["selectors"]
    timeouts = CONFIG["timeouts"]
    
    # Try to find and click the return button
    returned = find_and_click_element(page, selectors["return_button_selectors"], "Return to Search button")

    if returned:
        try:
            logging.info("Waiting for search results page to reload after return click...")
            page.wait_for_load_state('networkidle', timeout=timeouts["return_navigation"])
            expect(page.locator(selectors["search_button"])).to_be_visible(timeout=timeouts["return_element_wait"])
            logging.info("Successfully returned to search results page via button.")
            page.wait_for_timeout(timeouts["short_pause"])
            safe_screenshot(page, "after_return_to_search")
            return True
        except Exception as e:
            logging.error(f"Error confirming return navigation: {e}")
            # Continue to fallback even if confirmation fails

    # Fallback: Browser back if button click failed
    if not returned:
        logging.warning("Return button failed. Trying browser back navigation.")
        try:
            page.go_back(wait_until="networkidle", timeout=timeouts["return_navigation"])
            expect(page.locator(selectors["search_button"])).to_be_visible(timeout=timeouts["return_element_wait"])
            logging.info("Successfully returned using browser back.")
            return True
        except Exception as e_back:
            logging.error(f"Browser back navigation also failed: {e_back}")

    # Final Fallback: Direct navigation
    try:
        page.goto(CONFIG["target_url"], wait_until="networkidle", timeout=timeouts["navigation"])
        logging.info("Successfully navigated directly to search page as last resort.")
        return True
    except Exception as e_nav:
        logging.critical(f"FATAL: Could not return to search page: {e_nav}")
        return False


def process_details_page(page, process_id):
    """Handles actions on the details page: finding table, processing documents, returning."""
    logger.info(f"--- Processing Details Page for Process ID: {process_id} ---")
    selectors = CONFIG["selectors"]
    timeouts = CONFIG["timeouts"]
    
    # For recovery calls
    if process_id == "recovery":
        return return_to_search_results(page)
    
    process_dir = CONFIG["temp_dir"] / process_id
    process_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Current URL: {page.url}")
    logger.info(f"Page title: {page.title()}")
    safe_screenshot(page, f"details_page_{process_id}")
    
    processing_success = False  # Track if processing was successful
    start_time = time.time()  # Record start time for timeout tracking
    last_activity_time = start_time  # Track time of last successful activity

    # Find the document table
    doc_table = find_element_sequentially(page, selectors["doc_tables"], "document table")

    if not doc_table:
        logger.warning("No document table found on details page. Skipping download.")
    else:
        # Get the selector that worked (needed for row processing)
        # This requires knowing which selector matched - find_element_sequentially needs modification
        # For now, let's assume the first selector in the list if found, or handle based on known IDs.
        # A better way: find_element_sequentially could return the locator AND the selector string.
        # Simplified approach: Re-check which selector exists.
        found_table_selector = None
        for sel in selectors["doc_tables"]:
             if page.locator(sel).count() > 0:
                 found_table_selector = sel
                 break

        if not found_table_selector:
             logger.error("Found a table element, but could not determine its selector. Cannot process rows.")
        else:
            logger.info(f"Using document table selector: {found_table_selector}")

            # Save table HTML for debugging
            try:
                table_html = doc_table.evaluate("el => el.outerHTML")
                with open(process_dir / "document_table.html", "w", encoding="utf-8") as f:
                    f.write(table_html)
                last_activity_time = time.time()  # Update activity time
            except Exception as e:
                logger.warning(f"Could not save document table HTML: {e}")

            # Count rows - use a more specific selector to avoid counting header/footer rows
            rows_selector = f"{found_table_selector} > tbody > tr:not(.rich-table-header):not(.rich-table-footer)"
            rows = page.locator(rows_selector)
            rows_count = rows.count()
            logger.info(f"Found {rows_count} document rows in the table.")

            if rows_count > 0:
                processed_doc_names = set()
                downloads_succeeded = 0
                
                for i in range(rows_count):
                    # Check for timeout before processing each row
                    current_time = time.time()
                    time_since_activity = current_time - last_activity_time
                    if time_since_activity > DETAILS_PAGE_TIMEOUT:
                        logger.warning(f"Timeout detected! No activity for {time_since_activity:.1f} seconds (threshold: {DETAILS_PAGE_TIMEOUT}s)")
                        break
                        
                    # Try to process row
                    if process_document_row(page, i, found_table_selector, process_dir, processed_doc_names):
                        downloads_succeeded += 1
                        last_activity_time = time.time()  # Update activity time after successful download
                    
                logger.info(f"Attempted to download {downloads_succeeded}/{rows_count} unique documents.")
                
                # Set processing_success = True only if we successfully created the archive
                if downloads_succeeded > 0:
                    archive_path = create_zip_archive(process_dir, process_id)
                    if archive_path:
                        logger.info(f"Successfully created archive: {archive_path}")
                        processing_success = True  # Mark as successful
                        last_activity_time = time.time()  # Update activity time
                    else:
                        logger.error(f"Failed to create archive for process ID: {process_id}")
            else:
                logger.info("No document rows found to process.")

    # Check for timeout before returning to search results
    current_time = time.time()
    time_since_activity = current_time - last_activity_time
    if time_since_activity > DETAILS_PAGE_TIMEOUT:
        logger.warning(f"Timeout detected before return! No activity for {time_since_activity:.1f} seconds (threshold: {DETAILS_PAGE_TIMEOUT}s)")
        return False
    
    # Return to search results
    return_success = return_to_search_results(page)
    if return_success:
        last_activity_time = time.time()  # Update activity time after successful return
    
    # Final timeout check
    current_time = time.time()
    time_since_activity = current_time - last_activity_time
    if time_since_activity > DETAILS_PAGE_TIMEOUT:
        logger.warning(f"Timeout detected after return attempt! No activity for {time_since_activity:.1f} seconds (threshold: {DETAILS_PAGE_TIMEOUT}s)")
        return False
    
    # Return True only if both processing and navigation were successful
    return processing_success and return_success


def extract_process_id(row_text):
    """
    Extracts process ID from row text with proper format matching.
    Returns both the numeric part (for storage/comparison) and the full ID (for display).
    """
    # First look specifically in the N° DA PUBLICAÇÃO column format (YYYY/NNNNN)
    # This is the proper publication ID format we see in the table
    publication_id_match = re.search(r'\b(20\d{2}/\d{5})\b', row_text)
    if publication_id_match:
        full_id = publication_id_match.group(1)
        # Extract only the numeric part after the slash for storage/comparison
        numeric_part = full_id.split('/')[1]
        logger.info(f"Extracted publication ID: {full_id} (storing as: {numeric_part})")
        return numeric_part, full_id
    
    # As a fallback, look for N° PROCESSO column value which appears to be a 16-digit number
    # But we should avoid this if possible as it's not the unique identifier we want
    process_match = re.search(r'\b(\d{16})\b', row_text)
    if process_match:
        logger.warning(f"Could not find publication ID, using proceso number instead")
        return process_match.group(1), None
    
    # No valid ID found
    logger.error(f"Could not extract any valid process ID from row text")
    return None, None


def get_search_results_count(page):
    """
    Extracts the total number of rows from the pagination display.
    Returns the count as an integer, or None if extraction fails.
    """
    logger.info("Extracting search results count from pagination...")
    
    try:
        # Look for the pagination div with the result count
        pagination_selector = "div.numeracaoPagina span#formularioDeCrud\\:outputNumeracaoInferior"
        pagination_element = page.locator(pagination_selector)
        
        if pagination_element.count() > 0:
            pagination_text = pagination_element.inner_text()
            logger.info(f"Found pagination text: '{pagination_text}'")
            
            # Extract the total count using regex (format: "1 a X de Y")
            match = re.search(r'de\s+(\d+)', pagination_text)
            
            if match:
                total_count = int(match.group(1))
                logger.info(f"Total number of results: {total_count}")
                return total_count
            else:
                logger.warning(f"Could not extract total count from pagination text: '{pagination_text}'")
        else:
            logger.warning("Pagination element not found. Search might have returned no results.")
            
        return None
            
    except Exception as e:
        logger.error(f"Error extracting search results count: {e}")
        safe_screenshot(page, "error_extracting_count")
        return None


def process_search_results(page, processed_ids=None):
    """Processes rows in the search results table."""
    logger.info("--- Processing Search Results ---")
    selectors = CONFIG["selectors"]
    timeouts = CONFIG["timeouts"]
    
    # Load processed IDs if not provided
    if processed_ids is None:
        processed_ids = load_processed_ids_json()
    
    # Track IDs processed in this session to prevent reprocessing within a run
    session_processed_ids = set()

    # Wait for results to appear
    logger.info("Waiting for search results to appear...")
    try:
        page.wait_for_selector(
            f"{selectors['results_table']}, {selectors['visualize_button']}",
            state="visible",
            timeout=timeouts["search_results"]
        )
        logger.info("Results table or visualize button detected.")
        page.wait_for_timeout(timeouts["short_pause"]) # Allow rendering
        safe_screenshot(page, "search_results_visible")

        # Wait for AJAX updates to complete
        logger.info("Waiting 3 seconds before extracting search results count...")
        page.wait_for_timeout(3000)  # 3 seconds in milliseconds

        # Get the total results count from pagination
        total_results_count = get_search_results_count(page)
        if total_results_count is not None:
            logger.info(f"TOTAL SEARCH RESULTS: {total_results_count}")
        else:
            logger.warning("Could not determine total search results count from pagination.")
            # If we can't get count, continue but without a specific limit
            total_results_count = float('inf')  # Process indefinitely if count unknown

    except PlaywrightTimeoutError:
        logger.warning("Timeout waiting for results table or visualize button.")
        safe_screenshot(page, "search_results_timeout")
        # Continue anyway - don't exit early
        total_results_count = float('inf')  # Process indefinitely if timeout
    
    # Counter for processed rows
    processed_row_count = 0
    
    logger.info(f"Will process exactly {total_results_count} rows from search results")

    # --- Try Specific Radio Button First (as per original logic) ---
    logger.info(f"Checking for specific radio button: {selectors['results_row_radio_img_exact']}")
    exact_radio = page.locator(selectors["results_row_radio_img_exact"])
    exact_radio_processed = False
    
    if exact_radio.count() > 0 and processed_row_count < total_results_count:
        logger.info("Found specific radio button. Attempting to process it.")
        try:
            safe_screenshot(page, "before_exact_radio_click")
            expect(exact_radio.first).to_be_visible(timeout=timeouts["element_wait"])
            exact_radio.first.scroll_into_view_if_needed()
            exact_radio.first.click()
            logger.info("Clicked the specific radio button.")
            page.wait_for_timeout(timeouts["radio_click_wait"]) # Wait for AJAX
            safe_screenshot(page, "after_exact_radio_click")

            # Wait for Visualizar button to enable and click it
            logger.info("Waiting for Visualizar button to enable after specific radio click...")
            viz_button = page.locator(selectors["visualize_button_enabled"]).first
            expect(viz_button).to_be_enabled(timeout=timeouts["button_enable"])
            expect(viz_button).to_be_visible(timeout=timeouts["element_wait"])
            logger.info("Visualizar button is enabled. Clicking...")
            safe_screenshot(page, "before_visualizar_click_exact")
            viz_button.click()

            # Wait for details page load
            logger.info("Waiting for details page to load (network idle)...")
            page.wait_for_load_state('networkidle', timeout=timeouts["details_load"])
            logger.info("Details page network idle. Waiting for rendering...")
            page.wait_for_timeout(timeouts["details_render_wait"]) # Extra wait

            # Extract process ID from the page if possible
            try:
                # Attempt to find a process ID in the page content
                # Look for both process number and publication ID in the page content
                page_text = page.content()
                process_id, full_id = extract_process_id(page_text)
                
                if process_id:
                    if full_id:
                        logger.info(f"Found publication ID: {full_id} (storing as: {process_id}) on details page")
                    else:
                        logger.info(f"Found processo number: {process_id} on details page")
                else:
                    # Fallback to older method if extract_process_id fails
                    process_id_element = page.locator('text=Processo').first
                    if process_id_element.count() > 0:
                        process_text = process_id_element.inner_text()
                        # Extract process ID using regex
                        process_id_match = re.search(r'\d+', process_text)
                        if process_id_match:
                            process_id = process_id_match.group(0)
                            logger.info(f"Found processo number: {process_id} on details page")
                        else:
                            process_id = f"exact_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                            logger.warning(f"Could not extract processo number from text, using temporary ID: {process_id}")
                    else:
                        process_id = f"exact_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        logger.warning(f"Could not find processo element, using temporary ID: {process_id}")
            except Exception as e:
                logger.warning(f"Could not extract process ID: {e}")
                process_id = f"exact_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                
            # Process the details page with the extracted or generated ID
            successful = process_details_page(page, process_id)
            
            # Only save to JSON and increment count if processing was successful
            if successful:
                if re.match(r'^\d+$', process_id):
                    save_processed_id_json(process_id)  # Save to JSON
                    session_processed_ids.add(process_id)  # Mark as processed in this session
                else:
                    logger.info(f"Not saving temporary ID {process_id} to processed list")
                
                # Increment the processed count if successful
                processed_row_count += 1
                logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                
                exact_radio_processed = True # Mark as processed
                
                # Check if we've reached our target
                if processed_row_count >= total_results_count:
                    logger.info(f"Reached target of {total_results_count} processed rows. Processing complete!")
                    return
            else:
                logger.warning(f"Failed to process specific radio button ID: {process_id}. Not counting toward total.")

        except Exception as e:
            logger.error(f"Error processing specific radio button: {e}")
            safe_screenshot(page, "error_exact_radio_process")
            # Continue to generic processing even on error
            if "process_details_page" not in str(e): # Avoid double return if error was in details processing
                 logger.warning("Attempting to return to search page after specific radio error...")
                 if not return_to_search_results(page): # Use the dedicated return function
                      logger.error("Failed to return to search results after specific radio error.")
                      # Continue to generic processing anyway

    # --- Process Generic Radio Buttons ---
    while processed_row_count < total_results_count:
        logger.info("Processing generic result rows...")
        # Get current visible radio buttons
        radio_images = page.locator(selectors["results_row_radio_img_generic"])
        visible_rows = radio_images.count()
        logger.info(f"Found {visible_rows} visible rows with clickable radio buttons")

        # If no rows visible but we haven't reached our target, try page navigation or other approaches
        if visible_rows == 0:
            logger.warning("No more radio buttons visible, but target count not reached.")
            logger.warning(f"Processed {processed_row_count}/{total_results_count} rows so far.")
            # TODO: Add pagination navigation here if the site supports it
            logger.warning("Attempting to continue anyway...")
            
            # Check for alternative button types or other navigation options
            # This is where you would add code to click "next page" or try alternative methods
            # For now, we'll pause and retry once more before giving up
            page.wait_for_timeout(5000)
            
            # Re-check for radio buttons after pause
            radio_images = page.locator(selectors["results_row_radio_img_generic"])
            visible_rows = radio_images.count()
            
            if visible_rows == 0:
                logger.error("Still no radio buttons found after retry. Cannot reach target count.")
                break  # Exit loop only if we've tried everything and still can't find rows

        # Process visible rows
        for i in range(visible_rows):
            # Skip if we've already processed this index in a previous batch
            if processed_row_count >= total_results_count:
                logger.info(f"Reached target of {total_results_count} processed rows. Processing complete!")
                return
                
            logger.info(f"--- Processing Generic Row {i+1}/{visible_rows} ({processed_row_count+1}/{total_results_count} total) ---")
            
            try:
                # Re-locate the element in each iteration to avoid staleness
                current_radio_img = page.locator(selectors["results_row_radio_img_generic"]).nth(i)
                    
                # Try to extract process ID from the row before clicking
                try:
                    # Get the row containing this radio button
                    row = current_radio_img.locator('xpath=ancestor::tr').first
                    # Try to find process ID in the row text
                    row_text = row.inner_text()
                    
                    # Use the new extract_process_id function
                    process_id, full_id = extract_process_id(row_text)
                    
                    if process_id:
                        # Check if already processed in JSON OR in this session
                        if process_id in processed_ids:
                            if full_id:
                                logger.info(f"Publication ID {full_id} (ID: {process_id}) already in JSON, skipping.")
                            else:
                                logger.info(f"Process ID {process_id} already in JSON, skipping.")
                            processed_row_count += 1  # Still count as processed
                            logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                            continue
                            
                        if process_id in session_processed_ids:
                            if full_id:
                                logger.info(f"Publication ID {full_id} (ID: {process_id}) already processed in this session, skipping.")
                            else:
                                logger.info(f"Process ID {process_id} already processed in this session, skipping.")
                            processed_row_count += 1 
                            logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                            continue
                        
                        if full_id:
                            logger.info(f"Found publication ID: {full_id} (storing as: {process_id}) in row {i+1}")
                        else:    
                            logger.info(f"Found process ID: {process_id} in row {i+1}")
                    else:
                        # Fallback to old method if extract_process_id fails
                        id_match = re.search(r'\b\d{5,}\b', row_text)
                        if id_match:
                            process_id = id_match.group(0)
                            
                            # Check if already processed in JSON OR in this session
                            if process_id in processed_ids:
                                logger.info(f"Process ID {process_id} already in JSON, skipping.")
                                processed_row_count += 1  # Still count as processed
                                logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                                continue
                                
                            if process_id in session_processed_ids:
                                logger.info(f"Process ID {process_id} already processed in this session, skipping.")
                                processed_row_count += 1 
                                logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                                continue
                                
                            logger.info(f"Found process ID: {process_id} in row {i+1} (using fallback method)")
                        else:
                            # Generate a temporary ID that includes row number
                            process_id = f"row{i+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                            logger.warning(f"Could not extract processo number from row {i+1}, using temporary ID: {process_id}")
                except Exception as e:
                    logger.warning(f"Could not extract process ID from row {i+1}: {e}")
                    # Generate a temporary ID that includes row number
                    process_id = f"row{i+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                
                logger.info(f"Locating and clicking radio image for row {i+1}...")
                current_radio_img.scroll_into_view_if_needed()
                expect(current_radio_img).to_be_visible(timeout=timeouts["element_wait"])
                current_radio_img.click()
                logger.info(f"Clicked radio image for row {i+1}.")
                page.wait_for_timeout(timeouts["radio_click_wait"]) # Wait for AJAX

                # Wait for and click the Visualizar button
                logging.info(f"Waiting for Visualizar button to be enabled for row {i+1}...")
                viz_button = page.locator(selectors["visualize_button_enabled"]).first
                expect(viz_button).to_be_enabled(timeout=timeouts["button_enable"])
                expect(viz_button).to_be_visible(timeout=timeouts["element_wait"])
                logging.info("Visualizar button enabled. Clicking...")
                safe_screenshot(page, f"before_visualizar_click_generic_{i+1}")
                viz_button.click()

                # Wait for details page load
                logger.info("Waiting for details page to load (network idle)...")
                page.wait_for_load_state('networkidle', timeout=timeouts["details_load"])
                logger.info("Details page network idle. Waiting for rendering...")
                page.wait_for_timeout(timeouts["details_render_wait"]) # Extra wait

                # Process the details page
                successful = process_details_page(page, process_id)
                
                # Only save to JSON and increment count if processing was successful
                if successful:
                    if re.match(r'^\d+$', process_id):
                        save_processed_id_json(process_id)  # Save to JSON
                        session_processed_ids.add(process_id)  # Mark as processed in this session
                    else:
                        logger.info(f"Not saving temporary ID {process_id} to processed list")
                    
                    # After successful processing, increment counter
                    processed_row_count += 1
                    logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                    
                    # Check if we've reached our target
                    if processed_row_count >= total_results_count:
                        logger.info(f"Reached target of {total_results_count} processed rows. Processing complete!")
                        return
                else:
                    logger.warning(f"Failed to process {process_id}. Not counting toward total.")

            except PlaywrightTimeoutError as te:
                logger.error(f"TimeoutError processing generic row {i+1}: {te}")
                safe_screenshot(page, f"timeout_error_row_{i+1}")
                logger.warning("Attempting to recover and continue...")
                # Attempt to recover by returning to search page if possible
                if not return_to_search_results(page): # Use the dedicated return function
                     logger.critical("Failed to return to search results after timeout. Stopping.")
                     break # Stop if we can't get back

            except Exception as e:
                logger.error(f"An error occurred processing generic row {i+1}: {type(e).__name__} - {e}")
                safe_screenshot(page, f"general_error_row_{i+1}")
                logger.warning("Attempting to recover and continue to the next row...")
                # Try to return to search page to salvage subsequent rows
                if "process_details_page" not in str(e): # Avoid double return
                     logger.warning("Attempting to return to search page after generic row error...")
                     if not return_to_search_results(page): # Use the dedicated return function
                         logger.critical("Failed to return to search results after error. Stopping.")
                         break # Stop if we can't get back

                # If return was successful (or error occurred before navigation), continue loop
                logger.info("Continuing to the next row...")
        
        # If we've processed all rows but haven't reached our target,
        # check if any are left to process
        all_ids_already_processed = True
        for i in range(visible_rows):
            try:
                current_radio = page.locator(selectors["results_row_radio_img_generic"]).nth(i)
                row = current_radio.locator('xpath=ancestor::tr').first
                row_text = row.inner_text()
                
                # Use the new extract_process_id function for consistency
                process_id, _ = extract_process_id(row_text)
                
                if process_id and process_id not in processed_ids and process_id not in session_processed_ids:
                    all_ids_already_processed = False
                    break
                    
                # Fallback to old method if extract_process_id fails
                if not process_id:
                    id_match = re.search(r'\b\d{5,}\b', row_text)
                    if id_match:
                        process_id = id_match.group(0)
                        if process_id not in processed_ids and process_id not in session_processed_ids:
                            all_ids_already_processed = False
                            break
            except:
                pass
                
        if all_ids_already_processed:
            logger.info("All visible rows are already processed. Exiting loop.")
            break
            
    logger.info(f"Finished processing search results. Processed {processed_row_count}/{total_results_count} rows.")


# --- Main Script Execution ---
def process_keyword(keyword):
    """Process a single keyword search."""
    logging.info(f"\n\n--- PROCESSING KEYWORD: {keyword} ---\n")
    
    # Reload processed IDs before processing each keyword
    processed_ids = load_processed_ids_json()
    logging.info(f"Reloaded {len(processed_ids)} processed IDs before processing keyword '{keyword}'")
    
    # Handle PAM keyword to match exactly as a whole word
    search_keyword = keyword
    if keyword == "PAM":
        # Use space before and after to ensure exact match
        search_keyword = f" {keyword} "
        logging.info(f"Modified search keyword '{keyword}' to use spaces around: {search_keyword}")
    
    # Save the keyword to the config
    CONFIG["search_params"]["object_keyword"] = search_keyword
    
    with sync_playwright() as p:
        browser = None
        context = None
        page = None
        try:
            logging.info("Launching browser...")
            browser = p.chromium.launch(
                headless=CONFIG["headless_mode"],
                args=[
                    '--start-maximized',
                    '--disable-web-security', # May be needed for certain interactions/downloads
                    '--disable-features=IsolateOrigins,site-per-process', # Stability flags
                    '--disable-site-isolation-trials'
                ]
            )
            logging.info("Browser launched.")

            context = browser.new_context(
                no_viewport=True, # Use browser's full window size when maximized
                ignore_https_errors=True,
                accept_downloads=True,
                # Set default timeouts for the context
                # viewport={'width': 1920, 'height': 1080}, # Set viewport if not using maximized/no_viewport
                java_script_enabled=True,
                bypass_csp=True # May help with content loading issues
            )
            context.set_default_timeout(CONFIG["timeouts"]["default_page"])
            context.set_default_navigation_timeout(CONFIG["timeouts"]["navigation"])
            logging.info("Browser context created.")

            page = context.new_page()
            logging.info(f"Navigating to: {CONFIG['target_url']}")
            page.goto(CONFIG["target_url"], wait_until="domcontentloaded") # Wait for DOM first
            logging.info("Initial page load (DOM content loaded). Waiting for network idle...")
            # Add a wait for network idle after initial load
            page.wait_for_load_state('networkidle', timeout=CONFIG["timeouts"]["network_idle"])
            logging.info("Page navigation complete and network idle.")
            safe_screenshot(page, f"page_loaded_{keyword}")

            # --- Wait for Form ---
            logging.info("Waiting for search form elements to be ready...")
            expect(page.locator(CONFIG["selectors"]["main_form"])).to_be_visible(timeout=CONFIG["timeouts"]["element_wait"])
            expect(page.locator(CONFIG["selectors"]["organization_dropdown"])).to_be_enabled(timeout=CONFIG["timeouts"]["element_wait"])
            logging.info("Search form is ready.")

            # --- Fill Search Form ---
            logging.info("Filling search form...")
            search_params = CONFIG["search_params"]
            selectors = CONFIG["selectors"]
            timeouts = CONFIG["timeouts"]

            # 1. Organization
            select_dropdown_option_by_label(page, selectors["organization_dropdown"], search_params["organization_label"])

            # 2. Acquisition Nature
            select_dropdown_option_by_label(page, selectors["acquisition_nature_dropdown"], search_params["acquisition_nature_label"])

            # 3. Start Date Calendar
            logging.info("Setting Start Date using calendar...")
            date_params = search_params['start_date']
            calendar_button = page.locator(selectors["start_date_button"])
            if calendar_button.count() > 0:
                 calendar_button.click()
                 page.wait_for_timeout(500) # Brief pause for calendar to open
                 select_date_in_calendar(
                     page,
                     day=date_params['day'],
                     target_month_index=date_params['month'],
                     target_year=date_params['year']
                 )
            else:
                 logging.warning("Start date calendar button not found. Skipping date selection.")


            # 4. Object Keyword
            logging.info(f"Entering Object Keyword: {search_params['object_keyword']}")
            object_input = page.locator(selectors["object_input"])
            filled_object = False
            if object_input.count() > 0:
                 try:
                     expect(object_input).to_be_visible(timeout=timeouts["element_wait"])
                     object_input.fill(search_params["object_keyword"])
                     logging.info("Filled object keyword using primary input selector.")
                     filled_object = True
                     page.wait_for_timeout(500)
                 except Exception as e:
                     logging.warning(f"Primary object input selector {selectors['object_input']} failed: {e}. Trying fallback.")

            if not filled_object:
                 logging.info("Trying object keyword textarea fallback...")
                 textarea_fallback = page.locator(selectors["object_input_fallback"])
                 if textarea_fallback.count() > 0:
                     try:
                         expect(textarea_fallback.first).to_be_visible(timeout=timeouts["element_wait"])
                         textarea_fallback.first.fill(search_params["object_keyword"])
                         logging.info("Filled object keyword using textarea fallback.")
                         filled_object = True
                         page.wait_for_timeout(500)
                     except Exception as fallback_e:
                         logging.error(f"Textarea fallback for object keyword also failed: {fallback_e}")
                 else:
                     logging.error("Object keyword textarea fallback selector not found.")

            if not filled_object:
                 raise Exception("Critical Error: Could not fill the Object Keyword field.")

            logging.info("Search form filled.")
            safe_screenshot(page, f"form_filled_{keyword}")

            # --- Execute Search ---
            logging.info("Clicking search button...")
            # Use the helper function to find and click
            search_clicked = find_and_click_element(
                page,
                [selectors["search_button"], selectors["search_button_fallback"]],
                "Search button"
            )

            if not search_clicked:
                 raise Exception("Failed to click the search button.")

            # --- Process Results ---
            process_search_results(page, processed_ids)

            logger.info(f"Finished processing keyword: {keyword}")

        except PlaywrightTimeoutError as pte:
             logging.critical(f"A Playwright timeout error occurred for keyword {keyword}: {pte}")
             if page: safe_screenshot(page, f"error_timeout_state_{keyword}")
        except Exception as e:
            logging.critical(f"An unexpected error occurred during automation for keyword {keyword}: {e}", exc_info=True)
            if page: safe_screenshot(page, f"error_unexpected_state_{keyword}")
        finally:
            # --- Cleanup ---
            logging.info(f"Cleaning up for keyword: {keyword}...")
            if page and CONFIG["headless_mode"] is False and not page.is_closed():
                 try:
                     # Keep browser open briefly in headful mode for final inspection
                     logging.info("Keeping browser open for 10 seconds before closing (headful mode)...")
                     page.wait_for_timeout(10000)
                 except Exception:
                      pass # Ignore errors if page is already closing

            if context:
                try:
                    context.close()
                    logging.info("Browser context closed.")
                except Exception as e:
                    logging.warning(f"Error closing context: {e}")
            if browser:
                try:
                    browser.close()
                    logging.info("Browser closed.")
                except Exception as e:
                    logging.warning(f"Error closing browser: {e}")
            logging.info(f"Cleanup complete for keyword: {keyword}.")

def main():
    """Main function to run the web scraping process for multiple keywords."""
    # Ensure all output directories exist
    setup_output_dirs()
    
    logger.info(f"Script started. Downloads will be saved to: {CONFIG['downloads_dir']}")
    logger.info(f"Screenshots enabled: {CONFIG['take_screenshots']}")
    logger.info(f"Headless mode: {CONFIG['headless_mode']}")
    logger.info(f"Details page timeout: {DETAILS_PAGE_TIMEOUT} seconds")
    logger.info(f"Will process {len(KEYWORDS)} keywords: {KEYWORDS}")

    # Process each keyword in the list
    for keyword in KEYWORDS:
        try:
            process_keyword(keyword)
        except Exception as e:
            logger.error(f"Failed to process keyword '{keyword}': {e}")
            # Continue with next keyword even if this one fails
    
    logger.info("Script finished processing all keywords.")


if __name__ == "__main__":
    main()
