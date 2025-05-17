#!/usr/bin/env python3
"""
CAGECE Download Handler for Procurement Download System

This handler automates the download of procurement documents from CAGECE's procurement portal
by navigating through their web interface using Playwright.

The handler implements a standard interface for the procurement system:
- Takes company_id, output_dir, keywords, notion_database_id parameters
- Returns a standardized JSON structure with processed tenders
- Downloads procurement documents and creates ZIP archives
"""

import os
import sys
import time
import re
import json
import logging
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Any, Tuple

# Import Playwright components with error handling
try:
    from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print(json.dumps({
        "success": False,
        "company_id": "CAGECE",
        "new_tenders_processed": [],
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)


def setup_logging(log_dir_path: str, company_id: str) -> logging.Logger:
    """
    Configures and returns a logger for the handler.
    
    Args:
        log_dir_path: Directory where logs should be saved
        company_id: Company identifier for log file naming
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(f"CAGECEHandler_{company_id}")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to prevent duplicate logging
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Ensure log directory exists
    os.makedirs(log_dir_path, exist_ok=True)
    
    # Create log file with timestamp
    log_file = f"dynamic_handler_cagece_{company_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(log_dir_path, log_file)
    
    # Create file handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized for CAGECE handler (Company ID: {company_id})")
    return logger


def get_dynamic_config(base_output_dir: str, company_id: str, search_keyword_override: str = "") -> Dict[str, Any]:
    """
    Generates the dynamic configuration dictionary based on the output directory.
    
    Args:
        base_output_dir: Base path for all output files
        company_id: Company identifier
        search_keyword_override: Keyword to use for search (optional)
        
    Returns:
        Configuration dictionary
    """
    # Convert string path to Path object for consistent handling
    downloads_dir = Path(base_output_dir)
    
    return {
        "target_url": "https://s2gpr.sefaz.ce.gov.br/licita-web/paginas/licita/PublicacaoList.seam",
        "base_output_dir": str(downloads_dir),
        "pdfs_dir": str(downloads_dir / "pdfs"),
        "archives_dir": str(downloads_dir / "archives"),
        "temp_dir": str(downloads_dir / "temp"),
        "debug_dir": str(downloads_dir / "debug"),
        "logs_dir": str(downloads_dir / "logs"),
        "screenshots_dir": str(downloads_dir / "debug" / "screenshots"),
        "processed_ids_json": str(downloads_dir / f"processed_{company_id}.json"),
        "take_screenshots": True,
        "headless_mode": True,  # Default to headless mode for production
        "search_params": {
            "organization_label": "COMPANHIA DE AGUA E ESGOTO DO CEARA",
            "acquisition_nature_label": "EQUIPAMENTOS E MATERIAL PERMANENTE",
            "start_date": {
                "day": 1,
                "month": 0,  # 0-based month (0 = January)
                "year": datetime.now().year  # Default to current year
            },
            "object_keyword": search_keyword_override  # Keyword to search
        },
        # Additional configuration parameters remain the same
        "selectors": {
            # Main Form & Search Fields
            "main_form": "#formularioDeCrud",
            "organization_dropdown": "#formularioDeCrud\:promotorCotacaoDecoration\:promotorLicitacao",
            "acquisition_nature_dropdown": "#formularioDeCrud\:naturezaAquisicaoDecoration\:naturezaAquisicao",
            "object_input": "#formularioDeCrud\:objetoContratacaoDecoration\:objetoContratacao",
            "object_input_fallback": '#formularioDeCrud textarea',
            "search_button": '#formularioDeCrud\:pesquisar',
            "search_button_fallback": 'input[value="Pesquisar"]',
            "start_date_input": "#formularioDeCrud\:inicioAcolhimentoDecoration\:inicioAcolhimentoPropostasInputDate",
            "start_date_button": "#formularioDeCrud\:inicioAcolhimentoDecoration\:inicioAcolhimentoPropostasPopupButton",
            "calendar_popup": 'table.rich-calendar-popup[style*="z-index"]',
            "calendar_header": 'td.rich-calendar-header',
            "prev_year_button": 'div[onclick*="prevYear"]',
            "next_year_button": 'div[onclick*="nextYear"]',
            "prev_month_button": 'div[onclick*="prevMonth"]',
            "next_month_button": 'div[onclick*="nextMonth"]',
            "calendar_day_cell": 'td.rich-calendar-cell:not(.rich-calendar-boundary-dates)',
            "calendar_apply_button": 'div.rich-calendar-tool-btn:has-text("Apply")',
            "results_table": '#formularioDeCrud\:pagedDataTable',
            "results_row_radio_img_generic": "table#formularioDeCrud\:pagedDataTable tbody tr td.primeiraColuna img[style*='cursor:pointer']",
            "visualize_button": 'input[value="Visualizar"]',
            "visualize_button_enabled": 'input[value="Visualizar"]:not([disabled])',
            "doc_tables": [
                '#formularioDeCrud\:docTermoListAction',
                '#formularioDeCrud\:arquivoProcessoTable',
                'table[id*="docTermoListAction"]',
                'table[id*="arquivoProcessoTable"]',
                'div#formularioDeCrud\:docTermoParticipacao table',
                'div#formularioDeCrud\:documentos table',
                'table[id*="formularioDeCrud"][id*="List"]',
                'div.tabelas table'
            ],
            "doc_download_button_primary_termo": '#formularioDeCrud\\:downloadButtonInf',
            "doc_download_button_primary_arquivo": 'div#formularioDeCrud\\:download input[value="Baixar"]',
            "doc_download_buttons_fallback": [
                'input[id="formularioDeCrud:downloadButtonInf"]', 'input[value="Download"]',
                'input[type="submit"][value="Download"]', 'input[value="Baixar"]',
                'input[type="submit"][value="Baixar"]', 'input[name="formularioDeCrud:j_id330"]',
                'div#formularioDeCrud\\:download input', 'div#formularioDeCrud\\:grupoButtonsInf input',
                'div.actionButtons input[value="Download"]', 'div.actionButtons input[value="Baixar"]',
                'div.actionButtons input[type="submit"]', 'input[id*="download"][type="button"]',
                'input[id*="download"][type="submit"]'
            ],
            "return_button_selectors": [
                 'input[id="formularioDeCrud:pesquisar"]', 'input[value="Retornar para Pesquisa"]',
                 'input.retornarPesquisa', 'input.sec.retornarPesquisa', '#formularioDeCrud\\:pesquisar'
            ],
            "doc_name_arquivo": '#{base_id}:{index}:j_id324',
            "doc_name_termo": 'id={base_id}:{index}:docTermo',
            "doc_name_fallback_arquivo": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(2)',
            "doc_name_fallback_termo": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(2) span',
            "doc_radio_cell_arquivo": 'td[id="{base_id}:{index}:j_id321"]',
            "doc_radio_cell_termo": 'td[id="{base_id}:{index}:j_id203"]',
            "doc_radio_fallback_img": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(1) img',
            "doc_radio_fallback_span_img": '{table_selector} tbody tr:nth-child({row_num}) td:nth-child(1) span img'
        },
        "timeouts": { 
            "navigation": 90000, "default_page": 60000, "default_expect": 30000,
            "element_wait": 15000, "search_results": 30000, "button_enable": 15000,
            "download": 60000, "details_load": 20000, "network_idle": 20000,
            "short_pause": 1500, "calendar_popup": 7000, "calendar_day_click": 3000,
            "calendar_apply_click": 3000, "radio_click_wait": 2000,
            "details_render_wait": 7000, "return_navigation": 20000,
            "return_element_wait": 15000
        },
        "calendar_months_pt": [
            'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
            'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
        ]
    }


def setup_output_dirs(config: Dict[str, Any], logger: logging.Logger) -> Dict[str, Path]:
    """
    Ensure all necessary output directories exist.
    
    Args:
        config: Configuration dictionary with directory paths
        logger: Logger instance for logging messages
        
    Returns:
        Dictionary of Path objects for each output directory
    """
    output_dirs = {
        'pdf': Path(config["pdfs_dir"]),
        'archive': Path(config["archives_dir"]),
        'debug': Path(config["debug_dir"]),
        'logs': Path(config["logs_dir"]),
        'screenshots': Path(config["screenshots_dir"]),
        'temp': Path(config["temp_dir"])
    }
    
    for dir_name, dir_path in output_dirs.items():
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created or verified directory: {dir_path}")
        except Exception as e:
            logger.error(f"Failed to create {dir_name} directory ({dir_path}): {e}")
            raise
        
    return output_dirs


def load_processed_ids_json(config: Dict[str, Any], logger: logging.Logger) -> List[str]:
    """
    Load processed IDs from JSON file.
    
    Args:
        config: Configuration dictionary with file path
        logger: Logger instance for logging messages
        
    Returns:
        List of processed tender IDs
    """
    processed_ids = []
    json_path = Path(config["processed_ids_json"])
    
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
def save_processed_id_json(process_id: str, config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Save a processed ID to the JSON file with proper structure.
    
    Args:
        process_id: The ID to save
        config: Configuration dictionary with file path
        logger: Logger instance for logging messages
        
    Returns:
        Boolean indicating success
    """
    json_path = Path(config["processed_ids_json"])
    
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


def safe_screenshot(page, filename_prefix: str, config: Dict[str, Any], logger: logging.Logger) -> Optional[Path]:
    """
    Takes a screenshot safely, ignoring errors if the page/browser is closed.
    
    Args:
        page: Playwright page object
        filename_prefix: Prefix for the screenshot filename
        config: Configuration dictionary with screenshot settings
        logger: Logger instance for logging messages
        
    Returns:
        Path to the screenshot or None if failed
    """
    if not config["take_screenshots"]:
        return None

    try:
        # Ensure the screenshots directory exists
        screenshots_dir = Path(config["screenshots_dir"])
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = screenshots_dir / f"{filename_prefix}_{timestamp}.png"
        
        page.screenshot(path=str(path))
        logger.info(f"Screenshot saved: {path}")
        return path
    except Exception as e:
        logger.warning(f"Could not take screenshot '{filename_prefix}': {e}")
        return None


def create_zip_archive(source_dir: Path, process_id: str, config: Dict[str, Any], logger: logging.Logger) -> Optional[Path]:
    """
    Creates a ZIP archive from downloaded files for a process.
    
    Args:
        source_dir: Directory containing files to zip
        process_id: Process identifier for the ZIP filename
        config: Configuration dictionary with archive path
        logger: Logger instance for logging messages
        
    Returns:
        Path to created ZIP file or None if failed
    """
    if not source_dir.exists() or not any(source_dir.iterdir()):
        logger.warning(f"No files found to zip in {source_dir}")
        return None
    
    # Create ZIP filename with CAGECE prefix and process_id
    zip_filename = f"CAGECE_{process_id}.zip"
    archive_path = Path(config["archives_dir"]) / zip_filename
    
    try:
        # Ensure the archive directory exists
        Path(config["archives_dir"]).mkdir(parents=True, exist_ok=True)
        
        # Create the ZIP file
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in source_dir.glob('**/*'):
                if file_path.is_file():
                    # Calculate relative path for the file inside the ZIP
                    relative_path = file_path.relative_to(source_dir)
                    zipf.write(file_path, arcname=relative_path)
                    logger.debug(f"Added {relative_path} to {zip_filename}")
        
        logger.info(f"Created archive {archive_path} with files from {source_dir}")
        return archive_path
    except Exception as e:
        logger.error(f"Error creating ZIP archive {archive_path}: {e}")
        return None


def select_dropdown_option_by_label(page, dropdown_selector: str, label_text: str, config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Selects an option from a dropdown based on its visible text label.
    
    Args:
        page: Playwright page object
        dropdown_selector: CSS selector for the dropdown
        label_text: Text label to select
        config: Configuration dictionary with timeouts
        logger: Logger instance for logging messages
        
    Returns:
        Boolean indicating success
    """
    logging.info(f"Selecting dropdown option '{label_text}' from '{dropdown_selector}'")
    try:
        dropdown = page.locator(dropdown_selector)
        expect(dropdown).to_be_enabled(timeout=config["timeouts"]["element_wait"])
        # Use Playwright's built-in label matching
        dropdown.select_option(label=re.compile(re.escape(label_text), re.IGNORECASE))
        logging.info(f"Selected option containing text: {label_text}")
        page.wait_for_timeout(config["timeouts"]["short_pause"]) # Wait for potential updates
        return True
    except PlaywrightTimeoutError:
        logging.error(f"Timeout waiting for dropdown '{dropdown_selector}' to be enabled.")
        safe_screenshot(page, f"error_dropdown_timeout_{dropdown_selector.replace(':', '_')}", config, logger)
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
                    page.wait_for_timeout(config["timeouts"]["short_pause"])
                    return True
            logging.error(f"Could not find any option containing text: {label_text}")
            safe_screenshot(page, f"error_dropdown_notfound_{label_text[:20]}", config, logger)
            return False
        except Exception as e_fallback:
            logging.error(f"Error during manual dropdown option search for '{label_text}': {e_fallback}")
            safe_screenshot(page, f"error_dropdown_fallback_{label_text[:20]}", config, logger)
            return False
    return False


def parse_calendar_header(header_text: str, config: Dict[str, Any], logger: logging.Logger) -> Tuple[Optional[int], Optional[int]]:
    """
    Parses the calendar header (e.g., '<< < abril 2025 > >>') to get month index and year.
    
    Args:
        header_text: Text from calendar header
        config: Configuration dictionary with month names
        logger: Logger instance for logging messages
        
    Returns:
        Tuple of (month_index, year) or (None, None) if parsing fails
    """
    clean_text = header_text.replace('<', '').replace('>', '').replace('x', '').strip().lower()
    logger.debug(f"Parsing calendar header: '{clean_text}'")

    year_match = re.search(r'\b(20\d{2})\b', clean_text)
    current_year = int(year_match.group(1)) if year_match else None

    current_month_index = None
    for i, month_name in enumerate(config["calendar_months_pt"]):
        if month_name in clean_text:
            current_month_index = i
            break

    if current_year is None or current_month_index is None:
        logger.warning(f"Could not reliably parse year/month from header: '{header_text}'. Year: {current_year}, Month Index: {current_month_index}")
        # Add fallbacks for common cases
        if 'abril' in clean_text: current_month_index = 3 # Example fallback
        if '2025' in clean_text: current_year = 2025

    logger.debug(f"Parsed calendar: Month Index={current_month_index}, Year={current_year}")
    return current_month_index, current_year


def select_date_in_calendar(page, day: int, target_month_index: int, target_year: int, config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Selects a specific date in the RichFaces calendar popup.
    
    Args:
        page: Playwright page object
        day: Day to select
        target_month_index: Month index (0-11) to select
        target_year: Year to select
        config: Configuration dictionary with selectors and timeouts
        logger: Logger instance for logging messages
        
    Returns:
        Boolean indicating success
    """
    logger.info(f"Selecting date in calendar: {day}/{target_month_index + 1}/{target_year}")
    selectors = config["selectors"]
    timeouts = config["timeouts"]
    safe_screenshot(page, "calendar_before_interaction", config, logger)

    try:
        calendar_popup = page.locator(selectors["calendar_popup"]).first
        expect(calendar_popup).to_be_visible(timeout=timeouts["calendar_popup"])
        logger.info("Calendar popup is visible.")

        # Navigate Year
        while True:
            header_text = calendar_popup.locator(selectors["calendar_header"]).text_content()
            current_month_idx, current_year = parse_calendar_header(header_text, config, logger)

            if current_year == target_year or current_year is None:
                logger.info(f"Reached target year {target_year} or failed to parse current year.")
                break # Exit year loop

            year_nav_button = selectors["next_year_button"] if current_year < target_year else selectors["prev_year_button"]
            logger.info(f"Navigating year: Clicking {'next' if current_year < target_year else 'prev'} year button.")
            calendar_popup.locator(year_nav_button).click()
            page.wait_for_timeout(300) # Short pause for calendar update

        # Navigate Month (handle potential parsing failure)
        header_text = calendar_popup.locator(selectors["calendar_header"]).text_content() # Re-read header
        current_month_idx, current_year = parse_calendar_header(header_text, config, logger) # Re-parse

        if current_month_idx is not None:
            month_diff = current_month_idx - target_month_index
            if month_diff != 0:
                month_nav_button = selectors["next_month_button"] if month_diff < 0 else selectors["prev_month_button"]
                clicks_needed = abs(month_diff)
                logger.info(f"Navigating month: Clicking {'next' if month_diff < 0 else 'prev'} month button {clicks_needed} times.")
                for _ in range(clicks_needed):
                    calendar_popup.locator(month_nav_button).click()
                    page.wait_for_timeout(300)
        else:
            # Fallback if month parsing failed
            logger.warning("Could not determine current month. Calendar interaction might fail.")

        safe_screenshot(page, "calendar_after_navigation", config, logger)

        # Select Day
        logger.info(f"Selecting day: {day}")
        # Regex to match the exact day number, potentially surrounded by whitespace
        day_regex = re.compile(f'^\\s*{day}\\s*$')
        day_cell = calendar_popup.locator(selectors["calendar_day_cell"]).filter(has_text=day_regex).first
        expect(day_cell).to_be_visible(timeout=timeouts["calendar_day_click"])
        day_cell.click()
        logger.info(f"Clicked day {day}.")
        page.wait_for_timeout(500) # Pause after clicking day
        safe_screenshot(page, f"calendar_after_day_{day}_click", config, logger)

        # Click Apply
        logger.info("Clicking calendar 'Apply' button.")
        apply_button = calendar_popup.locator(selectors["calendar_apply_button"]).first
        expect(apply_button).to_be_visible(timeout=timeouts["calendar_apply_click"])
        apply_button.click()
        logger.info("Clicked 'Apply'. Calendar should close.")
        page.wait_for_timeout(timeouts["short_pause"]) # Wait for calendar to close and input update
        return True

    except Exception as e:
        logger.error(f"Error selecting date in calendar: {e}")
        safe_screenshot(page, "error_calendar_selection", config, logger)
        return False
def find_element_sequentially(page, selectors: List[str], description: str, config: Dict[str, Any], logger: logging.Logger):
    """
    Tries a list of selectors sequentially and returns the first locator found.
    
    Args:
        page: Playwright page object
        selectors: List of selectors to try
        description: Description of what we're looking for (for logging)
        config: Configuration dictionary
        logger: Logger instance for logging messages
        
    Returns:
        Locator object if found, None otherwise
    """
    logger.debug(f"Trying to find {description} using selectors: {selectors}")
    for selector in selectors:
        try:
            element = page.locator(selector)
            if element.count() > 0:
                # Perform a quick visibility check to prefer visible elements
                if element.first.is_visible(timeout=1000):
                    logger.info(f"Found {description} with selector (visible): {selector}")
                    return element.first # Return the first locator object
                else:
                    logger.debug(f"Found {description} with selector (not visible): {selector}. Trying next.")
        except Exception as e:
            # Ignore timeouts or other errors during the check loop for a single selector
            logger.debug(f"Check failed for selector {selector}: {e}")
            pass
    logger.warning(f"Could not find {description} using any provided selectors.")
    return None


def find_and_click_element(page, selectors: List[str], description: str, config: Dict[str, Any], logger: logging.Logger, timeout: Optional[int] = None) -> bool:
    """
    Finds an element using multiple selectors and clicks it.
    
    Args:
        page: Playwright page object
        selectors: List of selectors to try
        description: Description of the element (for logging)
        config: Configuration dictionary with timeouts
        logger: Logger instance for logging messages
        timeout: Optional timeout override
        
    Returns:
        Boolean indicating if element was found and clicked
    """
    if timeout is None:
        timeout = config["timeouts"]["element_wait"]
        
    element = find_element_sequentially(page, selectors, description, config, logger)
    if element:
        try:
            logger.info(f"Clicking {description}...")
            expect(element).to_be_visible(timeout=timeout)
            expect(element).to_be_enabled(timeout=timeout)
            element.click(timeout=timeout)
            logger.info(f"Clicked {description} successfully.")
            return True
        except Exception as e:
            logger.error(f"Error clicking {description}: {e}")
            safe_screenshot(page, f"error_click_{description}", config, logger)
            # Try JS click as a fallback for stubborn elements
            try:
                logger.warning("Trying JavaScript click as fallback...")
                element.evaluate("el => el.click()")
                logger.info("JavaScript click executed.")
                return True
            except Exception as js_e:
                logger.error(f"JavaScript click also failed: {js_e}")
                return False
    return False


def return_to_search_results(page, config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Helper function to return to search results page from anywhere.
    
    Args:
        page: Playwright page object
        config: Configuration dictionary with selectors and timeouts
        logger: Logger instance for logging messages
        
    Returns:
        Boolean indicating success
    """
    logger.info("Attempting to return to search results page...")
    selectors = config["selectors"]
    timeouts = config["timeouts"]
    
    # Try to find and click the return button
    returned = find_and_click_element(
        page, 
        selectors["return_button_selectors"], 
        "Return to Search button", 
        config, 
        logger
    )

    if returned:
        try:
            logger.info("Waiting for search results page to reload after return click...")
            page.wait_for_load_state('networkidle', timeout=timeouts["return_navigation"])
            expect(page.locator(selectors["search_button"])).to_be_visible(timeout=timeouts["return_element_wait"])
            logger.info("Successfully returned to search results page via button.")
            page.wait_for_timeout(timeouts["short_pause"])
            safe_screenshot(page, "after_return_to_search", config, logger)
            return True
        except Exception as e:
            logger.error(f"Error confirming return navigation: {e}")
            # Continue to fallback even if confirmation fails

    # Fallback: Browser back if button click failed
    if not returned:
        logger.warning("Return button failed. Trying browser back navigation.")
        try:
            page.go_back(wait_until="networkidle", timeout=timeouts["return_navigation"])
            expect(page.locator(selectors["search_button"])).to_be_visible(timeout=timeouts["return_element_wait"])
            logger.info("Successfully returned using browser back.")
            return True
        except Exception as e_back:
            logger.error(f"Browser back navigation also failed: {e_back}")

    # Final Fallback: Direct navigation
    try:
        page.goto(config["target_url"], wait_until="networkidle", timeout=timeouts["navigation"])
        logger.info("Successfully navigated directly to search page as last resort.")
        return True
    except Exception as e_nav:
        logger.critical(f"FATAL: Could not return to search page: {e_nav}")
        return False


def extract_process_id(row_text: str, logger: logging.Logger) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts process ID from row text with proper format matching.
    
    Args:
        row_text: Text from a table row
        logger: Logger instance for logging messages
        
    Returns:
        Tuple of (numeric_id, full_id) or (None, None) if extraction fails
    """
    # First look specifically in the N° DA PUBLICAÇÃO column format (YYYY/NNNNN)
    publication_id_match = re.search(r'\b(20\d{2}/\d{5})\b', row_text)
    if publication_id_match:
        full_id = publication_id_match.group(1)
        # Extract only the numeric part after the slash for storage/comparison
        numeric_part = full_id.split('/')[1]
        logger.info(f"Extracted publication ID: {full_id} (storing as: {numeric_part})")
        return numeric_part, full_id
    
    # As a fallback, look for N° PROCESSO column value which appears to be a 16-digit number
    process_match = re.search(r'\b(\d{16})\b', row_text)
    if process_match:
        logger.warning(f"Could not find publication ID, using proceso number instead")
        return process_match.group(1), None
    
    # No valid ID found
    logger.error(f"Could not extract any valid process ID from row text")
    return None, None
def attempt_download(page, button_selector: str, doc_name: str, save_dir: Path, config: Dict[str, Any], logger: logging.Logger) -> Optional[str]:
    """
    Attempts to click a download button and save the file.
    
    Args:
        page: Playwright page object
        button_selector: CSS selector for the download button
        doc_name: Document name for logging and filename
        save_dir: Directory to save download
        config: Configuration dictionary with timeouts
        logger: Logger instance for logging messages
        
    Returns:
        Path to downloaded file as string, or None if failed
    """
    logger.info(f"Attempting download for '{doc_name}' using button: {button_selector}")
    timeouts = config["timeouts"]
    download_path = None

    try:
        button = page.locator(button_selector).first # Assume first match is the one
        expect(button).to_be_visible(timeout=timeouts["element_wait"])
        expect(button).to_be_enabled(timeout=timeouts["button_enable"])
        logger.info(f"Download button '{button_selector}' is visible and enabled.")

        # Sanitize doc_name for filename base
        safe_filename_base = re.sub(r'[\\/*?:"<>|]', '_', doc_name)[:150]

        # Start waiting for download *before* clicking
        with page.expect_download(timeout=timeouts["download"]) as download_info:
            logger.info(f"Clicking download button: {button_selector}")
            try:
                # Use force=True cautiously, might hide issues
                button.click(timeout=timeouts["element_wait"], force=True)
            except Exception as click_error:
                logger.warning(f"Standard click failed for {button_selector}: {click_error}. Trying JS click.")
                page.evaluate(f"document.querySelector('{button_selector}').click()")

        download = download_info.value
        suggested_filename = download.suggested_filename
        logger.info(f"Download started. Suggested filename: {suggested_filename}")

        # Determine final filename
        if suggested_filename and '.' in suggested_filename[-5:]: # Basic check for extension
            final_filename = suggested_filename
        elif '.' in safe_filename_base[-5:]:
            final_filename = safe_filename_base
        else:
            final_filename = safe_filename_base + ".pdf" # Default extension if unsure
            logger.warning(f"Could not determine file extension. Defaulting to '.pdf' for: {final_filename}")

        download_path = save_dir / final_filename
        download_path.parent.mkdir(parents=True, exist_ok=True) # Ensure dir exists

        logger.info(f"Saving download to: {download_path}")
        download.save_as(str(download_path))
        logger.info(f"Download saved successfully: {download_path}")
        return str(download_path) # Return path on success

    except PlaywrightTimeoutError as te:
        logger.error(f"Timeout error during download for '{doc_name}' using {button_selector}: {te}")
        if "expect_download" in str(te):
            logger.error("Timeout waiting for download event to trigger after click.")
        else:
            logger.error(f"Timeout waiting for button '{button_selector}' state.")
        safe_screenshot(page, f"error_download_timeout_{doc_name[:20]}", config, logger)
    except Exception as e:
        logger.error(f"Error during download process for '{doc_name}' using {button_selector}: {e}")
        if "Target page, context or browser has been closed" in str(e):
            logger.warning("Browser closed during download. File might exist but wasn't saved via Playwright.")
        safe_screenshot(page, f"error_download_general_{doc_name[:20]}", config, logger)
        # Clean up potentially partial file if path was determined
        if download_path and download_path.exists():
            try:
                download_path.unlink()
                logger.info(f"Removed potentially partial download: {download_path}")
            except OSError as unlink_e:
                logger.warning(f"Error removing partial download file {download_path}: {unlink_e}")

    return None # Return None on failure


def get_document_details(page, index: int, table_selector: str, config: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    """
    Gets document name and determines table type and selectors for a given row index.
    
    Args:
        page: Playwright page object
        index: Row index (0-based)
        table_selector: CSS selector for the table
        config: Configuration dictionary with selectors
        logger: Logger instance for logging messages
        
    Returns:
        Dictionary with document details
    """
    logger.info(f"Getting details for document row {index + 1} in table '{table_selector}'")
    selectors = config["selectors"]
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

        name_element = find_element_sequentially(
            page, 
            [name_selector, fallback_name_selector], 
            f"document name for row {row_num}",
            config,
            logger
        )

        if name_element:
            doc_name = name_element.inner_text().strip()
            logger.info(f"Document name found: {doc_name}")
        else:
             logger.warning(f"Document name element not found for row {row_num}.")

    except Exception as e:
        logger.error(f"Error getting document name for row {row_num}: {e}")
        safe_screenshot(page, f"error_get_doc_name_{row_num}", config, logger)
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
def process_document_row(page, index: int, table_selector: str, process_dir: Path, processed_doc_names: Set[str], config: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Selects, downloads, and potentially deselects a document in a table row.
    
    Args:
        page: Playwright page object
        index: Row index (0-based)
        table_selector: CSS selector for the table
        process_dir: Directory to save downloaded files
        processed_doc_names: Set of already processed document names
        config: Configuration dictionary
        logger: Logger instance for logging messages
        
    Returns:
        Boolean indicating if download was successful
    """
    logger.info(f"--- Processing Document Row {index + 1} ---")
    timeouts = config["timeouts"]

    doc_details = get_document_details(page, index, table_selector, config, logger)
    doc_name = doc_details["name"]
    is_arquivo_processo = doc_details["is_arquivo_processo"]
    radio_selectors = doc_details["radio_selectors"]
    download_selectors = doc_details["download_selectors"]

    if doc_name in processed_doc_names:
        logger.info(f"Skipping duplicate document: {doc_name}")
        return False # Indicate no download occurred
    processed_doc_names.add(doc_name)

    # --- Select Document Radio/Cell ---
    logger.info(f"Selecting document radio/cell for row {index + 1}")
    safe_screenshot(page, f"doc_{index+1}_before_select", config, logger)
    # Use find_and_click_element helper
    radio_clicked = find_and_click_element(
        page, 
        radio_selectors, 
        f"document radio for row {index + 1}",
        config,
        logger
    )

    if not radio_clicked:
        # Decide if this is critical. Maybe selection isn't needed?
        logger.warning(f"Could not click radio/cell for row {index + 1}. Proceeding to download attempt anyway.")
        # return False # Or decide to stop if selection is mandatory

    # Wait after clicking radio for potential AJAX updates
    logger.info("Waiting after radio click for potential updates...")
    page.wait_for_timeout(timeouts["short_pause"])
    try:
        # Wait for network idle briefly to catch AJAX calls
        page.wait_for_load_state('networkidle', timeout=5000)
    except PlaywrightTimeoutError:
        logger.warning("Network did not become idle after radio click, continuing...")

    # --- Attempt Download ---
    logger.info(f"Attempting download for document: {doc_name}")
    downloaded_path = None
    for btn_selector in download_selectors:
        downloaded_path = attempt_download(
            page, 
            btn_selector, 
            doc_name, 
            process_dir, 
            config, 
            logger
        )
        if downloaded_path:
            logger.info(f"Successfully downloaded '{doc_name}' using selector: {btn_selector}")
            break # Exit loop on successful download
        else:
            logger.info(f"Download attempt failed with selector: {btn_selector}. Trying next.")

    if not downloaded_path:
        logger.error(f"Failed to download document: {doc_name} after trying all selectors.")
        safe_screenshot(page, f"failed_download_{doc_name[:20]}_{index + 1}", config, logger)
        # Decide whether to continue processing other documents or stop

    # --- Deselect Radio (if needed for specific table type) ---
    if is_arquivo_processo and radio_clicked: # Only deselect if it's the special table AND we successfully clicked it initially
        logger.info(f"Deselecting radio button for document {index + 1} (arquivoProcessoTable special case)")
        # We need to click the *same* element again to deselect
        deselected = find_and_click_element(
            page, 
            radio_selectors, 
            f"document radio DESELECT for row {index + 1}",
            config,
            logger
        )
        if deselected:
            logger.info(f"Deselected radio for row {index + 1}. Waiting for updates.")
            page.wait_for_timeout(2000)  # 2-second delay after deselecting radio button
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except PlaywrightTimeoutError:
                 logger.warning("Network did not become idle after radio deselect click, continuing...")
        else:
            logger.warning(f"Failed to deselect radio for row {index + 1}. This might affect subsequent rows.")
            safe_screenshot(page, f"error_deselect_radio_{index + 1}", config, logger)

    return downloaded_path is not None # Return True if download was successful


def process_details_page(page, process_id: str, config: Dict[str, Any], logger: logging.Logger) -> Tuple[bool, Optional[Path]]:
    """
    Handles actions on the details page: finding table, processing documents, returning.
    
    Args:
        page: Playwright page object
        process_id: Process identifier
        config: Configuration dictionary
        logger: Logger instance for logging messages
        
    Returns:
        Tuple of (success_bool, archive_path)
    """
    logger.info(f"--- Processing Details Page for Process ID: {process_id} ---")
    selectors = config["selectors"]
    timeouts = config["timeouts"]
    
    # For recovery calls
    if process_id == "recovery":
        return return_to_search_results(page, config, logger), None
    
    process_dir = Path(config["temp_dir"]) / process_id
    process_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Current URL: {page.url}")
    logger.info(f"Page title: {page.title()}")
    safe_screenshot(page, f"details_page_{process_id}", config, logger)
    
    processing_success = False  # Track if processing was successful
    archive_path = None  # Path to created ZIP
    start_time = time.time()  # Record start time for timeout tracking
    last_activity_time = start_time  # Track time of last successful activity

    # Find the document table
    doc_table = find_element_sequentially(page, selectors["doc_tables"], "document table", config, logger)

    if not doc_table:
        logger.warning("No document table found on details page. Skipping download.")
    else:
        # Re-check which selector exists.
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
                    if time_since_activity > 25:
                        logger.warning(f"Timeout detected! No activity for {time_since_activity:.1f} seconds (threshold: 25s)")
                        break
                        
                    # Try to process row
                    if process_document_row(page, i, found_table_selector, process_dir, processed_doc_names, config, logger):
                        downloads_succeeded += 1
                        last_activity_time = time.time()  # Update activity time after successful download
                    
                logger.info(f"Attempted to download {downloads_succeeded}/{rows_count} unique documents.")
                
                # Set processing_success = True only if we successfully created the archive
                if downloads_succeeded > 0:
                    archive_path = create_zip_archive(process_dir, process_id, config, logger)
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
    if time_since_activity > 25:
        logger.warning(f"Timeout detected before return! No activity for {time_since_activity:.1f} seconds (threshold: 25s)")
        return False, None
    
    # Return to search results
    return_success = return_to_search_results(page, config, logger)
    
    # Return True only if both processing and navigation were successful
    return (processing_success and return_success), archive_path

def get_search_results_count(page, config: Dict[str, Any], logger: logging.Logger) -> Optional[int]:
    """
    Extracts the total number of rows from the pagination display.
    
    Args:
        page: Playwright page object
        config: Configuration dictionary
        logger: Logger instance for logging messages
        
    Returns:
        Total count as integer, or None if extraction fails
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
        safe_screenshot(page, "error_extracting_count", config, logger)
        return None


def process_search_results(page, config: Dict[str, Any], logger: logging.Logger, processed_ids: List[str] = None) -> List[Dict[str, Any]]:
    """
    Processes rows in the search results table.
    
    Args:
        page: Playwright page object
        config: Configuration dictionary
        logger: Logger instance for logging messages
        processed_ids: Optional list of IDs already processed
        
    Returns:
        List of processed tender dictionaries
    """
    logger.info("--- Processing Search Results ---")
    selectors = config["selectors"]
    timeouts = config["timeouts"]
    processed_tenders = []  # List to hold processed tender data
    
    # Load processed IDs if not provided
    if processed_ids is None:
        processed_ids = load_processed_ids_json(config, logger)
    
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
        safe_screenshot(page, "search_results_visible", config, logger)

        # Wait for AJAX updates to complete
        logger.info("Waiting 3 seconds before extracting search results count...")
        page.wait_for_timeout(3000)  # 3 seconds in milliseconds

        # Get the total results count from pagination
        total_results_count = get_search_results_count(page, config, logger)
        if total_results_count is None:
            logger.warning("Could not determine total search results count. Will process all visible rows.")
            total_results_count = float('inf')  # Process all visible rows

    except PlaywrightTimeoutError:
        logger.warning("Timeout waiting for results table or visualize button.")
        safe_screenshot(page, "search_results_timeout", config, logger)
        # Continue anyway - don't exit early
        total_results_count = float('inf')  # Process indefinitely if timeout
    
    # Counter for processed rows
    processed_row_count = 0
    max_rows_to_process = 100  # Safety limit to avoid infinite loops
    
    logger.info(f"Will process up to {total_results_count} rows from search results (max {max_rows_to_process})")

    # --- Process Radio Buttons ---
    while processed_row_count < total_results_count and processed_row_count < max_rows_to_process:
        logger.info("Processing result rows...")
        # Get current visible radio buttons
        radio_images = page.locator(selectors["results_row_radio_img_generic"])
        visible_rows = radio_images.count()
        logger.info(f"Found {visible_rows} visible rows with clickable radio buttons")

        # If no rows visible but we haven't reached our target, try page navigation or other approaches
        if visible_rows == 0:
            logger.warning("No more radio buttons visible, but target count not reached.")
            logger.warning(f"Processed {processed_row_count}/{total_results_count} rows so far.")
            # Attempt one more time after a pause
            page.wait_for_timeout(5000)
            
            # Re-check for radio buttons after pause
            radio_images = page.locator(selectors["results_row_radio_img_generic"])
            visible_rows = radio_images.count()
            
            if visible_rows == 0:
                logger.info("Still no radio buttons found after retry. Finishing processing.")
                break  # Exit loop if we can't find more rows
                
        # Process visible rows
        for i in range(visible_rows):
            # Skip if we've already reached our target
            if processed_row_count >= total_results_count or processed_row_count >= max_rows_to_process:
                logger.info(f"Reached target of {processed_row_count} processed rows. Processing complete!")
                return processed_tenders
                
            logger.info(f"--- Processing Row {i+1}/{visible_rows} ({processed_row_count+1}/{total_results_count} total) ---")
            
            try:
                # Re-locate the element to avoid staleness
                current_radio_img = page.locator(selectors["results_row_radio_img_generic"]).nth(i)
                    
                # Try to extract process ID from the row before clicking
                try:
                    # Get the row containing this radio button
                    row = current_radio_img.locator('xpath=ancestor::tr').first
                    # Try to find process ID in the row text
                    row_text = row.inner_text()
                    
                    # Extract process ID and full ID
                    process_id, full_id = extract_process_id(row_text, logger)
                    
                    # Extract title/objeto for the tender
                    tender_title = "Unknown Title"
                    try:
                        # Extract title from the row - typically in a cell with descriptive text
                        title_cell = row.locator('td:nth-child(2)').first # Adjust selector as needed
                        if title_cell:
                            tender_title = title_cell.inner_text().strip()
                            if not tender_title:
                                # Try a different approach if the first one failed
                                tender_title = row_text.split('\n')[1].strip() if '\n' in row_text else row_text
                    except Exception as title_err:
                        logger.warning(f"Could not extract title from row: {title_err}")
                        tender_title = f"Title for Process {process_id or 'Unknown'}"
                    
                    if process_id:
                        # Check if already processed in JSON OR in this session
                        if process_id in processed_ids:
                            logger.info(f"Process ID {process_id} already in JSON, skipping.")
                            processed_row_count += 1  # Still count as processed
                            continue
                            
                        if process_id in session_processed_ids:
                            logger.info(f"Process ID {process_id} already processed in this session, skipping.")
                            processed_row_count += 1 
                            continue
                        
                        if full_id:
                            logger.info(f"Found publication ID: {full_id} (storing as: {process_id}) in row {i+1}")
                        else:    
                            logger.info(f"Found process ID: {process_id} in row {i+1}")
                    else:
                        # Generate a temporary ID if extraction fails
                        process_id = f"row{i+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        logger.warning(f"Could not extract processo number, using temporary ID: {process_id}")
                except Exception as e:
                    logger.warning(f"Could not extract process ID from row {i+1}: {e}")
                    # Generate a temporary ID
                    process_id = f"row{i+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    tender_title = f"Unknown Title for Row {i+1}"
                
                # Get source URL (the current search page URL)
                source_url = page.url
                
                logger.info(f"Clicking radio image for row {i+1} (Process ID: {process_id})...")
                current_radio_img.scroll_into_view_if_needed()
                expect(current_radio_img).to_be_visible(timeout=timeouts["element_wait"])
                current_radio_img.click()
                logger.info(f"Clicked radio image for row {i+1}.")
                page.wait_for_timeout(timeouts["radio_click_wait"]) # Wait for AJAX

                # Wait for and click the Visualizar button
                logger.info(f"Waiting for Visualizar button to be enabled...")
                viz_button = page.locator(selectors["visualize_button_enabled"]).first
                expect(viz_button).to_be_enabled(timeout=timeouts["button_enable"])
                expect(viz_button).to_be_visible(timeout=timeouts["element_wait"])
                logger.info("Visualizar button enabled. Clicking...")
                safe_screenshot(page, f"before_visualizar_click_{i+1}", config, logger)
                viz_button.click()

                # Wait for details page load
                logger.info("Waiting for details page to load (network idle)...")
                page.wait_for_load_state('networkidle', timeout=timeouts["details_load"])
                logger.info("Details page network idle. Waiting for rendering...")
                page.wait_for_timeout(timeouts["details_render_wait"]) # Extra wait

                # Process the details page
                successful, archive_path = process_details_page(page, process_id, config, logger)
                
                # Only save to JSON and increment count if processing was successful
                if successful:
                    save_processed_id_json(process_id, config, logger)
                    session_processed_ids.add(process_id)
                    
                    # Create tender data dictionary for return
                    tender_data = {
                        "tender_id": process_id,
                        "title": tender_title,
                        "downloaded_zip_path": str(archive_path) if archive_path else None,
                        "source_url": source_url
                    }
                    
                    # Only add to processed_tenders if we have a valid ZIP path
                    if archive_path:
                        processed_tenders.append(tender_data)
                        logger.info(f"Added processed tender: {tender_data}")
                    else:
                        logger.warning(f"Processing succeeded but no ZIP created for {process_id}")
                    
                    # Increment counter
                    processed_row_count += 1
                    logger.info(f"Processed {processed_row_count}/{total_results_count} rows")
                else:
                    logger.warning(f"Failed to process {process_id}. Not counting toward total.")

            except PlaywrightTimeoutError as te:
                logger.error(f"TimeoutError processing row {i+1}: {te}")
                safe_screenshot(page, f"timeout_error_row_{i+1}", config, logger)
                logger.warning("Attempting to recover and continue...")
                # Attempt to recover by returning to search page
                if not return_to_search_results(page, config, logger):
                     logger.critical("Failed to return to search results after timeout. Stopping.")
                     break # Stop if we can't get back

            except Exception as e:
                logger.error(f"Error processing row {i+1}: {type(e).__name__} - {e}")
                safe_screenshot(page, f"general_error_row_{i+1}", config, logger)
                logger.warning("Attempting to recover and continue to the next row...")
                if not return_to_search_results(page, config, logger):
                     logger.critical("Failed to return to search results after error. Stopping.")
                     break # Stop if we can't get back
                
        # Check if all visible rows are already processed
        all_ids_already_processed = True
        for i in range(visible_rows):
            try:
                current_radio = page.locator(selectors["results_row_radio_img_generic"]).nth(i)
                row = current_radio.locator('xpath=ancestor::tr').first
                row_text = row.inner_text()
                
                process_id, _ = extract_process_id(row_text, logger)
                
                if process_id and process_id not in processed_ids and process_id not in session_processed_ids:
                    all_ids_already_processed = False
                    break
            except:
                pass
                
        if all_ids_already_processed:
            logger.info("All visible rows are already processed. Exiting loop.")
            break
            
    logger.info(f"Finished processing search results. Processed {processed_row_count} rows.")
    return processed_tenders


def process_single_keyword(page, keyword: str, config: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Processes a single keyword search on the CAGECE portal.
    
    Args:
        page: Playwright page object
        keyword: Keyword to search for
        config: Configuration dictionary
        logger: Logger instance for logging messages
        
    Returns:
        List of tender dictionaries
    """
    logger.info(f"Processing CAGECE with keyword: {keyword}")
    # Create a config copy for this keyword
    keyword_config = config.copy()
    keyword_config['search_params'] = config['search_params'].copy()
    keyword_config['search_params']['object_keyword'] = keyword

    try:
        page.goto(config["target_url"], timeout=config["timeouts"]["navigation"])
        logger.info(f"Navigated to {config['target_url']}")
        safe_screenshot(page, f"{keyword}_0_initial_page", config, logger)

        # Fill organization dropdown
        select_dropdown_option_by_label(
            page,
            config["selectors"]["organization_dropdown"],
            config["search_params"]["organization_label"],
            config,
            logger
        )
        logger.info(f"Selected organization: {config['search_params']['organization_label']}")
        page.wait_for_timeout(config["timeouts"]["short_pause"])
        safe_screenshot(page, f"{keyword}_1_organization_selected", config, logger)

        # Fill acquisition nature dropdown
        select_dropdown_option_by_label(
            page,
            config["selectors"]["acquisition_nature_dropdown"],
            config["search_params"]["acquisition_nature_label"],
            config,
            logger
        )
        logger.info(f"Selected acquisition nature: {config['search_params']['acquisition_nature_label']}")
        page.wait_for_timeout(config["timeouts"]["short_pause"])
        safe_screenshot(page, f"{keyword}_2_acquisition_nature_selected", config, logger)

        # Fill object (keyword)
        try:
            page.locator(config["selectors"]["object_input"]).fill(keyword)
        except PlaywrightTimeoutError:
            logger.warning("Primary object input timed out, trying fallback.")
            page.locator(config["selectors"]["object_input_fallback"]).fill(keyword)
        logger.info(f"Filled object with keyword: {keyword}")
        safe_screenshot(page, f"{keyword}_3_object_filled", config, logger)

        # Select Date (e.g., from Jan 1st of current year)
        start_date_params = config["search_params"]["start_date"]
        page.locator(config["selectors"]["start_date_button"]).click()
        select_date_in_calendar(
            page, 
            start_date_params["day"], 
            start_date_params["month"], 
            start_date_params["year"],
            config,
            logger
        )
        logger.info(f"Selected start date: {start_date_params['day']}/{start_date_params['month']+1}/{start_date_params['year']}")
        safe_screenshot(page, f"{keyword}_4_date_selected", config, logger)
        page.wait_for_timeout(config["timeouts"]["short_pause"])

        # Click search button
        find_and_click_element(
            page, 
            [config["selectors"]["search_button"], config["selectors"]["search_button_fallback"]], 
            "Search Button",
            config,
            logger
        )
        logger.info("Clicked search button.")
        page.wait_for_load_state("networkidle", timeout=config["timeouts"]["search_results"])
        safe_screenshot(page, f"{keyword}_5_search_results", config, logger)

        # Process search results and get tender data
        processed_ids = load_processed_ids_json(config, logger)
        keyword_tender_data = process_search_results(page, config, logger, processed_ids)
        logger.info(f"Found {len(keyword_tender_data)} new tenders for keyword '{keyword}'.")
        
        return keyword_tender_data

    except PlaywrightTimeoutError as pte:
        logger.error(f"Playwright timeout during processing for keyword '{keyword}': {pte}")
        safe_screenshot(page, f"{keyword}_playwright_timeout_error", config, logger)
    except Exception as e:
        logger.error(f"Unexpected error during processing for keyword '{keyword}': {e}", exc_info=True)
        safe_screenshot(page, f"{keyword}_unexpected_error", config, logger)
    
    return []  # Return empty list if errors occurred


def run_handler(company_id: str, output_dir: str, keywords: List[str], notion_database_id: str = None, headless_mode: bool = True) -> Dict[str, Any]:
    """
    Main entry point for the CAGECE handler.
    
    Args:
        company_id: Company identifier
        output_dir: Directory for outputs (downloads, logs, etc.)
        keywords: List of keywords to search for
        notion_database_id: Optional Notion database ID
        headless_mode: Whether to run browser in headless mode
        
    Returns:
        Dictionary with standardized result format
    """
    # Initialize configuration and logging
    config = get_dynamic_config(output_dir, company_id)
    logger = setup_logging(config['logs_dir'], company_id)
    
    # Override headless mode from parameter
    config["headless_mode"] = headless_mode
    
    logger.info(f"CAGECE handler started for Company ID: {company_id}, Output Dir: {output_dir}")
    logger.info(f"Using keywords: {keywords}")
    
    if not keywords:
        logger.warning("No keywords provided to CAGECE handler.")
        return {
            "success": False,
            "company_id": company_id,
            "new_tenders_processed": [],
            "error_message": "No keywords provided for search."
        }

    # Setup output directories
    try:
        setup_output_dirs(config, logger)
    except Exception as dir_error:
        logger.critical(f"Failed to create output directories: {dir_error}")
        return {
            "success": False,
            "company_id": company_id,
            "new_tenders_processed": [],
            "error_message": f"Failed to create output directories: {dir_error}"
        }

    # List to collect all processed tenders
    all_processed_tenders = []
    overall_success = False
    error_message = None

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=config["headless_mode"])
            context = browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            context.set_default_timeout(config["timeouts"]["default_page"])
            page = context.new_page()

            for keyword in keywords:
                logger.info(f"Processing keyword: {keyword}")
                try:
                    # Process the keyword and collect tender data
                    tenders_for_keyword = process_single_keyword(page, keyword, config, logger)
                    
                    if tenders_for_keyword:
                        all_processed_tenders.extend(tenders_for_keyword)
                        logger.info(f"Collected {len(tenders_for_keyword)} tenders for keyword '{keyword}'.")
                    else:
                        logger.info(f"No new tenders found for keyword '{keyword}'.")
                except Exception as e:
                    logger.error(f"Error processing keyword '{keyword}': {e}", exc_info=True)
                    # Continue to next keyword despite errors
                finally:
                    # Ensure we are back on the search page before next keyword
                    try:
                        if not page.is_closed() and page.url != config["target_url"]:
                            return_to_search_results(page, config, logger)
                    except Exception as e_nav:
                        logger.error(f"Error returning to search page before next keyword: {e_nav}")
            
            # Set success flag based on whether we processed any tenders
            overall_success = True
            browser.close()
            
        except Exception as e:
            logger.critical(f"Critical error in CAGECE handler: {e}", exc_info=True)
            overall_success = False
            error_message = f"Critical error: {e}"
            # Ensure browser is closed even if an error occurs
            if 'browser' in locals() and browser:
                try:
                    browser.close()
                except Exception:
                    pass

    # Prepare final result in required format
    result = {
        "success": overall_success,
        "company_id": company_id,
        "new_tenders_processed": all_processed_tenders,
        "error_message": error_message
    }
    
    logger.info(f"CAGECE handler finished. Success: {overall_success}, Tenders processed: {len(all_processed_tenders)}")
    if error_message:
        logger.info(f"Error message: {error_message}")
        
    return result


# Entry point for direct script execution
if __name__ == "__main__":
    # Simple command line interface for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="CAGECE Procurement Download Handler")
    parser.add_argument("--company-id", default="CAGECE", help="Company ID for context")
    parser.add_argument("--output-dir", default="./downloads", help="Output directory")
    parser.add_argument("--keywords", nargs="+", default=["tubo", "polietileno", "PEAD", "hidrômetro"], help="Keywords to search")
    parser.add_argument("--notion-database-id", help="Notion database ID (optional)")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    
    args = parser.parse_args()
    
    result = run_handler(
        company_id=args.company_id,
        output_dir=args.output_dir,
        keywords=args.keywords,
        notion_database_id=args.notion_database_id,
        headless_mode=args.headless
    )
    
    print(json.dumps(result, indent=2))