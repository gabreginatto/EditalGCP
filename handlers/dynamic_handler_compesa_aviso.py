#!/usr/bin/env python3
"""
Compesa Download Handler for Procurement Download System

Handles downloading documents from the Compesa procurement portal (portalscl.compesa.com.br).

Workflow:
1. Navigates to the main listing page 
   (https://portalscl.compesa.com.br:8743/webrunstudio/form.jsp?sys=SCL&action=openform&formID=7&...).
2. Filters processes based on keywords in the 'Objeto' column within the main iframe.
3. Avoids reprocessing using a state file (processed_compesa.json).
4. For matching processes, clicks the details trigger (three dots icon).
5. Handles the multi-step popup sequence within its own iframe:
    - Selects 'Pessoa Jurídica'.
    - Selects the first 'Lote'.
    - Enters a predefined CNPJ.
    - Clicks 'Consultar'.
    - Waits for 'Salvar'.
    - Clicks 'Salvar' and handles the subsequent confirmation dialog.
    - Waits for and clicks the 'DOWNLOAD DO EDITAL' button.
6. Downloads the main document using Playwright's download event (primary)
   or attempts URL extraction + requests (secondary, currently commented out).
7. Creates a ZIP archive containing the downloaded document for each process.

Usage:
    python dynamic_handler_compesa.py --url URL --output-dir DIR [--timeout SECONDS]
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
        FrameLocator,
        Locator,
        TimeoutError as PlaywrightTimeoutError,
        Error as PlaywrightError,
        BrowserContext,
        Dialog
    )
except ImportError:
    print(json.dumps({
        "success": False,
        "error_message": "Playwright not installed. Run 'pip install playwright' and 'playwright install'"
    }))
    sys.exit(1)

# --- Logging Configuration ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_download_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'downloads'))
log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = f"dynamic_handler_compesa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    level=logging.INFO, # INFO for production, DEBUG for development
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("CompesaHandler")

# --- Configuration ---
KEYWORDS = ["tubo", "polietileno", "PEAD", "polimero", "PAM", "hidrômetro", "medidor"]
STATE_FILE_NAME = "processed_compesa.json"
STATE_KEY = "processed_compesa_processos"
BASE_URL = "https://portalscl.compesa.com.br:8743/" # Adjust if necessary
CNPJ_TO_USE = "39.726.816/0001-32" # Predefined CNPJ

# --- Helper Functions ---

def setup_output_dirs(base_dir):
    """Ensure all necessary output directories exist relative to the base download dir."""
    base_dir_abs = os.path.abspath(base_dir)
    logger.info(f"Setting up directories relative to: {base_dir_abs}")
    output_dirs = {
        'pdf': os.path.join(base_dir_abs, 'pdfs'),
        'archive': os.path.join(base_dir_abs, 'archives'),
        'debug': os.path.join(base_dir_abs, 'debug'),
        'logs': log_dir,
        'screenshots': os.path.join(base_dir_abs, 'debug', 'screenshots'),
        'temp': os.path.join(base_dir_abs, 'temp'),
        'state': base_dir_abs
    }
    for key, dir_path in output_dirs.items():
        if key != 'state':
            try:
                os.makedirs(dir_path, exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create directory {dir_path}: {e}")
                raise
    return output_dirs

def clean_filename(filename: Optional[str], max_length: int = 100) -> str:
    """Clean a filename to make it safe for the filesystem."""
    if not filename: return f"unknown_file_{int(time.time())}"
    filename = os.path.basename(filename)
    filename = re.sub(r'[\\/*?:"<>|\n\r]', '_', filename)
    filename = re.sub(r'[\s_]+', '_', filename)
    filename = filename.strip('_ ')
    if len(filename) > max_length:
        name, ext = os.path.splitext(filename)
        ext = ext[:max_length]
        name = name[:max_length - len(ext)]
        filename = name + ext
    if not filename: return f"cleaned_empty_{int(time.time())}"
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
            try: os.remove(output_zip_path)
            except Exception as rm_e: logger.error(f"Failed to remove partial zip {output_zip_path}: {rm_e}")
        return False

def load_processed_state(output_dirs: Dict[str, str]) -> Set[str]:
    """Load the set of processed process IDs from the state file."""
    state_file_path = os.path.join(output_dirs['state'], STATE_FILE_NAME)
    try:
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                processed_set = set(data.get(STATE_KEY, []))
                logger.info(f"Loaded {len(processed_set)} processed IDs from {state_file_path}")
                return processed_set
        else:
            logger.info(f"State file not found ({state_file_path}). Starting fresh.")
            return set()
    except (json.JSONDecodeError, IOError, Exception) as e:
        logger.error(f"Error loading state file {state_file_path}: {e}. Starting fresh.", exc_info=True)
        return set()

def save_processed_state(output_dirs: Dict[str, str], processed_set: Set[str]):
    """Save the set of processed process IDs to the state file."""
    state_file_path = os.path.join(output_dirs['state'], STATE_FILE_NAME)
    try:
        processed_list = sorted([str(item) for item in processed_set])
        data = {STATE_KEY: processed_list}
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(processed_list)} processed IDs to {state_file_path}")
    except (IOError, TypeError, Exception) as e:
        logger.error(f"Error saving state file {state_file_path}: {e}", exc_info=True)

async def handle_dialog(dialog: Dialog):
    """Handles confirmation dialogs by accepting them."""
    logger.info(f"Dialog message detected: '{dialog.message}'. Accepting.")
    await dialog.accept()

async def simulate_human_typing(page_or_frame, input_element, text, delay_range=(50, 150)):
    """Simulates human typing by typing characters one by one with random delays.
    
    Args:
        page_or_frame: Playwright page or frame to type in
        input_element: Locator for the input element
        text: The text to type
        delay_range: Tuple of (min_ms, max_ms) for random delay between keystrokes
    """
    import random
    
    # Clear the input field first
    await input_element.fill("")
    await page_or_frame.wait_for_timeout(random.randint(100, 300))
    
    # Type each character with a random delay
    for char in text:
        await input_element.type(char, delay=random.randint(delay_range[0], delay_range[1]))
        await page_or_frame.wait_for_timeout(random.randint(10, 50))

async def process_detail_popup(
    popup_page: Page,
    context: BrowserContext,
    processo_id: str,
    objeto_clean: str,
    output_dirs: Dict[str, str]
) -> bool:
    """Handles the multi-step popup to download the edital document."""
    logger.info(f"--- Processing Detail Popup for Process: {processo_id} ---")
    temp_process_dir = os.path.join(output_dirs['temp'], processo_id)
    os.makedirs(temp_process_dir, exist_ok=True)
    downloaded_file_path = None
    final_zip_success = False

    try:
        # Take an initial screenshot to see what we're working with
        await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_1_initial.png"))
        
        # Examine the structure of the popup to determine the correct approach
        frame_info = await popup_page.evaluate("""() => {
            try {
                // Check for iframe presence
                const iframes = document.querySelectorAll('iframe');
                const frameInfo = [];
                
                for (let i = 0; i < iframes.length; i++) {
                    const iframe = iframes[i];
                    frameInfo.push({
                        index: i,
                        id: iframe.id || 'no-id',
                        name: iframe.name || 'no-name',
                        src: iframe.src || 'no-src'
                    });
                }
                
                return {
                    iframeCount: iframes.length,
                    frames: frameInfo,
                    bodyHTML: document.body.innerHTML.substring(0, 1000),
                    radioInputs: document.querySelectorAll('input[type="radio"]').length,
                    formElements: document.querySelectorAll('form').length
                };
            } catch (e) {
                return { error: e.toString() };
            }
        }""")
        
        logger.debug(f"Popup structure for {processo_id}: {frame_info}")
        
        # Determine approach based on frame inspection
        if 'error' in frame_info:
            logger.warning(f"Error inspecting popup structure: {frame_info['error']}")
        
        # Check if we have an iframe to work with
        target_frame = None
        if frame_info.get('iframeCount', 0) > 0:
            logger.info(f"Found {frame_info['iframeCount']} iframes in popup.")
            
            # Try to find the mainform iframe first
            frames = popup_page.frames
            for frame in frames:
                try:
                    frame_name = frame.name
                    if frame_name == "mainform":
                        target_frame = frame
                        logger.info("Found iframe with name 'mainform'")
                        break
                except Exception as frame_err:
                    logger.warning(f"Error getting frame name: {frame_err}")
            
            # If mainform not found, try the first iframe
            if not target_frame and len(frames) > 1:
                logger.info("Using first child iframe as target")
                target_frame = frames[1]  # Skip main frame (index 0)
        
        # If we have a target frame, use it; otherwise try to interact directly with the page
        if target_frame:
            logger.info("Using target iframe for interactions")
            
            # --- Step 1: Look for pessoa jurídica radio button in the frame ---
            radio_visible = await target_frame.evaluate("""() => {
                try {
                    // First try specific value
                    let radioJ = document.querySelector('input[type="radio"][value="J"]');
                    
                    // If not found, try more generic approaches
                    if (!radioJ) {
                        // Try all radio buttons
                        const radios = document.querySelectorAll('input[type="radio"]');
                        
                        // If we have exactly 2 radios, assume PF/PJ (second is usually PJ)
                        if (radios.length === 2) {
                            radioJ = radios[1]; // Select the second radio (index 1)
                        } 
                        // Or try to find by label text
                        else {
                            for (let radio of radios) {
                                // Check for label near the radio
                                const id = radio.id;
                                if (id) {
                                    const label = document.querySelector(`label[for="${id}"]`);
                                    if (label && (
                                        label.textContent.includes('Jurídica') || 
                                        label.textContent.includes('JURÍDICA') ||
                                        label.textContent.includes('PJ') ||
                                        label.textContent.includes('Pessoa Jurídica')
                                    )) {
                                        radioJ = radio;
                                        break;
                                    }
                                }
                                
                                // Check for text near the radio
                                const parent = radio.parentElement;
                                if (parent && (
                                    parent.textContent.includes('Jurídica') || 
                                    parent.textContent.includes('JURÍDICA') ||
                                    parent.textContent.includes('PJ') ||
                                    parent.textContent.includes('Pessoa Jurídica')
                                )) {
                                    radioJ = radio;
                                    break;
                                }
                            }
                        }
                    }
                    
                    if (radioJ) {
                        // Click the radio button
                        radioJ.click();
                        return { 
                            clicked: true, 
                            message: "Found and clicked radio button",
                            value: radioJ.value || 'unknown'
                        };
                    }
                    
                    // If we couldn't find the right radio, report what we found
                    const radios = document.querySelectorAll('input[type="radio"]');
                    return { 
                        found: false, 
                        message: "No appropriate radio found", 
                        radioCount: radios.length,
                        radioValues: Array.from(radios).map(r => ({
                            value: r.value || 'no-value',
                            id: r.id || 'no-id',
                            name: r.name || 'no-name',
                            parentText: r.parentElement ? r.parentElement.textContent.trim().substring(0, 30) : 'no-parent'
                        }))
                    };
                } catch (e) {
                    return { error: e.toString() };
                }
            }""")
            
            logger.info(f"Radio button selection result: {radio_visible}")
            
            # --- Step 2: Look for lote dropdown using robust Playwright selectors ---
            try:
                # More robust way to click the LOTE dropdown
                await target_frame.click('button:near(:text("LOTE"))', timeout=5000)
                logger.info("Successfully clicked LOTE dropdown using robust selector")
                
                # Wait a moment for dropdown to open
                await target_frame.wait_for_timeout(1000)
                
                # Try to click the first option in the dropdown using various selectors
                try:
                    # Try JavaScript approach to select first option immediately since Playwright selections aren't working
                    selected_option = await target_frame.evaluate("""() => {
                        try {
                            // Find all select elements
                            const selects = document.querySelectorAll('select');
                            if (selects.length === 0) return { success: false, message: "No select elements found" };
                            
                            // Find a visible select with options
                            let targetSelect = null;
                            for (const select of selects) {
                                if (select.options.length > 0 && select.offsetParent !== null) {
                                    targetSelect = select;
                                    break;
                                }
                            }
                            
                            if (!targetSelect) return { success: false, message: "No visible select with options found" };
                            
                            // Select first non-empty option (usually index 1, skipping placeholder)
                            let selectedIndex = 0;
                            if (targetSelect.options.length > 1 && 
                                (!targetSelect.options[0].value || targetSelect.options[0].value === "")) {
                                selectedIndex = 1;
                            }
                            
                            // First programmatically select the option to ensure it's active
                            targetSelect.selectedIndex = selectedIndex;
                            targetSelect.dispatchEvent(new Event('change', { bubbles: true }));
                            
                            // Then explicitly click on the option to trigger any click handlers
                            // Make sure option is visible first by clicking the select to open it
                            targetSelect.click();
                            setTimeout(() => {
                                const option = targetSelect.options[selectedIndex];
                                if (option) {
                                    // Force a proper click event on the option
                                    const clickEvent = new MouseEvent('click', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    option.dispatchEvent(clickEvent);
                                }
                            }, 100);
                            
                            return { 
                                success: true, 
                                message: `Selected and clicked first option at index ${selectedIndex}`,
                                optionText: targetSelect.options[selectedIndex].textContent || "Unknown option"
                            };
                        } catch (e) {
                            return { success: false, error: e.toString() };
                        }
                    }""")
                    
                    if selected_option.get('success', False):
                        logger.info(f"Selected and clicked first option via JavaScript: {selected_option.get('optionText', 'Unknown')}")
                        # Wait a moment for click to take effect
                        await target_frame.wait_for_timeout(500)
                    else:
                        # If JavaScript selection fails, try direct click with force option
                        try:
                            # Try to forcibly click the option directly
                            option = await target_frame.query_selector('select option:nth-child(2)')
                            if option:
                                await option.click(force=True, timeout=2000)
                                logger.info("Force-clicked first option using select option selector")
                            else:
                                # Last resort - click by JS
                                await target_frame.evaluate("""() => {
                                    // Try to click the first visible option in all selects
                                    const selects = document.querySelectorAll('select');
                                    for (const select of selects) {
                                        if (select.options.length > 1) {
                                            // Try to click the option directly
                                            const option = select.options[1]; // Use second option (index 1)
                                            option.click();
                                            return true;
                                        }
                                    }
                                    return false;
                                }""")
                                logger.info("Clicked first option via direct JavaScript click")
                        except Exception as click_err:
                            logger.warning(f"Error clicking option: {click_err}")
                            # Final fallback - use dispatchEvent
                            await target_frame.evaluate("""() => {
                                const select = document.querySelector('select');
                                if (select && select.options.length > 1) {
                                    // First select the option
                                    select.selectedIndex = 1;
                                    // Create and dispatch click events on the option
                                    const option = select.options[1];
                                    const evt = new MouseEvent('click', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    option.dispatchEvent(evt);
                                    // Also trigger change event on select
                                    select.dispatchEvent(new Event('change', {bubbles: true}));
                                }
                            }""")
                            logger.info("Force-triggered click event on option as last resort")
                    
                    lote_result = {'success': True, 'message': 'Selected first option in dropdown'}
                except Exception as e:
                    logger.warning(f"All attempts to select dropdown option failed: {e}")
                    # Continue with the fallback JavaScript approach outside this block
                    raise PlaywrightTimeoutError(f"Could not select dropdown option: {e}")
                
                lote_result = {'success': True, 'message': 'Selected LOTE option using Playwright selectors'}
            except PlaywrightTimeoutError as e:
                logger.warning(f"Could not find or click LOTE dropdown using robust selectors: {e}")
                # Fallback to the existing JavaScript approach
                lote_result = await target_frame.evaluate("""() => {
                    try {
                        // Try multiple approaches to find the Lote dropdown with TUBO options
                        
                        // Helper function to check visibility
                        function isElementVisible(el) {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            return style.display !== 'none' && 
                                  style.visibility !== 'hidden' && 
                                  style.opacity !== '0' &&
                                  el.offsetParent !== null;
                        }
                        
                        // 1. Search specifically for select with TUBO options
                        const allSelects = document.querySelectorAll('select');
                        let loteSelect = null;
                        let tuboOptionsFound = false;
                        
                        for (let select of allSelects) {
                            // Check options for "TUBO" text
                            const options = Array.from(select.options);
                            const hasTuboOption = options.some(option => 
                                option.textContent && option.textContent.includes('TUBO'));
                                
                            if (hasTuboOption) {
                                loteSelect = select;
                                tuboOptionsFound = true;
                                break;
                            }
                            
                            // Check for options with values starting with "1000"
                            const has1000Option = options.some(option => 
                                option.value && option.value.startsWith('1000'));
                                
                            if (has1000Option) {
                                loteSelect = select;
                                tuboOptionsFound = true;
                                break;
                            }
                        }
                        
                        // 2. If not found above, look for the HTMLLookupDetails element from the screenshot
                        if (!tuboOptionsFound) {
                            const lookupDetails = document.querySelectorAll('.HTMLLookupDetails, div[class*="Lookup"]');
                            for (const lookup of lookupDetails) {
                                if (isElementVisible(lookup)) {
                                    const selectInLookup = lookup.querySelector('select');
                                    if (selectInLookup && selectInLookup.options.length > 0) {
                                        loteSelect = selectInLookup;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // 3. Look for a dropdown inside lookupInput container
                        if (!loteSelect) {
                            const lookupInput = document.getElementById('lookupInput');
                            if (lookupInput) {
                                const selectInLookup = lookupInput.querySelector('select');
                                if (selectInLookup && selectInLookup.options.length > 0) {
                                    loteSelect = selectInLookup;
                                }
                            }
                        }
                        
                        // If we found a select element, try to select an option
                        if (loteSelect) {
                            // Get information about available options
                            const optionsInfo = Array.from(loteSelect.options).map((opt, idx) => ({
                                index: idx,
                                value: opt.value || 'no-value',
                                text: opt.textContent?.trim() || 'no-text'
                            }));
                            
                            // Check for TUBO options specifically
                            const tuboOptions = optionsInfo.filter(opt => 
                                opt.text.includes('TUBO') || 
                                (opt.value && opt.value.startsWith('1000')));
                                
                            // If we found TUBO options, select the first one
                            if (tuboOptions.length > 0) {
                                const selectedIndex = tuboOptions[0].index;
                                loteSelect.selectedIndex = selectedIndex;
                                loteSelect.dispatchEvent(new Event('change', { bubbles: true }));
                                
                                return { 
                                    success: true, 
                                    message: `Selected TUBO option at index ${selectedIndex}`,
                                    selectInfo: {
                                        id: loteSelect.id || 'no-id',
                                        name: loteSelect.name || 'no-name',
                                        options: optionsInfo,
                                        selectedIndex: selectedIndex,
                                        selectedText: optionsInfo[selectedIndex]?.text || 'unknown',
                                        selectedValue: optionsInfo[selectedIndex]?.value || 'unknown',
                                        tuboOptionsFound: tuboOptions.length
                                    }
                                };
                            } 
                            else {
                                // If no TUBO options, select the first non-placeholder option
                                let selectedIndex = 0;
                                
                                // Skip index 0 if it looks like a placeholder
                                if (optionsInfo.length > 1 && 
                                    (optionsInfo[0].text === 'no-text' || 
                                     optionsInfo[0].value === 'no-value' ||
                                     optionsInfo[0].text.toLowerCase().includes('selec') ||
                                     optionsInfo[0].text === '-' ||
                                     !optionsInfo[0].value)) {
                                    selectedIndex = 1;
                                }
                                
                                // Make sure we have a valid index
                                if (selectedIndex < loteSelect.options.length) {
                                    // Try to select by index and trigger change event
                                    loteSelect.selectedIndex = selectedIndex;
                                    loteSelect.dispatchEvent(new Event('change', { bubbles: true }));
                                    
                                    // Return success info
                                    return { 
                                        success: true, 
                                        message: `Selected fallback option at index ${selectedIndex}`,
                                        selectInfo: {
                                            id: loteSelect.id || 'no-id',
                                            name: loteSelect.name || 'no-name',
                                            options: optionsInfo,
                                            selectedIndex: selectedIndex,
                                            selectedText: optionsInfo[selectedIndex]?.text || 'unknown',
                                            selectedValue: optionsInfo[selectedIndex]?.value || 'unknown',
                                            warning: 'No TUBO options found, selected fallback'
                                        }
                                    };
                                } else {
                                    return {
                                        success: false,
                                        message: "No valid options found in dropdown",
                                        options: optionsInfo
                                    };
                                }
                            }
                        }
                        
                        // Extra debugging - try to get the innerHTML of possible lookup containers
                        let lookupDetailsHTML = '';
                        const possibleLookupContainer = document.querySelector('.HTMLLookupDetails, div[class*="Lookup"]');
                        if (possibleLookupContainer) {
                            lookupDetailsHTML = possibleLookupContainer.innerHTML.substring(0, 500) + '...'; // Truncate to avoid excessive output
                        }
                        
                        // Diagnostic information if we can't find the dropdown
                        const allSelectsInfo = Array.from(allSelects).map(select => ({
                            id: select.id || 'no-id',
                            name: select.name || 'no-name',
                            optionCount: select.options.length,
                            optionSample: Array.from(select.options).slice(0, 3).map(o => o.textContent),
                            parent: select.parentElement ? select.parentElement.tagName : 'no-parent',
                            visible: isElementVisible(select)
                        }));
                        
                        return { 
                            success: false, 
                            message: "Could not find appropriate LOTE dropdown with TUBO options",
                            selectsFound: allSelects.length,
                            selects: allSelectsInfo,
                            lookupDetailsHTML: lookupDetailsHTML
                        };
                    } catch (e) {
                        return { error: e.toString() };
                    }
                }""")
            
            logger.info(f"Lote selection result: {lote_result}")
            
            # --- Step 3: Enter CNPJ ---
            # First find all visible text inputs to identify the CNPJ field
            visible_inputs = await target_frame.evaluate("""() => {
                try {
                    // Get all input elements that are visible
                    const inputs = Array.from(document.querySelectorAll('input[type="text"]')).filter(el => {
                        // Check if the element is visible
                        const style = window.getComputedStyle(el);
                        return style.display !== 'none' && 
                               style.visibility !== 'hidden' && 
                               style.opacity !== '0' &&
                               el.offsetParent !== null; // Element is visible in the layout
                    });
                    
                    return inputs.map(input => ({
                        id: input.id || 'no-id',
                        name: input.name || 'no-name',
                        placeholder: input.placeholder || 'no-placeholder',
                        value: input.value || '',
                        boundingBox: input.getBoundingClientRect().toJSON(),
                        isDisabled: input.disabled,
                        isReadOnly: input.readOnly
                    }));
                } catch (e) {
                    return { error: e.toString() };
                }
            }""")
            
            logger.info(f"Found {len(visible_inputs)} visible text inputs")
            
            # Identify which input is likely the CNPJ field
            cnpj_input = None
            if isinstance(visible_inputs, list) and len(visible_inputs) > 0:
                # Try to find CNPJ input based on various heuristics
                for i, input_info in enumerate(visible_inputs):
                    # Skip disabled or readonly inputs
                    if input_info.get('isDisabled', False) or input_info.get('isReadOnly', False):
                        continue
                        
                    # Check various attributes for CNPJ-related hints
                    input_id = input_info.get('id', '').lower()
                    input_name = input_info.get('name', '').lower()
                    input_placeholder = input_info.get('placeholder', '').lower()
                    
                    if ('cnpj' in input_id or 'cpf' in input_id or 
                        'cnpj' in input_name or 'cpf' in input_name or 
                        'cnpj' in input_placeholder or 'cpf' in input_placeholder):
                        logger.info(f"Found likely CNPJ input field at index {i}: {input_info}")
                        cnpj_input_selector = f"input[name='{input_info['name']}']" if input_info['name'] != 'no-name' else f"input[id='{input_info['id']}']"
                        cnpj_input = await target_frame.query_selector(cnpj_input_selector)
                        break
                
                # If no specific CNPJ field found, use the first visible input as a fallback
                if not cnpj_input and len(visible_inputs) > 0:
                    first_input = visible_inputs[0]
                    logger.info(f"No specific CNPJ field found. Using first visible input as fallback: {first_input}")
                    first_input_selector = f"input[name='{first_input['name']}']" if first_input['name'] != 'no-name' else f"input[id='{first_input['id']}']"
                    cnpj_input = await target_frame.query_selector(first_input_selector)
            
            cnpj_result = None
            if cnpj_input:
                try:
                    # Type CNPJ like a human would - character by character with slight delays
                    await simulate_human_typing(target_frame, cnpj_input, CNPJ_TO_USE)
                    logger.info(f"Typed CNPJ {CNPJ_TO_USE} into input field character by character")
                    cnpj_result = {'success': True, 'message': 'CNPJ typed manually character by character'}
                except Exception as typing_err:
                    logger.error(f"Error typing CNPJ: {typing_err}")
                    # Fallback to the original JavaScript approach
                    cnpj_result = await target_frame.evaluate("""(cnpj) => {
                        try {
                            // Try multiple approaches to find the CNPJ input - but only visible ones
                            const inputs = Array.from(document.querySelectorAll('input[type="text"]')).filter(el => {
                                // Check if the element is visible
                                const style = window.getComputedStyle(el);
                                return style.display !== 'none' && 
                                       style.visibility !== 'hidden' && 
                                       style.opacity !== '0' &&
                                       el.offsetParent !== null;
                            });
                            
                            if (inputs.length > 0) {
                                // Use the first visible input
                                const input = inputs[0];
                                input.value = cnpj;
                                input.dispatchEvent(new Event('input', { bubbles: true }));
                                input.dispatchEvent(new Event('change', { bubbles: true }));
                                return { 
                                    success: true, 
                                    message: "CNPJ entered via JS fallback",
                                    inputInfo: {
                                        id: input.id || 'no-id',
                                        name: input.name || 'no-name',
                                        type: input.type || 'no-type'
                                    }
                                };
                            }
                            
                            return { 
                                success: false, 
                                message: "No visible CNPJ input found"
                            };
                        } catch (e) {
                            return { error: e.toString() };
                        }
                    }""", CNPJ_TO_USE)
            else:
                logger.warning("No visible input fields found for CNPJ entry")
                cnpj_result = {'success': False, 'message': 'No visible input fields found'}
            
            logger.info(f"CNPJ entry result: {cnpj_result}")
            
            # --- Step 4: Click CONSULTAR button ---
            consultar_result = await target_frame.evaluate("""() => {
                try {
                    // Try to find consultar button
                    let consultarBtn = Array.from(document.querySelectorAll('button')).find(
                        b => b.textContent && b.textContent.trim().toUpperCase() === 'CONSULTAR'
                    );
                    
                    if (consultarBtn) {
                        consultarBtn.click();
                        return { success: true, message: "Clicked CONSULTAR button" };
                    }
                    
                    return { 
                        success: false, 
                        message: "CONSULTAR button not found",
                        buttonCount: document.querySelectorAll('button').length,
                        buttonTexts: Array.from(document.querySelectorAll('button')).map(b => b.textContent.trim()).slice(0, 5)
                    };
                } catch (e) {
                    return { error: e.toString() };
                }
            }""")
            
            logger.info(f"CONSULTAR button result: {consultar_result}")
            
            # Take screenshot after form interaction
            await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_2_after_form.png"))
            
            # --- Step 5: Wait for and click SALVAR ---
            await popup_page.wait_for_timeout(5000)  # Give time for SALVAR to appear
            
            # Set up dialog handler
            popup_page.once('dialog', handle_dialog)
            
            salvar_result = await target_frame.evaluate("""() => {
                try {
                    // Try to find salvar button
                    let salvarBtn = Array.from(document.querySelectorAll('button')).find(
                        b => b.textContent && b.textContent.trim().toUpperCase() === 'SALVAR'
                    );
                    
                    if (salvarBtn) {
                        salvarBtn.click();
                        return { success: true, message: "Clicked SALVAR button" };
                    }
                    
                    return { 
                        success: false, 
                        message: "SALVAR button not found",
                        buttonCount: document.querySelectorAll('button').length,
                        buttonTexts: Array.from(document.querySelectorAll('button')).map(b => b.textContent.trim()).slice(0, 5)
                    };
                } catch (e) {
                    return { error: e.toString() };
                }
            }""")
            
            logger.info(f"SALVAR button result: {salvar_result}")
            
            # Give time for dialog and subsequent UI changes
            await popup_page.wait_for_timeout(5000)
            
            # Take screenshot after SALVAR
            await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_3_after_salvar.png"))
            
            # --- Step 6: Look for and click download button ---
            # Wait longer after SALVAR to ensure UI updates
            await popup_page.wait_for_timeout(8000)  # Extended from 5000ms to 8000ms
            
            # Take another screenshot to see the state before attempting to find the download button
            await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_4_before_download.png"))
            
            # Use a more aggressive approach to check for download buttons or links
            download_result = await target_frame.evaluate("""() => {
                try {
                    // Scroll down to reveal potential hidden elements
                    window.scrollTo(0, document.body.scrollHeight);
                    
                    // Try multiple potential selectors for download elements
                    const downloadElements = [];
                    
                    // 1. Look for buttons with "download" text
                    const downloadButtons = Array.from(document.querySelectorAll('button')).filter(
                        b => b.textContent && b.textContent.trim().toUpperCase().includes('DOWNLOAD')
                    );
                    downloadElements.push(...downloadButtons.map(b => ({
                        type: 'button',
                        text: b.textContent.trim(),
                        visible: isElementVisible(b),
                        index: Array.from(document.querySelectorAll('button')).indexOf(b)
                    })));
                    
                    // 2. Look for links with "download" text
                    const downloadLinks = Array.from(document.querySelectorAll('a')).filter(
                        a => a.textContent && a.textContent.trim().toUpperCase().includes('DOWNLOAD')
                    );
                    downloadElements.push(...downloadLinks.map(a => ({
                        type: 'link',
                        text: a.textContent.trim(),
                        href: a.href,
                        visible: isElementVisible(a),
                        index: Array.from(document.querySelectorAll('a')).indexOf(a)
                    })));
                    
                    // 3. Look for any elements with a download attribute
                    const downloadAttrs = Array.from(document.querySelectorAll('[download]'));
                    downloadElements.push(...downloadAttrs.map(el => ({
                        type: el.tagName.toLowerCase(),
                        download: el.getAttribute('download'),
                        href: el.href,
                        visible: isElementVisible(el),
                        index: Array.from(document.querySelectorAll(el.tagName)).indexOf(el)
                    })));
                    
                    // Helper function to check visibility
                    function isElementVisible(el) {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        return style.display !== 'none' && 
                               style.visibility !== 'hidden' && 
                               style.opacity !== '0' &&
                               el.offsetParent !== null;
                    }
                    
                    // Attempt to make buttons visible if they're not
                    downloadButtons.forEach(btn => {
                        if (!isElementVisible(btn)) {
                            try {
                                // Try to make it visible for interaction
                                btn.style.display = 'block';
                                btn.style.visibility = 'visible';
                                btn.style.opacity = '1';
                                
                                // Also try parent elements
                                let parent = btn.parentElement;
                                for (let i = 0; i < 3 && parent; i++, parent = parent.parentElement) {
                                    parent.style.display = 'block';
                                    parent.style.visibility = 'visible';
                                    parent.style.opacity = '1';
                                }
                            } catch (e) {
                                // Ignore errors during style manipulation
                            }
                        }
                    });
                    
                    if (downloadElements.length > 0) {
                        // First try to find a visible element
                        const visibleElements = downloadElements.filter(el => el.visible);
                        if (visibleElements.length > 0) {
                            return { 
                                success: true, 
                                message: `Found visible download element: ${visibleElements[0].type}`,
                                element: visibleElements[0]
                            };
                        }
                        
                        // If none are visible, return the first one anyway
                        return { 
                            success: true, 
                            message: `Found non-visible download element: ${downloadElements[0].type}`,
                            element: downloadElements[0],
                            allElements: downloadElements
                        };
                    }
                    
                    // Collect all possible interactive elements for debugging
                    const allButtons = Array.from(document.querySelectorAll('button')).map(b => ({
                        type: 'button',
                        text: b.textContent.trim(),
                        visible: isElementVisible(b)
                    }));
                    
                    const allLinks = Array.from(document.querySelectorAll('a')).map(a => ({
                        type: 'link',
                        text: a.textContent.trim(),
                        href: a.href,
                        visible: isElementVisible(a)
                    })).filter(a => a.text || a.href); // Filter out empty links
                    
                    return { 
                        success: false, 
                        message: "No download elements found",
                        buttonsFound: allButtons.length,
                        linksFound: allLinks.length,
                        sampleButtons: allButtons.slice(0, 5),
                        sampleLinks: allLinks.slice(0, 5)
                    };
                } catch (e) {
                    return { error: e.toString() };
                }
            }""")
            
            logger.info(f"Download element search result: {download_result}")
            
            if download_result.get('success', False):
                element_info = download_result.get('element', {})
                element_type = element_info.get('type', '')
                
                try:
                    if element_type == 'button':
                        # Try to directly use JavaScript to click the button
                        logger.info(f"Attempting to click download button via JavaScript...")
                        
                        # Try direct JavaScript click
                        await target_frame.evaluate("""(index) => {
                            try {
                                const buttons = document.querySelectorAll('button');
                                if (index >= 0 && index < buttons.length) {
                                    // Try to ensure button is visible and clickable
                                    const btn = buttons[index];
                                    btn.style.display = 'block';
                                    btn.style.visibility = 'visible';
                                    btn.style.opacity = '1';
                                    
                                    // Scroll to the button
                                    btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                                    
                                    // Give time for scrolling
                                    setTimeout(() => {
                                        // Both click methods for redundancy
                                        btn.click();
                                        
                                        // Alternate click method
                                        const clickEvent = new MouseEvent('click', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window
                                        });
                                        btn.dispatchEvent(clickEvent);
                                    }, 300);
                                    
                                    return true;
                                }
                                return false;
                            } catch (e) {
                                console.error("Error clicking button:", e);
                                return false;
                            }
                        }""", element_info.get('index', -1))
                        
                        # Track multiple downloads and wait for all to complete
                        all_downloads = []
                        download_complete = False
                        max_download_wait_time = 300000  # 5 minutes max wait time for large files
                        
                        try:
                            # Set up initial download expectation with extended timeout
                            logger.info("Setting up download expectation with extended timeout...")
                            async with popup_page.expect_download(timeout=max_download_wait_time) as download_info:
                                # Additional wait time to allow the javascript click to complete
                                await popup_page.wait_for_timeout(2000)
                                
                                # If JS click didn't work, try Playwright's click as fallback
                                # with forced state to try to overcome visibility issues
                                if element_info.get('index', -1) >= 0:
                                    try:
                                        download_buttons = await target_frame.locator('button').all()
                                        if len(download_buttons) > element_info.get('index', -1):
                                            download_button = download_buttons[element_info['index']]
                                            
                                            # Force-click using Playwright (with 'force' option)
                                            logger.info(f"Fallback: Trying force-click with Playwright...")
                                            await download_button.click(force=True, timeout=10000)
                                    except Exception as click_err:
                                        logger.warning(f"Fallback Playwright click failed: {click_err}")
                                
                                # Wait for first download to start
                                logger.info("Waiting for download to start...")
                                download = await download_info.value
                                all_downloads.append(download)
                                logger.info(f"First download started: {download.suggested_filename}")
                            
                            # Keep waiting for additional downloads (some sites trigger multiple files)
                            # using a short polling approach
                            start_time = time.time()
                            consecutive_no_downloads = 0
                            
                            while time.time() - start_time < 60:  # Check for additional downloads for up to 60 seconds
                                try:
                                    # Short timeout for additional downloads
                                    async with popup_page.expect_download(timeout=5000) as next_download_info:
                                        # Just wait for the next download, no additional interaction needed
                                        next_download = await next_download_info.value
                                        all_downloads.append(next_download)
                                        logger.info(f"Additional download detected: {next_download.suggested_filename}")
                                        consecutive_no_downloads = 0  # Reset counter when we find a download
                                except PlaywrightTimeoutError:
                                    # No new download in this period
                                    consecutive_no_downloads += 1
                                    if consecutive_no_downloads >= 3:  # If no new downloads for ~15 seconds, we're probably done
                                        logger.info("No new downloads detected for a while, assuming all downloads started.")
                                        break
                            
                            # Process all the downloads
                            if all_downloads:
                                downloaded_files = []
                                
                                # Save all downloads
                                for i, download in enumerate(all_downloads):
                                    suggested_filename = download.suggested_filename or f"{processo_id}_edital_{i+1}.pdf"
                                    target_path = os.path.join(temp_process_dir, clean_filename(suggested_filename))
                                    
                                    logger.info(f"Saving download {i+1}/{len(all_downloads)} to: {target_path}")
                                    try:
                                        # Wait for each download to complete with extended timeout
                                        await download.save_as(target_path)
                                        
                                        if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
                                            logger.info(f"Download {i+1} successful: {target_path} ({os.path.getsize(target_path)} bytes)")
                                            downloaded_files.append(target_path)
                                        else:
                                            logger.error(f"Download {i+1} file is empty or missing: {target_path}")
                                    except Exception as save_err:
                                        logger.error(f"Error saving download {i+1}: {save_err}")
                                
                                # Update the downloaded_file_path to include all files
                                if downloaded_files:
                                    downloaded_file_path = downloaded_files[0]  # For backward compatibility
                                    if len(downloaded_files) > 1:
                                        logger.info(f"Multiple files downloaded: {len(downloaded_files)} files")
                                else:
                                    logger.error("No files were successfully downloaded")
                                
                                download_complete = len(downloaded_files) > 0
                            else:
                                logger.error("No downloads were detected")
                        
                        except PlaywrightTimeoutError:
                            logger.error("Timed out waiting for download to start")
                        except Exception as dl_err:
                            logger.error(f"Error during download attempt: {dl_err}", exc_info=True)
                        
                    elif element_type == 'link':
                        # For links, try to extract the href and download directly
                        href = element_info.get('href', '')
                        if href:
                            logger.info(f"Attempting to download from link URL: {href}")
                            
                            # Download using direct HTTP request
                            target_path = os.path.join(temp_process_dir, f"{processo_id}_edital.pdf")
                            
                            # Use requests to download the file
                            response = requests.get(href, stream=True, timeout=30, verify=False)
                            if response.status_code == 200:
                                with open(target_path, 'wb') as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        f.write(chunk)
                                
                                if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
                                    logger.info(f"Direct link download successful: {target_path} ({os.path.getsize(target_path)} bytes)")
                                    downloaded_file_path = target_path
                                else:
                                    logger.error(f"Direct link download file is empty or missing: {target_path}")
                            else:
                                logger.error(f"Direct link download failed with status code: {response.status_code}")
                        else:
                            logger.error("Link found but no href attribute")
                    
                    else:
                        logger.warning(f"Unsupported download element type: {element_type}")
                
                except PlaywrightTimeoutError:
                    logger.error("Timed out waiting for download to start")
                except Exception as dl_err:
                    logger.error(f"Error during download attempt: {dl_err}", exc_info=True)
            else:
                logger.error("No download button or link found")
        else:
            logger.warning("No suitable iframe found in popup. Cannot process.")
            await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_no_iframe.png"))

        # --- Create ZIP if we have a downloaded file ---
        if downloaded_file_path and os.path.exists(downloaded_file_path):
            zip_filename = clean_filename(f"{processo_id}_{objeto_clean}.zip", max_length=150)
            zip_filepath = os.path.join(output_dirs['archive'], zip_filename)
            if create_zip_archive([downloaded_file_path], zip_filepath):
                final_zip_success = True
                logger.info(f"Created ZIP archive: {zip_filepath}")
            else:
                logger.error(f"Failed to create final ZIP for {processo_id}")
        else:
            logger.error(f"No document was successfully downloaded for {processo_id}. No ZIP created.")

    except Exception as e:
        logger.error(f"Critical error processing detail popup for {processo_id}: {e}", exc_info=True)
        try:
             await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_{processo_id}_9_critical_error.png"))
        except Exception: pass # Ignore screenshot error during critical failure
        final_zip_success = False
    finally:
        # Clean up temp directory for this process
        try:
            if os.path.exists(temp_process_dir):
                shutil.rmtree(temp_process_dir)
                logger.debug(f"Cleaned up temp directory: {temp_process_dir}")
        except Exception as cleanup_e:
            logger.error(f"Error cleaning temp dir {temp_process_dir}: {cleanup_e}")

    logger.info(f"--- Finished Processing Detail Popup for Process: {processo_id} | Success: {final_zip_success} ---")
    return final_zip_success

async def process_search_page(page: Page, context: BrowserContext, search_url: str, output_dirs: Dict[str, str], processed_state: Set[str]) -> Set[str]:
    """Processes the main listing page using accessibility APIs, filters by keywords, and triggers detail popup processing."""
    newly_processed_in_this_run = set()
    try:
        logger.info(f"Navigating to Compesa search page: {search_url}")
        await page.goto(search_url, wait_until="networkidle", timeout=90000)
        
        # Take initial screenshot for debugging
        await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_page_initial.png"))
        
        # --- Target the main content iframe ---
        frame_handle = await page.wait_for_selector('iframe', timeout=30000)
        if not frame_handle:
            logger.error("Could not find iframe. Aborting.")
            return newly_processed_in_this_run
            
        frame = await frame_handle.content_frame()
        if not frame:
            logger.error("Could not access iframe content. Aborting.")
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_page_iframe_error.png"))
            return newly_processed_in_this_run
            
        logger.info("Located iframe. Waiting for content to load...")
        await page.wait_for_timeout(5000)  # Give time for iframe content to load

        # --- Use accessibility approach to find all list items (not just the first one) ---
        list_items_info = await frame.evaluate("""() => {
            try {
                // Find all list items
                const listItems = document.querySelectorAll('[role="listitem"]');
                const result = [];
                
                // Extract data from each list item
                for (let i = 0; i < listItems.length; i++) {
                    const item = listItems[i];
                    const itemText = item.textContent || '';
                    
                    // Look for process number using pattern matching (LC format)
                    const processMatch = itemText.match(/LC\\s+\\d+\\/\\d+\\s+[A-Z]+-\\d+/);
                    const processId = processMatch ? processMatch[0].trim() : '';
                    
                    // Get the whole text for keyword matching
                    result.push({
                        index: i,
                        text: itemText,
                        processId: processId,
                        // Find image elements which might be clickable triggers
                        hasImg: !!item.querySelector('img')
                    });
                }
                
                return {
                    count: listItems.length,
                    items: result
                };
            } catch (e) {
                return { error: e.toString(), count: 0, items: [] };
            }
        }""")
        
        if 'error' in list_items_info and list_items_info['error']:
            logger.warning(f"Error in list items evaluation: {list_items_info['error']}")
        
        if list_items_info['count'] > 0:
            logger.info(f"Found {list_items_info['count']} list items in frame using direct evaluation")
            
            # Process all list items, not just the first one
            rows_to_process = []
            
            for item in list_items_info['items']:
                item_text = item['text']
                processo_id = item['processId']
                
                # Skip if we couldn't extract a process ID
                if not processo_id:
                    logger.debug(f"Could not extract process ID from list item {item['index']}")
                    continue
                
                # Check for any of the handler's KEYWORDS (not just "BORBOLETAS")
                item_text_upper = item_text.upper()
                matching_keywords = [keyword for keyword in KEYWORDS if keyword.upper() in item_text_upper]
                
                if matching_keywords:
                    # Clean up the object text to use as filename
                    objeto_clean = clean_filename(item_text, max_length=60)
                    
                    # Check if already processed
                    if processo_id in processed_state:
                        logger.debug(f"Process {processo_id} already processed (from state file). Skipping.")
                        continue
                    if processo_id in newly_processed_in_this_run:
                        logger.debug(f"Process {processo_id} already processed (this run). Skipping.")
                        continue
                    
                    logger.info(f"Keyword match [{', '.join(matching_keywords)}] for process {processo_id}. Queuing.")
                    
                    # Queue for processing with the index to locate it later
                    rows_to_process.append({
                        "list_item_index": item['index'],
                        "processo_id": processo_id,
                        "objeto_clean": objeto_clean,
                        "matching_keywords": matching_keywords
                    })
            
            # --- Process all the identified rows ---
            logger.info(f"Processing popups for {len(rows_to_process)} new processes matching keywords...")
            for data in rows_to_process:
                processo_id = data['processo_id']
                objeto_clean = data['objeto_clean']
                item_index = data['list_item_index']
                popup_page = None
                
                try:
                    logger.info(f"Handling popup for process {processo_id} (matched keywords: {data['matching_keywords']})...")
                    
                    # Click the trigger (image) in the list item
                    success = await frame.evaluate("""(index) => {
                        try {
                            const listItems = document.querySelectorAll('[role="listitem"]');
                            if (index >= listItems.length) return { success: false, error: 'Index out of range' };
                            
                            const item = listItems[index];
                            const imgElement = item.querySelector('img');
                            
                            if (!imgElement) return { success: false, error: 'No image element found' };
                            
                            // Click the image element
                            imgElement.click();
                            return { success: true };
                        } catch (e) {
                            return { success: false, error: e.toString() };
                        }
                    }""", item_index)
                    
                    if not success.get('success', False):
                        logger.error(f"Failed to click trigger for {processo_id}: {success.get('error', 'Unknown error')}")
                        continue
                    
                    logger.debug(f"Clicked trigger for {processo_id}")
                    
                    # Wait for the popup to appear
                    async with context.expect_page(timeout=30000) as popup_info:
                        # The click already happened in the evaluation
                        await page.wait_for_timeout(2000)  # Wait a bit to ensure popup opens
                    
                    popup_page = await popup_info.value
                    logger.info(f"Popup window opened for {processo_id}")
                    
                    # Wait for popup content to be loaded
                    await popup_page.wait_for_load_state("domcontentloaded", timeout=60000)
                    
                    # Process the popup with your existing function
                    success = await process_detail_popup(
                        popup_page, context, processo_id, objeto_clean, output_dirs
                    )
                    
                    if success:
                        logger.info(f"Successfully processed popup and downloaded for {processo_id}")
                        newly_processed_in_this_run.add(processo_id)
                    else:
                        logger.warning(f"Failed processing popup for {processo_id}")
                        
                except PlaywrightTimeoutError:
                    logger.error(f"Timed out waiting for popup to open or load for {processo_id}")
                except Exception as popup_err:
                    logger.error(f"Error during popup handling for {processo_id}: {popup_err}", exc_info=True)
                    if popup_page:
                        try:
                            await popup_page.screenshot(path=os.path.join(output_dirs['screenshots'], f"popup_processing_error_{processo_id}.png"))
                        except Exception:
                            pass  # Ignore screenshot errors during failure
                finally:
                    # Ensure popup is closed even if errors occurred
                    if popup_page and not popup_page.is_closed():
                        try:
                            await popup_page.close()
                            logger.debug(f"Closed popup for {processo_id}")
                        except Exception as close_err:
                            logger.warning(f"Error closing popup for {processo_id}: {close_err}")
                    # Add a small delay before processing the next row's trigger
                    await page.wait_for_timeout(1000)
            
        else:
            logger.warning("No list items found using direct evaluation.")
            # Take a screenshot to help diagnose the issue
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"no_list_items_found.png"))
            
            # Try to get more diagnostic information about what's in the frame
            frame_content = await frame.evaluate("""() => {
                return {
                    bodyContent: document.body ? document.body.innerHTML.substring(0, 1000) : 'No body element',
                    elementCount: document.querySelectorAll('*').length,
                    roles: Array.from(document.querySelectorAll('[role]')).map(el => el.getAttribute('role'))
                };
            }""")
            
            logger.debug(f"Frame content diagnostic: {frame_content}")
    
    except PlaywrightTimeoutError as pte:
        logger.error(f"Timeout error on search page {search_url}: {pte}")
        try:
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_timeout_error.png"))
        except Exception:
            pass
    except Exception as e:
        logger.error(f"General error processing search page {search_url}: {e}", exc_info=True)
        try:
            await page.screenshot(path=os.path.join(output_dirs['screenshots'], f"search_general_error.png"))
        except Exception:
            pass
    
    return newly_processed_in_this_run

async def handle_compesa_download(url, output_dir, timeout=300):
    """Main handler function for Compesa downloads."""
    playwright = None; browser = None; context = None
    overall_success = False; processed_new_items_count = 0; error_message = None
    try:
        output_dirs = setup_output_dirs(output_dir)
        processed_state = load_processed_state(output_dirs)

        playwright = await async_playwright().start()
        headless = False # Start non-headless for easier debugging
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
        context.set_default_timeout(45000) # Default action timeout
        context.set_default_navigation_timeout(90000) # Navigation timeout

        page = await context.new_page()

        # Process the main search/listing page
        newly_processed = await process_search_page(page, context, url, output_dirs, processed_state)

        processed_new_items_count = len(newly_processed)
        if newly_processed:
            updated_state = processed_state.union(newly_processed)
            save_processed_state(output_dirs, updated_state)
            overall_success = True
        else:
            logger.info(f"No new processes matching keywords found or processed on {url} in this run.")
            overall_success = True # Considered success as the script ran, just no new data

    except Exception as e:
        logger.critical(f"Critical error in handle_compesa_download for {url}: {e}", exc_info=True)
        overall_success = False
        error_message = f"Critical error: {e}"
    finally:
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
        "file_path": None, # Output is ZIPs, not a single file
        "error_message": error_message,
        "processed_new_items_count": processed_new_items_count
    }
    print(json.dumps(result))
    return 0 if overall_success else 1

async def main():
    """Parses arguments and runs the handler."""
    parser = argparse.ArgumentParser(description="Compesa Download Handler")
    parser.add_argument("--url", required=True, help="URL of the Compesa listing page (e.g., ...formID=7...)")
    parser.add_argument("--output-dir", default=base_download_dir, help=f"Base output directory (defaults to: {base_download_dir})")
    parser.add_argument("--timeout", type=int, default=600, help="Overall timeout for the handler in seconds (approximate)")
    args = parser.parse_args()
    output_dir_abs = os.path.abspath(args.output_dir)
    exit_code = await handle_compesa_download(args.url, output_dir_abs, args.timeout)
    sys.exit(exit_code)

if __name__ == "__main__":
    if sys.version_info >= (3, 7):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())