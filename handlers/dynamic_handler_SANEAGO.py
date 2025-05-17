#!/usr/bin/env python3
"""
SANEAGO Download Handler for Procurement Download System

Handles downloading procurement documents from SANEAGO's procurement portal
using Stagehand JavaScript automation. This handler is designed to be called
by the dispatcher.

Workflow:
1. Receives a URL, company_id, output directory, and timeout.
2. Sets up logging and output directories under the main output_dir.
3. Ensures Node.js and npm dependencies for the Stagehand script are ready.
   (Optimized to skip 'npm install' if node_modules and package-lock.json exist).
4. Executes the Stagehand JavaScript handler (handler_saneago.js) via Node.js,
   passing the target URL and a Stagehand-specific temporary download directory.
5. The Stagehand script performs the download and places files (expected as a single .zip)
   in its temporary download directory.
6. This Python handler moves the downloaded .zip file from the Stagehand temporary
   location to the designated 'archives' directory within the main output_dir.
7. Returns a JSON result indicating success/failure, the original URL,
   the path to the downloaded archive (if successful), and any error messages.

Usage (CLI for testing):
    python dynamic_handler_SANEAGO.py --company-id <COMPANY_ID> --url <URL> --output-dir <DIR> [--timeout SECONDS]
"""

import os
import sys
import json
import time
import argparse
import logging
import subprocess
import shutil
import glob
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional, Any, List

# --- Constants ---
HANDLER_NAME = "SANEAGOHandler"

# Script directory to locate Stagehand resources
SCRIPT_ABS_PATH = Path(__file__).resolve()
HANDLER_DIR = SCRIPT_ABS_PATH.parent
STAGEHAND_DIR = HANDLER_DIR / "stagehand"
STAGEHAND_HANDLER_SCRIPT_NAME = "handler_saneago.js"
STAGEHAND_HANDLER_SCRIPT_PATH = STAGEHAND_DIR / STAGEHAND_HANDLER_SCRIPT_NAME

ARCHIVE_SUBDIR_NAME = "archives"
LOG_SUBDIR_NAME = "logs"
TEMP_SUBDIR_NAME = "temp"
STAGEHAND_TEMP_DOWNLOADS_SUBDIR_NAME = "stagehand_saneago_dl" # Specific to this handler's Stagehand output

# Placeholder logger, will be replaced by setup_logging
logger = logging.getLogger(f"{HANDLER_NAME}_placeholder")

# --- Logging Configuration ---
def setup_logging(log_output_dir: str, company_id: str, handler_name_prefix: str = HANDLER_NAME) -> logging.Logger:
    """Configures and returns a logger for the handler."""
    current_logger = logging.getLogger(handler_name_prefix)
    # Clear any existing handlers to prevent duplicate logging or conflicts if re-initialized
    if current_logger.hasHandlers():
        current_logger.handlers.clear()
    current_logger.propagate = False # Prevent duplicate logs in parent/root loggers

    log_dir_path = Path(log_output_dir) / LOG_SUBDIR_NAME
    log_dir_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = f"dynamic_handler_{handler_name_prefix.lower()}_{company_id}_{timestamp}.log"
    log_file_path = log_dir_path / log_file_name

    # Create file handler
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Create stream handler (for console output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') # Simpler for console
    stream_handler.setFormatter(stream_formatter)

    current_logger.addHandler(file_handler)
    current_logger.addHandler(stream_handler)
    current_logger.setLevel(logging.INFO) # Default to INFO

    return current_logger

# --- Directory Setup ---
def setup_output_dirs(base_output_dir: str, current_logger: logging.Logger) -> Dict[str, Path]:
    """Ensure all necessary output directories exist relative to the base_output_dir."""
    base_dir_path = Path(base_output_dir).resolve()
    current_logger.info(f"Setting up output directories relative to: {base_dir_path}")

    output_dirs_map = {
        'base': base_dir_path,
        'archive': base_dir_path / ARCHIVE_SUBDIR_NAME,
        'logs': base_dir_path / LOG_SUBDIR_NAME, # Ensured by setup_logging
        'temp': base_dir_path / TEMP_SUBDIR_NAME,
        'stagehand_temp_downloads': base_dir_path / TEMP_SUBDIR_NAME / STAGEHAND_TEMP_DOWNLOADS_SUBDIR_NAME,
    }

    for dir_key, dir_path_obj in output_dirs_map.items():
        try:
            dir_path_obj.mkdir(parents=True, exist_ok=True)
            current_logger.debug(f"Directory ensured: {dir_path_obj}")
        except OSError as e:
            current_logger.error(f"OSError creating directory {dir_path_obj}: {e}")
            # For critical directories, re-raise to halt execution if they can't be created
            if dir_key in ['archive', 'temp', 'stagehand_temp_downloads']:
                 current_logger.critical(f"Failed to create critical directory: {dir_path_obj}")
                 raise
    return output_dirs_map

# --- Core Stagehand Execution ---
def _run_stagehand_subprocess(
    target_url: str,
    output_dirs: Dict[str, Path],
    timeout_seconds: int,
    current_logger: logging.Logger
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Runs the Stagehand JavaScript handler via Node.js and manages downloaded files.
    Assumes handler_saneago.js takes --url and --output-dir arguments.
    Returns: (success, final_zip_path_str, error_message)
    """
    try:
        current_logger.info(f"Starting Stagehand process for URL: {target_url} (timeout: {timeout_seconds}s)")

        # Check Node.js installation
        try:
            node_version_proc = subprocess.run(
                ["node", "--version"], check=True, capture_output=True, text=True, timeout=10
            )
            current_logger.info(f"Node.js version: {node_version_proc.stdout.strip()}")
        except subprocess.TimeoutExpired:
            msg = "Timeout checking Node.js version."
            current_logger.error(msg)
            return False, None, msg
        except (subprocess.CalledProcessError, FileNotFoundError):
            msg = "Node.js is not installed or not found in PATH. Please install Node.js."
            current_logger.error(msg)
            return False, None, msg
        except Exception as e:
            msg = f"Unexpected error checking Node.js: {e}"
            current_logger.error(msg)
            return False, None, msg

        # Ensure npm dependencies are installed in STAGEHAND_DIR
        current_logger.info(f"Ensuring npm dependencies are installed in {STAGEHAND_DIR}...")
        try:
            if not (STAGEHAND_DIR / "node_modules").exists() or not (STAGEHAND_DIR / "package-lock.json").exists():
                current_logger.info("Missing node_modules or package-lock.json. Running 'npm install'...")
                npm_proc = subprocess.run(
                    ["npm", "install"], cwd=str(STAGEHAND_DIR), check=True,
                    capture_output=True, text=True, timeout=300 # 5 min timeout for npm install
                )
                current_logger.info(f"'npm install' stdout:\n{npm_proc.stdout}")
                if npm_proc.stderr:
                    current_logger.warning(f"'npm install' stderr:\n{npm_proc.stderr}")
            else:
                current_logger.info("Skipping 'npm install' as node_modules and package-lock.json exist.")
        except subprocess.TimeoutExpired:
            msg = "Timeout during 'npm install'."
            current_logger.error(msg)
            return False, None, msg
        except subprocess.CalledProcessError as e:
            msg = f"'npm install' failed: {e.stderr}"
            current_logger.error(msg)
            return False, None, msg
        except FileNotFoundError:
            msg = "'npm' command not found. Ensure Node.js (which includes npm) is installed and in PATH."
            current_logger.error(msg)
            return False, None, msg
        except Exception as e:
            msg = f"Unexpected error during 'npm install': {e}"
            current_logger.error(msg)
            return False, None, msg

        stagehand_temp_dl_dir = output_dirs['stagehand_temp_downloads']
        cmd = [
            "node", str(STAGEHAND_HANDLER_SCRIPT_PATH),
            "--url", target_url,
            "--output-dir", str(stagehand_temp_dl_dir)
        ]
        current_logger.info(f"Executing Stagehand command: {' '.join(cmd)}")

        process = subprocess.run(
            cmd, cwd=str(STAGEHAND_DIR), check=False, # check=False to handle errors manually
            capture_output=True, text=True, timeout=timeout_seconds
        )

        if process.stdout:
            current_logger.info(f"Stagehand handler stdout:\n{process.stdout}")
        if process.stderr:
            # Log stderr as error if non-zero exit, warning otherwise
            log_func = current_logger.error if process.returncode != 0 else current_logger.warning
            log_func(f"Stagehand handler stderr:\n{process.stderr}")

        if process.returncode == 0:
            current_logger.info("Stagehand handler completed successfully. Verifying downloaded files...")
            # Expecting Saneago_*.zip in the stagehand_temp_dl_dir
            zip_pattern = str(stagehand_temp_dl_dir / "Saneago_*.zip")
            downloaded_zips = glob.glob(zip_pattern)

            if not downloaded_zips:
                 # Check if Stagehand reported a specific success path in stdout
                if "DOWNLOAD_SUCCESS_PATH:" in process.stdout:
                    try:
                        reported_path_str = process.stdout.split("DOWNLOAD_SUCCESS_PATH:")[1].splitlines()[0].strip()
                        reported_path = Path(reported_path_str)
                        if reported_path.exists() and reported_path.is_file() and reported_path.name.lower().endswith(".zip"):
                            downloaded_zips = [str(reported_path)] # Use the path reported by Stagehand
                            current_logger.info(f"Stagehand reported successful download: {reported_path_str}")
                        else:
                            current_logger.warning(f"Stagehand reported path {reported_path_str}, but it's not a valid zip file.")
                    except IndexError:
                        current_logger.warning("Could not parse DOWNLOAD_SUCCESS_PATH from Stagehand stdout.")
                
                if not downloaded_zips:
                    msg = "Stagehand completed, but no 'Saneago_*.zip' file found in temporary download directory."
                    current_logger.error(msg)
                    return False, None, msg

            if len(downloaded_zips) > 1:
                current_logger.warning(f"Multiple 'Saneago_*.zip' files found: {downloaded_zips}. Using the first one.")
            
            src_zip_path = Path(downloaded_zips[0])
            dest_archive_dir = output_dirs['archive']
            dest_zip_path = dest_archive_dir / src_zip_path.name
            
            try:
                shutil.move(str(src_zip_path), str(dest_zip_path))
                current_logger.info(f"Successfully moved '{src_zip_path.name}' to '{dest_zip_path}'")
                return True, str(dest_zip_path), None
            except Exception as e:
                msg = f"Error moving '{src_zip_path.name}' to '{dest_zip_path}': {e}"
                current_logger.exception(msg)
                return False, None, msg
        else:
            error_msg = f"Stagehand handler failed with exit code {process.returncode}. See logs and stderr for details."
            current_logger.error(error_msg)
            # Include stderr in the returned error message if it's concise enough, or refer to logs
            detailed_error = error_msg + (f" Stderr: {process.stderr[:500]}..." if process.stderr else "")
            return False, None, detailed_error

    except subprocess.TimeoutExpired:
        msg = f"Stagehand handler timed out after {timeout_seconds} seconds for URL {target_url}."
        current_logger.error(msg)
        return False, None, msg
    except Exception as e:
        msg = f"An unexpected error occurred while running Stagehand for URL {target_url}: {e}"
        current_logger.exception(msg)
        return False, None, msg

# --- Main Handler Function ---
def run_handler(
    company_id: str,
    url: str,
    output_dir: str,
    timeout: int = 1800  # Default timeout 30 minutes
) -> Dict[str, Any]:
    """Main entry point for the SANEAGO download handler."""
    global logger # Allow reassignment of the global logger variable
    logger = setup_logging(output_dir, company_id, HANDLER_NAME)
    
    logger.info(f"=== Starting SANEAGO Handler for Company ID: {company_id}, URL: {url} ===")
    
    final_file_path: Optional[str] = None
    error_message: Optional[str] = None
    success = False

    try:
        output_dirs = setup_output_dirs(output_dir, logger)
        success, final_file_path, error_message = _run_stagehand_subprocess(
            target_url=url,
            output_dirs=output_dirs,
            timeout_seconds=timeout,
            current_logger=logger
        )
    except Exception as e:
        logger.exception(f"Critical error in run_handler before or during Stagehand execution: {e}")
        success = False
        error_message = f"Critical handler error: {e}"

    result = {
        "success": success,
        "company_id": company_id,
        "url": url,
        "file_path": final_file_path,
        "error_message": error_message,
        "handler_name": HANDLER_NAME
    }
    
    if success:
        logger.info(f"SANEAGO Handler completed successfully for URL: {url}. Archive: {final_file_path}")
    else:
        logger.error(f"SANEAGO Handler failed for URL: {url}. Error: {error_message}")
    logger.info(f"=== Finished SANEAGO Handler for Company ID: {company_id}, URL: {url} ===")
    
    return result

# --- CLI Execution (for testing) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"{HANDLER_NAME} - Test CLI")
    parser.add_argument("--company-id", required=True, help="Company ID for logging and context.")
    parser.add_argument("--url", required=True, help="Target URL for SANEAGO procurement page/document.")
    parser.add_argument("--output-dir", required=True, help="Base directory for all outputs (logs, archives, temp). Example: ./downloads/saneago_run_1")
    parser.add_argument("--timeout", type=int, default=1800, help="Timeout in seconds for the entire Stagehand process (default: 1800s / 30min).")
    
    args = parser.parse_args()
    
    # Initialize a basic logger for CLI execution if run_handler's setup_logging fails early
    # This logger will be replaced by the one setup in run_handler
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(f"{HANDLER_NAME}_CLI")

    handler_result = run_handler(
        company_id=args.company_id,
        url=args.url,
        output_dir=args.output_dir,
        timeout=args.timeout
    )
    
    # Print JSON result to stdout for orchestration or capture
    print(json.dumps(handler_result, indent=2))
    
    sys.exit(0 if handler_result["success"] else 1)