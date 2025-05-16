#!/usr/bin/env python3
"""
CESAN Download Handler for Procurement Download System

Handles downloading procurement documents from CESAN's procurement portal
using Stagehand JavaScript automation.

Usage:
    python dynamic_handler_CESAN.py --url URL --output-dir DIR [--timeout SECONDS]
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

# --- Logging Configuration ---
script_dir = os.path.dirname(os.path.abspath(__file__))
base_download_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_dir = os.path.join(base_download_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = f"dynamic_handler_cesan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("CESANHandler")

# Define paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STAGEHAND_DIR = os.path.join(SCRIPT_DIR, "stagehand")
HANDLER_SCRIPT = os.path.join(STAGEHAND_DIR, "handler_cesan.js")

def setup_output_dirs(base_dir):
    """Ensure all necessary output directories exist"""
    output_dirs = {
        'pdf': os.path.join(base_dir, 'pdfs'),
        'archive': os.path.join(base_dir, 'archives'),
        'debug': os.path.join(base_dir, 'debug'),
        'logs': os.path.join(base_dir, 'logs'),
        'screenshots': os.path.join(base_dir, 'debug', 'screenshots'),
        'temp': os.path.join(base_dir, 'temp'),
        'stagehand_downloads': os.path.join(STAGEHAND_DIR, 'downloads_cesan')
    }
    
    for dir_path in output_dirs.values():
        os.makedirs(dir_path, exist_ok=True)
        
    return output_dirs

def run_stagehand_handler(output_dir, timeout):
    """Run the Stagehand JavaScript handler via Node.js"""
    try:
        # Setup directories
        output_dirs = setup_output_dirs(output_dir)
        
        logger.info(f"Starting CESAN Stagehand handler (timeout: {timeout}s)")
        
        # Check if Node.js is installed
        try:
            node_version = subprocess.run(
                ["node", "--version"], 
                check=True, 
                capture_output=True, 
                text=True
            ).stdout.strip()
            logger.info(f"Node.js version: {node_version}")
        except subprocess.CalledProcessError:
            return False, None, "Node.js is not installed or not in PATH"
        except Exception as e:
            return False, None, f"Error checking Node.js: {str(e)}"
        
        # Make sure npm modules are installed
        logger.info("Ensuring npm dependencies are installed...")
        try:
            subprocess.run(
                ["npm", "install"], 
                cwd=STAGEHAND_DIR, 
                check=True, 
                capture_output=True
            )
        except subprocess.CalledProcessError as e:
            error_output = e.stderr.decode('utf-8', errors='replace')
            logger.error(f"Failed to install npm dependencies: {error_output}")
            return False, None, f"Failed to install npm dependencies: {error_output}"
        
        # Run the JavaScript handler
        logger.info(f"Executing Stagehand handler: {HANDLER_SCRIPT} with model gemini-2.0-flash-lite")
        process = subprocess.run(
            ["node", HANDLER_SCRIPT, "--output-dir", os.path.abspath(output_dir)],
            cwd=STAGEHAND_DIR,
            check=False, 
            capture_output=True,
            timeout=timeout
        )
        
        stdout = process.stdout.decode('utf-8', errors='replace')
        stderr = process.stderr.decode('utf-8', errors='replace')
        
        # Log the output regardless of success/failure
        if stdout:
            logger.info(f"Handler stdout:\n{stdout}")
        if stderr:
            logger.warning(f"Handler stderr:\n{stderr}")
        
        # Handle results - find any zip files created in the output directory
        if process.returncode == 0:
            logger.info("Handler completed successfully, looking for downloaded files")
            
            # Look for ZIP files in the stagehand download directories
            zip_pattern = os.path.join(output_dirs['stagehand_downloads'], "Cesan_*.zip")
            zip_files = glob.glob(zip_pattern)
            
            if not zip_files:
                logger.warning("No zip files found in the Stagehand download directory")
                return False, None, "No files were downloaded"
            
            # Move ZIP files to the procurement-downloader archives directory
            moved_files = []
            for src_zip in zip_files:
                zip_filename = os.path.basename(src_zip)
                dest_zip = os.path.join(output_dirs['archive'], zip_filename)
                try:
                    shutil.move(src_zip, dest_zip)
                    logger.info(f"Moved {zip_filename} to {dest_zip}")
                    moved_files.append(dest_zip)
                except Exception as e:
                    logger.error(f"Error moving {src_zip} to {dest_zip}: {e}")
            
            if moved_files:
                return True, moved_files[0], None  # Return the first moved file as the file_path
            else:
                return False, None, "Failed to move any downloaded files"
        else:
            error_msg = f"Handler failed with exit code {process.returncode}: {stderr}"
            logger.error(error_msg)
            return False, None, error_msg
            
    except subprocess.TimeoutExpired:
        logger.error(f"Handler timed out after {timeout} seconds")
        return False, None, f"Handler timed out after {timeout} seconds"
    except Exception as e:
        logger.exception(f"Error running handler: {str(e)}")
        return False, None, f"Error running handler: {str(e)}"

def main():
    """Main entry point for the handler"""
    parser = argparse.ArgumentParser(description="CESAN Download Handler")
    parser.add_argument("--url", required=True, help="URL to download from")
    parser.add_argument("--output-dir", default="downloads", help="Output directory")
    parser.add_argument("--timeout", type=int, default=1800, help="Timeout in seconds")
    
    args = parser.parse_args()
    
    # Run the handler
    success, file_path, error_message = run_stagehand_handler(
        args.output_dir,
        args.timeout
    )
    
    # Return results as JSON
    result = {
        "success": success,
        "url": args.url,
        "file_path": file_path,
        "error_message": error_message
    }
    
    # Print JSON for the orchestrator to capture
    print(json.dumps(result))
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 