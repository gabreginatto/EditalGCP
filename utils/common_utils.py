import os
import tempfile
import shutil
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CommonUtilsError(Exception):
    """Custom exception for common_utils errors."""
    pass

def create_temp_run_dir(base_temp_dir: str | None = None, prefix: str = "run_") -> str:
    """
    Creates a unique temporary directory for a processing run.

    Args:
        base_temp_dir: Optional. The base directory in which to create the temporary directory.
                       If None, uses the system's default temporary directory.
        prefix: A prefix for the temporary directory name, e.g., 'run_'
    
    Returns:
        The absolute path to the created temporary directory.
    
    Raises:
        CommonUtilsError: If directory creation fails.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    dir_name = f"{prefix}{timestamp}"
    
    if base_temp_dir:
        if not os.path.exists(base_temp_dir):
            try:
                os.makedirs(base_temp_dir, exist_ok=True)
            except OSError as e:
                raise CommonUtilsError(f"Failed to create base temporary directory {base_temp_dir}: {e}") from e
        temp_dir_path = os.path.join(base_temp_dir, dir_name)
        try:
            os.makedirs(temp_dir_path, exist_ok=True)
        except OSError as e:
            raise CommonUtilsError(f"Failed to create temporary directory {temp_dir_path}: {e}") from e
    else:
        # If no base_temp_dir, use tempfile.mkdtemp for system default tmp location
        try:
            temp_dir_path = tempfile.mkdtemp(prefix=prefix)
        except OSError as e:
            raise CommonUtilsError(f"Failed to create temporary directory using tempfile.mkdtemp: {e}") from e
            
    logger.info(f"Created temporary run directory: {temp_dir_path}")
    return temp_dir_path

def cleanup_temp_dir(dir_path: str):
    """
    Removes the specified directory and all its contents.

    Args:
        dir_path: The absolute path to the directory to be removed.
    
    Raises:
        CommonUtilsError: If directory removal fails.
    """
    if not dir_path or not isinstance(dir_path, str):
        logger.warning("cleanup_temp_dir called with invalid dir_path: {dir_path}")
        return

    if os.path.exists(dir_path):
        try:
            shutil.rmtree(dir_path)
            logger.info(f"Successfully cleaned up temporary directory: {dir_path}")
        except OSError as e:
            logger.error(f"Error cleaning up temporary directory {dir_path}: {e}", exc_info=True)
            raise CommonUtilsError(f"Failed to remove directory {dir_path}: {e}") from e
    else:
        logger.info(f"Temporary directory not found (already cleaned up or never created): {dir_path}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    print("--- Testing common_utils.py ---")
    
    custom_base = "./temp_test_runs"
    created_dirs = []

    # Test 1: Create temp dir in system default location
    print("\nTest 1: Creating temp directory in system default location...")
    try:
        sys_temp_dir = create_temp_run_dir(prefix="sys_default_test_")
        print(f"System temp dir created: {sys_temp_dir}")
        created_dirs.append(sys_temp_dir)
        if not os.path.exists(sys_temp_dir):
            print(f"ERROR: Directory {sys_temp_dir} was reported created but does not exist.")
    except CommonUtilsError as e:
        print(f"Error: {e}")

    # Test 2: Create temp dir in a custom base directory
    print(f"\nTest 2: Creating temp directory in custom base '{custom_base}'...")
    try:
        custom_temp_dir = create_temp_run_dir(base_temp_dir=custom_base, prefix="custom_base_test_")
        print(f"Custom base temp dir created: {custom_temp_dir}")
        created_dirs.append(custom_temp_dir)
        if not os.path.exists(custom_temp_dir):
            print(f"ERROR: Directory {custom_temp_dir} was reported created but does not exist.")
    except CommonUtilsError as e:
        print(f"Error: {e}")

    # Test 3: Cleanup
    print("\nTest 3: Cleaning up created directories...")
    for d in created_dirs:
        print(f"Attempting to clean up: {d}")
        try:
            cleanup_temp_dir(d)
            if os.path.exists(d):
                print(f"ERROR: Directory {d} still exists after cleanup attempt.")
            else:
                print(f"Successfully cleaned: {d}")
        except CommonUtilsError as e:
            print(f"Error cleaning up {d}: {e}")
    
    # Cleanup the custom base directory if it was created and is empty
    if os.path.exists(custom_base):
        try:
            if not os.listdir(custom_base): # Check if empty
                os.rmdir(custom_base)
                print(f"Successfully removed empty custom base directory: {custom_base}")
            else:
                print(f"Custom base directory '{custom_base}' is not empty, not removing.")
        except OSError as e:
            print(f"Error removing custom base directory '{custom_base}': {e}")

    print("\n--- Finished common_utils.py tests ---")
