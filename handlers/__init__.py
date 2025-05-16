# This file makes the 'handlers' directory a Python package.
# It could also contain a factory function or common utilities for handlers if needed.

# Example: A mapping of company_id to handler module names
# This would be used by handlers/__main__.py
COMPANY_HANDLER_MAP = {
    "CAGECE": "handlers.dynamic_handler_CAGECE",
    "CESAN": "handlers.dynamic_handler_CESAN", # Assuming one Cesan handler is primary or __main__ picks
    # "CESAN_OLD": "handlers.handler_cesan", # If you need to differentiate
    "SANEAGO": "handlers.dynamic_handler_SANEAGO",
    "SANEPAR": "handlers.dynamic_handler_SANEPAR",
    "COMPESA_ACOMP": "handlers.dynamic_handler_compesa_acompanhamento",
    "COMPESA_AVISO": "handlers.dynamic_handler_compesa_aviso",
    "COPASA": "handlers.dynamic_handler_copasa",
    # Add other mappings as needed
}
