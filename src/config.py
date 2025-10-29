"""Configuration file for CONUS404 data download and processing."""

import datetime as dt
from typing import Dict
import numpy as np

# Date range for download
START_DATE = dt.date(1988, 3, 31)
END_DATE = dt.date(1988, 4, 17)

# Variable aggregation map: True = intensive (average), False = extensive (sum)
VARIABLE_AGG_MAP: Dict[str, bool] = {
    "T2": True,      # Temperature - average
    "Q2": True,      # Specific humidity - average
    "TD2": True,     # Dewpoint - average
    "PSFC": True,    # Surface pressure - average
    "ACRAINLSM": False, # Accumulated rain - sum
    "LAI": True,     # Leaf area index - average
    "U10": True,     # U wind component - average
    "V10": True,     # V wind component - average
    "Z": True,       # Geopotential height - average
}

# Derived variables to calculate
DERIVED_VARS = {
    "W": {
        "depends_on": ("U10", "V10"),
        "intensive": True,
        "calc_fn": lambda u, v: np.sqrt(u**2 + v**2)
    }
}

# Subprocess settings (replaces CONCURRENT_DAYS)
MAX_CONCURRENT_PROCESSES = 8  # Number of parallel subprocess workers

# Directory paths
DATA_DIR = "data"
LOG_DIR = "logs"

# Memory monitoring settings
MEMORY_CHECK_INTERVAL = 30  # seconds between memory checks
MEMORY_WARNING_THRESHOLD = 85  # percent
MEMORY_CRITICAL_THRESHOLD = 90  # percent
