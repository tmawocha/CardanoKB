"""
config.py
=========
Central configuration for the Cardano Governance KG pipeline.
Set your Blockfrost project_id in the BLOCKFROST_API_KEY variable,
or export it as an environment variable before running.
"""

import os

BLOCKFROST_API_KEY = os.environ.get("BLOCKFROST_API_KEY", "YOUR_PROJECT_ID_HERE")

BASE_URL = "https://cardano-mainnet.blockfrost.io/api/v0"

# Scope limits 
MAX_POOLS        = 100
MAX_EPOCHS_BACK  = 20
PAGE_SIZE        = 100   # Blockfrost max per page

# ── Rate limiting 
# Blockfrost allows 10 req/s with burst of 500
REQUEST_DELAY_SECONDS = 0.12   # ~8 req/s — safely under the limit
MAX_RETRIES           = 5
RETRY_BACKOFF_BASE    = 2      # exponential backoff: 2^attempt seconds

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

