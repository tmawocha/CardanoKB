"""
fetch_epochs.py
===============
Endpoint group 1: Epochs

Fetches the latest epoch and walks back MAX_EPOCHS_BACK epochs to define
the temporal scope of the entire ingestion pipeline.

Output saved to: cache/epochs/
"""

import json
import logging
from pathlib import Path

from config import CACHE_DIR, MAX_EPOCHS_BACK
from fetcher import BlockfrostClient

log = logging.getLogger(__name__)


def fetch_epochs(client: BlockfrostClient) -> list[dict]:
    """
    Returns a list of epoch detail objects for the last MAX_EPOCHS_BACK epochs,
    ordered newest → oldest.
    """
    log.info("── Fetching epochs (last %d) ──", MAX_EPOCHS_BACK)

    latest = client.get("/epochs/latest")
    if not latest:
        raise RuntimeError("Could not fetch latest epoch from Blockfrost.")

    current_epoch = latest["epoch"]
    log.info("Current epoch: %d", current_epoch)

    epochs = [latest]

    for n in range(current_epoch - 1, current_epoch - MAX_EPOCHS_BACK, -1):
        epoch_data = client.get(f"/epochs/{n}")
        if epoch_data:
            epochs.append(epoch_data)
        else:
            log.warning("Could not fetch epoch %d — stopping walkback.", n)
            break

    out_path = Path(CACHE_DIR) / "epochs" / "epochs_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(epochs, f, indent=2)

    log.info("Saved %d epochs to %s", len(epochs), out_path)
    return epochs


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = BlockfrostClient()
    epochs = fetch_epochs(client)
