import json
import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

MANIFEST_PATH = "manifest.json"

# Gets the most recent tag reachable from the current commit (similar to hatch-vcs)
# so MCPB manifest version matches the package version
result = subprocess.run(
    ["git", "describe", "--tags", "--abbrev=0"],
    capture_output=True,
    text=True,
    check=True,
)
version = result.stdout.strip().lstrip("v")

if not version:
    logger.error("Could not determine version from git tags")
    sys.exit(1)

# Update manifest.json
with open(MANIFEST_PATH, "r+") as f:
    data = json.load(f)
    data["version"] = version
    f.seek(0)
    json.dump(data, f, indent=4)
    f.truncate()

logging.info(f"Wrote MCPB {MANIFEST_PATH} version: {version}")
