#!/bin/bash

# Define paths
REPO_DIR="/home/collector/homelab-monitoring"
VENV_DIR="$REPO_DIR/.venv"
LOG_FILE="$REPO_DIR/scheduler.log"

cd "$REPO_DIR" || exit

# 1. Fetch latest changes
git fetch origin main

# 2. Get the current and remote hashes
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

# 3. Check for updates
if [ "$LOCAL" != "$REMOTE" ]; then
    echo "[$(date)] Update found! Pulling changes..." >> "$LOG_FILE"
    
    # Reset to match remote exactly (safer than merge for bots)
    git reset --hard origin/main
    
    # Re-install requirements in case they changed
    source "$VENV_DIR/bin/activate"
    pip install -r requirements.txt
    
    echo "[$(date)] Update complete. Service will restart." >> "$LOG_FILE"
    
    # If we are running via systemd, we might want to just exit
    # and let systemd restart us. But for now, let's just fall through.
else
    echo "[$(date)] No updates found. Starting scheduler..."
fi

# 4. Run the Scheduler (Infinite Loop)
source "$VENV_DIR/bin/activate"
# Point to the local configs folder
python src/collector/scheduler.py --configs-dir src/configs
