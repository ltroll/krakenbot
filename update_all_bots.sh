#!/usr/bin/env bash

ENV_FILE=".env"

# Load .env file
if [[ -f "$ENV_FILE" ]]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo ".env file not found."
    exit 1
fi

# Ensure variable exists
if [[ -z "$BOT_DIR_LIST_FILE" ]]; then
    echo "BOT_DIR_LIST_FILE is not set in .env"
    exit 1
fi

# Ensure list file exists
if [[ ! -f "$BOT_DIR_LIST_FILE" ]]; then
    echo "Directory list file not found: $BOT_DIR_LIST_FILE"
    exit 1
fi

echo "Starting bot updates..."
echo "Using directory list: $BOT_DIR_LIST_FILE"
echo "----------------------------------"

while IFS= read -r dir
do
    [[ -z "$dir" ]] && continue

    echo ""
    echo "Checking directory: $dir"

    if [[ ! -d "$dir" ]]; then
        echo "Directory does not exist. Skipping."
        continue
    fi

    cd "$dir" || continue

    if [[ ! -d ".git" ]]; then
        echo "Not a git repository. Skipping."
        continue
    fi

    echo "Running git status..."
    git status -sb

    echo "Running git pull..."
    git pull --rebase --autostash

done < "$BOT_DIR_LIST_FILE"

echo ""
echo "All updates complete."
