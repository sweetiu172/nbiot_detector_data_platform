#!/bin/sh
# entrypoint.sh

# Exit immediately if a command exits with a non-zero status.
set -e

# 1. Apply the feature repository configuration
echo "🚀 Applying Feast repository..."
feast apply

# 2. Determine the end date for materialization
# Use the first argument passed to the script ($1), or default to today's date in YYYY-MM-DD format.
END_DATE=${1:-$(date -u +%Y-%m-%d)}

echo "🍲 Materializing features up to end date: $END_DATE"

# 3. Run the incremental materialization
feast materialize-incremental $END_DATE

echo "✅ Feast setup and materialization complete."