#!/bin/bash
# Postino plugin setup - runs after installation
set -e
cd "$(dirname "$0")/.."
npm install --production=false 2>/dev/null
npm run build 2>/dev/null
echo "postino: setup complete"
