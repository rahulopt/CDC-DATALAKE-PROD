#!/usr/bin/env bash
# ===========================================================================
# Run the LOCAL end-to-end CDC pipeline demo (no AWS needed).
#
# Handles the two macOS gotchas automatically:
#   * unsets a shadowing system SPARK_HOME
#   * prefers openjdk@17 (Spark 3.5 is not compatible with Java 21)
#
# Usage:  scripts/run_local_demo.sh
# ===========================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# Activate the project venv if present.
if [ -d ".venv" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# Pick a Spark-compatible JDK (17) if available, else fall back to JAVA_HOME.
if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

unset SPARK_HOME
export SPARK_LOCAL_IP=127.0.0.1

echo ">>> python: $(command -v python)"
echo ">>> java  : $(java -version 2>&1 | head -1)"
echo ">>> Running local CDC pipeline demo (first run downloads the Iceberg jar)..."
echo

python scripts/run_local_demo.py
