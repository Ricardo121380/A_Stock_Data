set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/Ricardo121380/A_Stock_Data.git}"
APP_DIR="${APP_DIR:-/home/$USER/A_Stock_Data}"
DATA_SOURCE="${DATA_SOURCE:-akshare}"
PRICE_SOURCE="${PRICE_SOURCE:-akshare}"
REQUEST_SLEEP="${REQUEST_SLEEP:-0.6}"
MAX_RETRIES="${MAX_RETRIES:-5}"
RETRY_BACKOFF="${RETRY_BACKOFF:-2}"
MODE="${MODE:-full}"
END_DATE="${END_DATE:-}"
RUN_BACKGROUND="${RUN_BACKGROUND:-1}"
LOG_FILE="${LOG_FILE:-run.log}"

if command -v apt >/dev/null 2>&1; then
  sudo apt update
  sudo apt install -y git python3 python3-venv python3-pip
elif command -v yum >/dev/null 2>&1; then
  sudo yum install -y git python3 python3-venv python3-pip || sudo yum install -y git python3 python3-pip
fi

if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" pull --rebase
else
  git clone "$REPO_URL" "$APP_DIR"
fi

cd "$APP_DIR"
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

export DATA_SOURCE
export PRICE_SOURCE
export REQUEST_SLEEP
export MAX_RETRIES
export RETRY_BACKOFF

python -m src.main init

if [ "$MODE" = "full" ]; then
  CMD="python -m src.main full"
else
  if [ -n "$END_DATE" ]; then
    CMD="python -m src.main update --end-date $END_DATE"
  else
    CMD="python -m src.main update"
  fi
fi

if [ "$RUN_BACKGROUND" = "1" ]; then
  nohup bash -c "$CMD" > "$LOG_FILE" 2>&1 &
  echo "$LOG_FILE"
else
  exec bash -c "$CMD"
fi
