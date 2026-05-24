#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$BASE_DIR/.bot.pid"
LOG_FILE="$BASE_DIR/bot.log"
VENV_PYTHON="$BASE_DIR/.venv/bin/python"

print_usage() {
  cat <<'EOF'
Uso: ./botctl.sh <comando>

Comandos:
  start     Inicia o bot em background
  stop      Para o bot
  restart   Reinicia o bot
  status    Mostra status atual
  logs      Mostra logs em tempo real (tail -f)
EOF
}

require_python() {
  if [[ -x "$VENV_PYTHON" ]]; then
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    VENV_PYTHON="$(command -v python3)"
    return 0
  fi

  echo "Erro: Python nao encontrado (.venv/bin/python ou python3)." >&2
  exit 1
}

is_running() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi

  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi

  if kill -0 "$pid" 2>/dev/null; then
    return 0
  fi

  return 1
}

list_running_pids() {
  ps -eo pid=,args= \
    | grep -E "python(3)? .*bot.py" \
    | grep -v grep \
    | awk '{print $1}'
}

repair_pid_file_if_needed() {
  if is_running; then
    return 0
  fi

  local running
  running="$(list_running_pids | tr '\n' ' ' | xargs echo -n || true)"
  if [[ -n "$running" ]]; then
    local pid
    pid="$(echo "$running" | awk '{print $NF}')"
    echo "$pid" > "$PID_FILE"
    echo "Aviso: PID file ausente/desatualizado. Adotando processo em execucao (PID $pid)."
  fi
}

start_bot() {
  require_python
  repair_pid_file_if_needed

  if is_running; then
    echo "Bot ja esta rodando (PID $(cat "$PID_FILE"))."
    return 0
  fi

  rm -f "$PID_FILE"
  cd "$BASE_DIR"
  nohup "$VENV_PYTHON" bot.py >> "$LOG_FILE" 2>&1 &

  local pid=$!
  echo "$pid" > "$PID_FILE"

  sleep 1
  if kill -0 "$pid" 2>/dev/null; then
    echo "Bot iniciado (PID $pid)."
  else
    echo "Falha ao iniciar o bot. Veja $LOG_FILE." >&2
    rm -f "$PID_FILE"
    exit 1
  fi
}

stop_bot() {
  repair_pid_file_if_needed

  local pids
  pids="$(list_running_pids | tr '\n' ' ' | xargs echo -n || true)"
  if [[ -z "$pids" ]]; then
    echo "Bot nao esta rodando."
    rm -f "$PID_FILE"
    return 0
  fi

  local pid
  for pid in $pids; do
    kill "$pid" 2>/dev/null || true
  done

  for _ in {1..20}; do
    local still_running
    still_running="$(list_running_pids | tr '\n' ' ' | xargs echo -n || true)"
    if [[ -z "$still_running" ]]; then
      break
    fi
    sleep 0.2
  done

  local remaining
  remaining="$(list_running_pids | tr '\n' ' ' | xargs echo -n || true)"
  if [[ -n "$remaining" ]]; then
    for pid in $remaining; do
      kill -9 "$pid" 2>/dev/null || true
    done
  fi

  rm -f "$PID_FILE"
  echo "Bot parado."
}

status_bot() {
  repair_pid_file_if_needed

  local pids
  pids="$(list_running_pids | tr '\n' ' ' | xargs echo -n || true)"
  if [[ -n "$pids" ]]; then
    echo "Status: rodando (PIDs $pids)."
    return 0
  fi

  echo "Status: parado."
}

logs_bot() {
  touch "$LOG_FILE"
  tail -f "$LOG_FILE"
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    start)
      start_bot
      ;;
    stop)
      stop_bot
      ;;
    restart)
      stop_bot
      start_bot
      ;;
    status)
      status_bot
      ;;
    logs)
      logs_bot
      ;;
    *)
      print_usage
      exit 1
      ;;
  esac
}

main "$@"