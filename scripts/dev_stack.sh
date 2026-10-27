#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${ROOT_DIR}/.data"
PG_DATA="${DATA_DIR}/postgres"
REDIS_DATA="${DATA_DIR}/redis"
LOGS_DIR="${DATA_DIR}/logs"
PID_DIR="${DATA_DIR}/pids"

# Export environments
export PATH="${ROOT_DIR}/.venv/bin:/home/omar/.local/bin:/home/omar/.local/postgres/usr/lib/postgresql/14/bin:${PATH}"
export LD_LIBRARY_PATH="/home/omar/.local/postgres/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
export PGLIB="/home/omar/.local/postgres/usr/lib/postgresql/14/lib"
export PYTHONPATH="${ROOT_DIR}:${PYTHONPATH:-}"

mkdir -p "${LOGS_DIR}" "${PID_DIR}" "${REDIS_DATA}"

is_running() {
    local pid_file="$1"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

start_postgres() {
    if is_running "${PID_DIR}/postgres.pid"; then
        echo "[Postgres] Already running (PID: $(cat "${PID_DIR}/postgres.pid"))"
        return 0
    fi

    if [ ! -d "${PG_DATA}" ] || [ ! -f "${PG_DATA}/PG_VERSION" ]; then
        echo "[Postgres] Initializing database cluster at ${PG_DATA}..."
        mkdir -p "${PG_DATA}"
        initdb -D "${PG_DATA}" -U pulse_user --auth=trust -E UTF8 > "${LOGS_DIR}/initdb.log" 2>&1
        echo "listen_addresses = '127.0.0.1'" >> "${PG_DATA}/postgresql.conf"
        echo "port = 5432" >> "${PG_DATA}/postgresql.conf"
        echo "unix_socket_directories = '/tmp'" >> "${PG_DATA}/postgresql.conf"
        echo "synchronous_commit = off" >> "${PG_DATA}/postgresql.conf"
        echo "wal_writer_delay = 10ms" >> "${PG_DATA}/postgresql.conf"
        echo "max_connections = 250" >> "${PG_DATA}/postgresql.conf"
        echo "shared_buffers = 256MB" >> "${PG_DATA}/postgresql.conf"
    fi

    echo "[Postgres] Starting postgres..."
    pg_ctl -D "${PG_DATA}" -l "${LOGS_DIR}/postgres.log" start
    
    # Wait for postgres to be ready
    for i in {1..30}; do
        if pg_isready -h 127.0.0.1 -p 5432 -U pulse_user >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done

    # Create pulse database if not exists
    psql -h 127.0.0.1 -p 5432 -U pulse_user -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'pulse'" | grep -q 1 || \
        psql -h 127.0.0.1 -p 5432 -U pulse_user -d postgres -c "CREATE DATABASE pulse OWNER pulse_user;" >/dev/null 2>&1 || true

    pg_pid=$(head -n 1 "${PG_DATA}/postmaster.pid" 2>/dev/null || true)
    if [ -n "$pg_pid" ]; then
        echo "$pg_pid" > "${PID_DIR}/postgres.pid"
        echo "[Postgres] Ready on port 5432 (PID: $pg_pid)"
    fi
}

start_redis() {
    if is_running "${PID_DIR}/redis.pid"; then
        echo "[Redis] Already running (PID: $(cat "${PID_DIR}/redis.pid"))"
        return 0
    fi

    echo "[Redis] Starting redis-server..."
    redis-server --port 6379 \
        --dir "${REDIS_DATA}" \
        --daemonize yes \
        --pidfile "${PID_DIR}/redis.pid" \
        --logfile "${LOGS_DIR}/redis.log" \
        --save "" \
        --appendonly yes

    for i in {1..30}; do
        if redis-cli -p 6379 ping >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done
    echo "[Redis] Ready on port 6379 (PID: $(cat "${PID_DIR}/redis.pid"))"
}

start_api() {
    if is_running "${PID_DIR}/api.pid"; then
        echo "[API] Already running (PID: $(cat "${PID_DIR}/api.pid"))"
        return 0
    fi

    echo "[API] Running database migrations..."
    cd "${ROOT_DIR}"
    alembic upgrade head

    echo "[API] Starting FastAPI server on port 8000..."
    setsid "${ROOT_DIR}/.venv/bin/uvicorn" api.main:app --host 0.0.0.0 --port 8000 --workers 8 > "${LOGS_DIR}/api.log" 2>&1 &
    local api_pid=$!
    echo "$api_pid" > "${PID_DIR}/api.pid"

    for i in {1..30}; do
        if curl -s "http://127.0.0.1:8000/healthz" >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done
    echo "[API] Ready on port 8000 (PID: $api_pid)"
}

start_worker() {
    if is_running "${PID_DIR}/worker.pid"; then
        echo "[Worker] Already running (PID: $(cat "${PID_DIR}/worker.pid"))"
        return 0
    fi

    echo "[Worker] Starting worker pool (8 processes)..."
    cd "${ROOT_DIR}"
    setsid "${ROOT_DIR}/.venv/bin/python" -m worker.main > "${LOGS_DIR}/worker.log" 2>&1 &
    local worker_pid=$!
    echo "$worker_pid" > "${PID_DIR}/worker.pid"

    for i in {1..30}; do
        if [ -f "/tmp/pulse_worker_healthy" ]; then
            break
        fi
        sleep 0.2
    done
    echo "[Worker] Ready with 8 worker processes (Supervisor PID: $worker_pid)"
}

stop_service() {
    local name="$1"
    local pid_file="${PID_DIR}/${name}.pid"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping ${name} (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            for i in {1..30}; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 0.1
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi
}

stop_all() {
    echo "Shutting down Pulse services..."
    stop_service "worker"
    # Kill any dangling worker sub-processes
    pkill -f "worker.process" 2>/dev/null || true
    rm -f /tmp/pulse_worker_healthy

    stop_service "api"
    pkill -f "uvicorn api.main:app" 2>/dev/null || true

    if is_running "${PID_DIR}/redis.pid"; then
        redis-cli -p 6379 shutdown nosave 2>/dev/null || true
        stop_service "redis"
    fi

    if [ -d "${PG_DATA}" ] && [ -f "${PG_DATA}/postmaster.pid" ]; then
        pg_ctl -D "${PG_DATA}" -m fast stop 2>/dev/null || true
        rm -f "${PID_DIR}/postgres.pid"
    fi
    echo "All services stopped."
}

status_all() {
    echo "=== Pulse Service Status ==="
    local all_ok=1

    # Postgres
    if pg_isready -h 127.0.0.1 -p 5432 -U pulse_user >/dev/null 2>&1; then
        echo "postgres: HEALTHY (port 5432)"
    else
        echo "postgres: DOWN"
        all_ok=0
    fi

    # Redis
    if redis-cli -p 6379 ping 2>/dev/null | grep -q PONG; then
        echo "redis:    HEALTHY (port 6379)"
    else
        echo "redis:    DOWN"
        all_ok=0
    fi

    # API
    local api_res
    api_res=$(curl -s "http://127.0.0.1:8000/healthz" 2>/dev/null || echo "")
    if echo "$api_res" | grep -q '"status":"ok"'; then
        echo "api:      HEALTHY (http://127.0.0.1:8000/healthz)"
    else
        echo "api:      DOWN"
        all_ok=0
    fi

    # Worker
    if [ -f "/tmp/pulse_worker_healthy" ] && is_running "${PID_DIR}/worker.pid"; then
        echo "worker:   HEALTHY (PID $(cat "${PID_DIR}/worker.pid"))"
    else
        echo "worker:   DOWN"
        all_ok=0
    fi

    return $(( 1 - all_ok ))
}

case "${1:-status}" in
    start)
        start_postgres
        start_redis
        start_api
        start_worker
        echo ""
        status_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 1
        start_postgres
        start_redis
        start_api
        start_worker
        status_all
        ;;
    status)
        status_all
        ;;
    clean)
        stop_all
        rm -rf "${DATA_DIR}"
        echo "Cleaned all data and logs."
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|clean}"
        exit 1
        ;;
esac
