# orchestrator.py
import os
import re
import subprocess
from pathlib import Path
from typing import Tuple, Optional

# Figure out paths relative to this file
CURRENT_DIR = Path(__file__).resolve().parent

# When running locally, repo root is 2 levels up: head-bank/backend -> head-bank -> repo root
POSSIBLE_REPO_ROOT = CURRENT_DIR.parents[2] if len(CURRENT_DIR.parents) >= 3 else CURRENT_DIR

STANDALONE_APP_PATH = POSSIBLE_REPO_ROOT / "singular-bank-standalone-app"
DOCKER_COMPOSE_FILE: Optional[Path] = None

if STANDALONE_APP_PATH.exists():
    DOCKER_COMPOSE_FILE = STANDALONE_APP_PATH / "docker-compose.yml"
else:
    # Inside Docker image or wrong path: we won't try to run docker compose
    print("[WARN] singular-bank-standalone-app not found; Docker orchestration disabled.")


def compute_ports(bank_id: str) -> Tuple[int, int]:
    """
    BANK1 -> backend 8001, frontend 3001
    BANK2 -> backend 8002, frontend 3002
    BANKX (no number) -> backend 8001, frontend 3001
    """
    match = re.search(r"(\d+)$", bank_id)
    idx = int(match.group(1)) if match else 1

    backend_port = 8000 + idx
    frontend_port = 3000 + idx
    return backend_port, frontend_port


def start_standalone_bank(bank_id: str, backend_port: int, frontend_port: int) -> None:
    """
    Start a standalone bank instance using docker compose with the given BANK_ID
    and host ports. This only works when run on the host machine where
    singular-bank-standalone-app and Docker are available.
    """
    if DOCKER_COMPOSE_FILE is None or not DOCKER_COMPOSE_FILE.exists():
        # Likely running inside the head-bank Docker container or repo not mounted.
        print("[WARN] Docker orchestration is disabled (docker-compose file not found).")
        return

    env = os.environ.copy()
    env["BANK_ID"] = bank_id
    env["BACKEND_PORT"] = str(backend_port)
    env["FRONTEND_PORT"] = str(frontend_port)

    project_name = f"bank_{bank_id.lower()}"

    cmd = [
        "docker", "compose",  # use "docker-compose" if that's what you have locally
        "-f", str(DOCKER_COMPOSE_FILE),
        "-p", project_name,
        "up",
        "-d",
    ]

    subprocess.Popen(
        cmd,
        cwd=str(STANDALONE_APP_PATH),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
