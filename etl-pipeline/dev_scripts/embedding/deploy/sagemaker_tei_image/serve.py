import asyncio
import os
import shlex
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import uvicorn


HF_MODEL_ID = os.environ.get("HF_MODEL_ID", "Qwen/Qwen3-Embedding-0.6B")

SAGEMAKER_BIND_HOST = os.environ.get("SAGEMAKER_BIND_HOST", "0.0.0.0")
SAGEMAKER_BIND_PORT = int(os.environ.get("SAGEMAKER_BIND_PORT", "8080"))

TEI_HOST = os.environ.get("TEI_HOST", "127.0.0.1")
TEI_PORT = int(os.environ.get("TEI_PORT", "8081"))
TEI_BASE_URL = f"http://{TEI_HOST}:{TEI_PORT}"

TEI_EXTRA_ARGS = os.environ.get("TEI_EXTRA_ARGS", "")
STARTUP_TIMEOUT_SECONDS = int(os.environ.get("STARTUP_TIMEOUT_SECONDS", "900"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "60"))

tei_process: subprocess.Popen | None = None


def log(message: str) -> None:
    print(message, flush=True)


def build_tei_command() -> list[str]:
    cmd = [
        "text-embeddings-router",
        "--model-id",
        HF_MODEL_ID,
        "--hostname",
        "0.0.0.0",
        "--port",
        str(TEI_PORT),
    ]

    if TEI_EXTRA_ARGS.strip():
        cmd.extend(shlex.split(TEI_EXTRA_ARGS))

    return cmd


def start_tei() -> subprocess.Popen:
    cmd = build_tei_command()
    log(f"Starting TEI: {' '.join(shlex.quote(x) for x in cmd)}")

    return subprocess.Popen(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        preexec_fn=os.setsid,
    )


async def wait_for_tei_ready() -> None:
    deadline = time.time() + STARTUP_TIMEOUT_SECONDS

    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.time() < deadline:
            if tei_process is not None and tei_process.poll() is not None:
                raise RuntimeError(
                    f"TEI exited early with code {tei_process.returncode}"
                )

            try:
                response = await client.get(f"{TEI_BASE_URL}/health")
                if response.status_code == 200:
                    log("TEI is healthy.")
                    return
            except Exception:
                pass

            await asyncio.sleep(2)

    raise TimeoutError(f"TEI did not become healthy within {STARTUP_TIMEOUT_SECONDS}s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tei_process

    tei_process = start_tei()
    await wait_for_tei_ready()

    try:
        yield
    finally:
        if tei_process and tei_process.poll() is None:
            log("Stopping TEI...")
            try:
                os.killpg(os.getpgid(tei_process.pid), signal.SIGTERM)
                tei_process.wait(timeout=30)
            except Exception:
                os.killpg(os.getpgid(tei_process.pid), signal.SIGKILL)


app = FastAPI(lifespan=lifespan)


@app.api_route("/ping", methods=["GET", "POST"])
async def ping():
    """
    Direct SageMaker /ping → TEI /health passthrough.
    """
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(f"{TEI_BASE_URL}/health")

    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "application/json"),
    )


@app.post("/invocations")
async def invocations(request: Request):
    """
    Direct SageMaker /invocations → TEI /embed passthrough.

    The request body is forwarded unchanged.
    """
    body = await request.body()

    headers = {
        "content-type": request.headers.get("content-type", "application/json"),
    }

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        response = await client.post(
            f"{TEI_BASE_URL}/embed",
            content=body,
            headers=headers,
        )

    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "application/json"),
    )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=SAGEMAKER_BIND_HOST,
        port=SAGEMAKER_BIND_PORT,
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
    )
