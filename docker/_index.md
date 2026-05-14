# docker/ — container build and run scripts

Docker image management for production deployment on Outerbounds/Kubernetes.

## Files

| File | What it does |
|---|---|
| `Dockerfile` | Production image definition (Python 3.10, mozaic package, run_forecast.sh entrypoint) |
| `requirements.outerbounds.txt` | Pinned Python dependencies for the Docker image |
| `build_and_push.sh` | Build the image for `--local` (arm64) or `--remote` (amd64) and push to Docker Hub |
| `run_mozaic_docker.sh` | Run the image interactively with Google Cloud credentials mounted |
| `test_docker.py` | Smoke test: verifies the image can import `mozaic_daily` and run a minimal forecast |
| `Dockerfile.bak` | Backup of a previous Dockerfile — not used in production |
| `Dockerfile.plus1` | Experimental image that adds Iran synthetic generation step |

## Key rules

- **Production images must be `linux/amd64`** — Outerbounds runs on amd64; arm64 images will fail at runtime
- **Always run these scripts from the `docker/` directory** — relative paths in the scripts assume that CWD
- **After building a new image, update `IMAGE` in `mozaic_daily_flow.py`** (line ~20)
- **Image naming convention**: `registry.hub.docker.com/brwells78094/mozaic-daily:v<version>_amd64`

## Where new code goes

- **Dependency change**: update `requirements.outerbounds.txt` with a pinned version
- **New entrypoint script**: add to this directory and reference from `Dockerfile`
- **Experimental Dockerfiles**: use a descriptive suffix (`Dockerfile.<purpose>`), not `Dockerfile.bak`
