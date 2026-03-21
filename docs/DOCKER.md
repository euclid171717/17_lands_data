# Docker Setup

This project can run entirely inside Docker. Data, config, and output are mounted from your host so they persist between container runs.

## Quick Start

```bash
docker compose build
docker compose run --rm app bash   # interactive shell

# Or run specific commands
docker compose run --rm app python -m scripts.ingest.cli --helpers
docker compose run --rm app dbt run
docker compose run --rm app python scripts/run_jobs.py ingest_set_mkm
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac) or Docker Engine + Docker Compose v2
- Ensure `config/datasets.yaml` and `config/jobs.yaml` exist (copy from `.example` if needed)

## Volume Mounts

| Host Path  | Container Path | Purpose                          |
|------------|----------------|----------------------------------|
| `./data`   | `/app/data`    | Raw data, DuckDB, downloads      |
| `./config` | `/app/config`  | datasets.yaml, jobs.yaml         |
| `./output` | `/app/output`  | Reports, charts, exported tables |

## Using a Dedicated Drive (e.g. 1TB SSD)

Docker Desktop stores images, containers, and build cache on your system drive by default. To use a spare drive (e.g. a 1TB SSD):

### 1. Format the Drive (Windows)

If the drive still has an old Windows install or data you want to clear:

1. **Open Disk Management**: Press `Win + X` → **Disk Management**
2. **Locate the 1TB drive** in the bottom panel (note the disk number, e.g. Disk 1)
3. **Delete existing partitions**:
   - Right‑click each partition on that disk → **Delete Volume**
   - Confirm; repeat until the disk shows as "Unallocated"
4. **Create a new partition**:
   - Right‑click the Unallocated space → **New Simple Volume**
   - Wizard: Next → use default size → assign a drive letter (e.g. `E:`)
   - Format as **NTFS**, name it (e.g. "DockerData"), enable **Quick Format**
   - Finish

> **Warning**: Deleting volumes erases all data on that disk. Double‑check you're on the correct drive.

### 2. Point Docker Desktop to the New Drive

1. Open **Docker Desktop** → **Settings** (gear icon)
2. Go to **Resources** → **Advanced**
3. Set **Disk image location** to your new drive, e.g. `E:\Docker`
4. Click **Apply & Restart**

Docker will move its data to the new location. This can take a while if you have many images.

### 3. Store Project Data on the SSD (Optional)

To keep the 17lands `data/` folder (raw files, DuckDB) on the SSD instead of your main drive:

- Clone or move the project to the SSD (e.g. `E:\17_lands_data`)
- Run `docker compose` from that directory

The volume mounts will then use the SSD for `data/`, `config/`, and `output/`.

### Alternative: WSL2 on the SSD

If you use WSL2 with Docker Desktop:

1. [Export your WSL distro](https://learn.microsoft.com/en-us/windows/wsl/disk-space#move-wsl-to-another-drive), then import it onto the new drive.
2. Or create a new WSL distro with `--import` pointing to a path on the SSD.

---

*Last updated: March 2025*
