import os
import time
import uuid
from io import BytesIO
from typing import Any, Dict, List

import pandas as pd
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware  # ✅ NEW

# === Config ===
EXPORT_DIR = "exports"
MAX_AGE_HOURS = 6
MAX_TOTAL_SIZE_BYTES = 400 * 1024 * 1024  # 400 MB safety limit

os.makedirs(EXPORT_DIR, exist_ok=True)

app = FastAPI(title="Airtable Export Service")

# ✅ Allow requests from Airtable (or anyone, to keep it simple for now)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # you can restrict later if you want
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def cleanup_old_exports() -> None:
    """Delete ZIPs older than MAX_AGE_HOURS and keep total size under MAX_TOTAL_SIZE_BYTES."""
    if not os.path.isdir(EXPORT_DIR):
        return

    now = time.time()
    max_age_seconds = MAX_AGE_HOURS * 3600

    files: List[str] = []
    for fname in os.listdir(EXPORT_DIR):
        path = os.path.join(EXPORT_DIR, fname)
        if not os.path.isfile(path):
            continue

        # Remove too old files
        age = now - os.path.getmtime(path)
        if age > max_age_seconds:
            try:
                os.remove(path)
            except OSError:
                pass
            continue

        files.append(path)

    # Recompute list and enforce total size
    files = [f for f in files if os.path.isfile(f)]
    # sort by modification time (oldest first)
    files.sort(key=lambda p: os.path.getmtime(p))

    total_size = sum(os.path.getsize(p) for p in files)

    while total_size > MAX_TOTAL_SIZE_BYTES and files:
        oldest = files.pop(0)
        try:
            size = os.path.getsize(oldest)
        except OSError:
            size = 0
        try:
            os.remove(oldest)
        except OSError:
            pass
        total_size -= size


def normalize_value(value: Any) -> Any:
    """Convert Airtable field values to something Excel-friendly."""
    if value is None:
        return ""

    # Attachments or multi-select / linked records often come as lists
    if isinstance(value, list):
        if not value:
            return ""

        first = value[0]

        # Case 1: attachments -> list of {url, filename, ...}
        if isinstance(first, dict) and "url" in first:
            return "; ".join(
                (att.get("filename") or att.get("url", "")) for att in value
            )

        # Case 2: multi-select / linked records -> list of {id, name, ...}
        if isinstance(first, dict) and "name" in first:
            return "; ".join(item.get("name", "") for item in value)

        # Fallback: just join as strings
        return "; ".join(str(v) for v in value)

    # Single select / single linked record: {id, name, color...}
    if isinstance(value, dict):
        if "name" in value:
            return value["name"]
        return str(value)

    # Numbers, strings, booleans…
    return value



def safe_folder_name(name: str) -> str:
    """Sanitize primary field to be safe as a folder/file prefix."""
    # Remove characters that are problematic in paths
    forbidden = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for ch in forbidden:
        name = name.replace(ch, "_")
    # Trim and limit length
    return name.strip()[:80] or "record"


@app.post("/export")
async def create_export(request: Request):
    """
    Receive Airtable records JSON, build Excel + ZIP, store it,
    and return a download URL.
    """
    cleanup_old_exports()

    body = await request.json()
    records: List[Dict[str, Any]] = body.get("records", [])

    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    # 1) Build Excel from records
    rows_for_excel: List[Dict[str, Any]] = []

    for r in records:
        primary = r.get("primary", "")
        fields = r.get("fields", {})
        row: Dict[str, Any] = {"Primary": primary}
        for field_name, value in fields.items():
            row[field_name] = normalize_value(value)
        rows_for_excel.append(row)

    df = pd.DataFrame(rows_for_excel)

    # 2) Create ZIP in memory
    from zipfile import ZipFile, ZIP_DEFLATED

    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w", ZIP_DEFLATED) as zip_file:
        # Add Excel file
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        zip_file.writestr("data.xlsx", excel_buffer.read())

        # Add attachments in folders per primary
        for r in records:
            primary_raw = str(r.get("primary", "record"))
            primary_folder = safe_folder_name(primary_raw)
            fields = r.get("fields", {})

            for field_name, value in fields.items():
                if (
                    isinstance(value, list)
                    and value
                    and isinstance(value[0], dict)
                    and "url" in value[0]
                ):
                    # Treat as attachment field
                    for att in value:
                        url = att.get("url")
                        if not url:
                            continue
                        filename = att.get("filename") or url.split("/")[-1]
                        try:
                            resp = requests.get(url, timeout=30)
                            resp.raise_for_status()
                        except Exception:
                            # Skip failed downloads
                            continue

                        path_in_zip = f"{primary_folder}/{filename}"
                        zip_file.writestr(path_in_zip, resp.content)

    zip_buffer.seek(0)

    # 3) Save ZIP to disk with a unique ID
    export_id = str(uuid.uuid4())
    zip_path = os.path.join(EXPORT_DIR, f"{export_id}.zip")
    with open(zip_path, "wb") as f:
        f.write(zip_buffer.read())

    # 4) Build download URL
    download_url = str(
        request.url_for("download_export", export_id=export_id)
    )

    return JSONResponse({"download_url": download_url})


@app.get("/download/{export_id}")
async def download_export(export_id: str):
    """
    Download a previously generated ZIP. Deletes file if expired.
    """
    cleanup_old_exports()

    zip_path = os.path.join(EXPORT_DIR, f"{export_id}.zip")
    if not os.path.exists(zip_path):
        raise HTTPException(
            status_code=404, detail="Export not found or expired"
        )

    # Double-check age
    age_seconds = time.time() - os.path.getmtime(zip_path)
    if age_seconds > MAX_AGE_HOURS * 3600:
        try:
            os.remove(zip_path)
        except OSError:
            pass
        raise HTTPException(
            status_code=404, detail="Export expired"
        )

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename="airtable_export.zip",
    )

@app.get("/")
def root():
    return {"status": "ok"}
