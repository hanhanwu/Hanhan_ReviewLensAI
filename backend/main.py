from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from backend.utils import count_csv_dimensions

app = FastAPI(
    title="ReviewLens CSV Counter",
    description="Upload a CSV and report how many rows and columns it contains.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    """Accept a CSV upload and return row/column counts."""
    filename = file.filename or "uploaded CSV"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    try:
        raw_bytes = await file.read()
        rows, columns = count_csv_dimensions(raw_bytes)
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail=f"Unable to parse the CSV file: {exc}"
        )
    finally:
        await file.close()

    return {"rows": rows, "columns": columns, "filename": filename}
