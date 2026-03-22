from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
import subprocess
import os
import uuid

app = FastAPI()

@app.get("/")
def root():
    return {"message": "server running"}

@app.get("/page")
def page():
    return FileResponse("index.html")

@app.get("/env-check")
def env_check():
    value = os.getenv("DART_API_KEY")
    return {
        "has_key": bool(value),
        "prefix": value[:5] if value else None
    }

@app.get("/download")
def download_excel(
    start_date: str = Query(...),
    end_date: str = Query(...),
    company: str = Query("")
):
    try:
        file_id = str(uuid.uuid4())[:8]
        output_file = f"filtered_result_{file_id}.xlsx"

        result = subprocess.run(
            [
                "python3",
                "generate_filtered_excel.py",
                "--start-date", start_date,
                "--end-date", end_date,
                "--company", company,
                "--output", output_file
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "generate_filtered_excel.py 실행 실패",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            )

        if not os.path.exists(output_file):
            return JSONResponse(
                status_code=500,
                content={
                    "error": "엑셀 파일 생성 실패",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            )

        return FileResponse(
            path=output_file,
            filename=output_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
