from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import subprocess
import os
import uuid
import json
import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache
import requests

app = FastAPI()

API_KEY = os.getenv("DART_API_KEY")
jobs = {}


def chunk_date_ranges(start_date_str, end_date_str, chunk_days=90):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")

    ranges = []
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        ranges.append(
            (
                current.strftime("%Y%m%d"),
                chunk_end.strftime("%Y%m%d")
            )
        )
        current = chunk_end + timedelta(days=1)

    return ranges


@lru_cache(maxsize=32)
def cached_company_names(start_date: str, end_date: str):
    if not API_KEY:
        return tuple()

    url = "https://opendart.fss.or.kr/api/list.json"
    names = set()

    for bgn_de, end_de in chunk_date_ranges(start_date, end_date):
        for corp_cls in ["Y", "K"]:
            page_no = 1

            while True:
                params = {
                    "crtfc_key": API_KEY,
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "corp_cls": corp_cls,
                    "page_no": page_no,
                    "page_count": 100,
                    "sort": "date",
                    "sort_mth": "desc"
                }

                res = requests.get(url, params=params, timeout=60)
                data = res.json()

                if data.get("status") != "000":
                    break

                items = data.get("list", [])
                if not items:
                    break

                for item in items:
                    report_nm = str(item.get("report_nm", ""))
                    if "기업지배구조보고서" not in report_nm:
                        continue
                    corp_name = str(item.get("corp_name", ""))
                    if corp_name:
                        names.add(corp_name)

                if len(items) < 100:
                    break

                page_no += 1
                time.sleep(0.1)

    return tuple(sorted(names))


class DownloadRequest(BaseModel):
    start_date: str
    end_date: str
    companies: list[str] = []


def monitor_process(job_id, proc):
    stdout, stderr = proc.communicate()
    jobs[job_id]["stdout"] = stdout
    jobs[job_id]["stderr"] = stderr
    jobs[job_id]["returncode"] = proc.returncode
    jobs[job_id]["finished_at"] = time.time()


@app.get("/")
def root():
    return FileResponse("index.html")


@app.get("/page")
def page():
    return FileResponse("index.html")


@app.get("/health")
def health():
    return {"message": "server running"}


@app.get("/env-check")
def env_check():
    value = os.getenv("DART_API_KEY")
    return {
        "has_key": bool(value),
        "prefix": value[:5] if value else None
    }


@app.get("/company-suggestions")
def company_suggestions(
    start_date: str = Query(...),
    end_date: str = Query(...),
    q: str = Query(...)
):
    if not q.strip():
        return {"companies": []}

    all_names = cached_company_names(start_date, end_date)
    q_lower = q.strip().lower()

    matched = [name for name in all_names if q_lower in name.lower()][:20]
    return {"companies": matched}


@app.post("/start-download")
def start_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = f"filtered_result_{job_id}.xlsx"
    progress_file = f"progress_{job_id}.json"

    args = [
        "python3",
        "generate_filtered_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
        "--companies-json", json.dumps(payload.companies, ensure_ascii=False),
        "--output", output_file,
        "--progress-file", progress_file
    ]

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    jobs[job_id] = {
        "process": proc,
        "output_file": output_file,
        "progress_file": progress_file,
        "stdout": "",
        "stderr": "",
        "returncode": None,
        "created_at": time.time()
    }

    t = threading.Thread(target=monitor_process, args=(job_id, proc), daemon=True)
    t.start()

    return {"job_id": job_id}


@app.get("/job-status/{job_id}")
def job_status(job_id: str):
    if job_id not in jobs:
        return JSONResponse(status_code=404, content={"error": "job not found"})

    job = jobs[job_id]
    proc = job["process"]
    progress = {
        "status": "running",
        "percent": 0,
        "message": "작업 준비 중...",
        "current": 0,
        "total": 0
    }

    if os.path.exists(job["progress_file"]):
        try:
            with open(job["progress_file"], "r", encoding="utf-8") as f:
                progress = json.load(f)
        except Exception:
            pass

    if proc.poll() is None:
        return {
            "job_id": job_id,
            "state": "running",
            "progress": progress
        }

    if job["returncode"] != 0:
        return {
            "job_id": job_id,
            "state": "error",
            "progress": progress,
            "stdout": job["stdout"],
            "stderr": job["stderr"]
        }

    if os.path.exists(job["output_file"]):
        return {
            "job_id": job_id,
            "state": "done",
            "progress": {
                "status": "done",
                "percent": 100,
                "message": "엑셀 생성 완료",
                "current": progress.get("current", 0),
                "total": progress.get("total", 0)
            },
            "download_url": f"/download-file/{job_id}"
        }

    return {
        "job_id": job_id,
        "state": "error",
        "progress": progress,
        "stdout": job["stdout"],
        "stderr": job["stderr"],
        "error": "엑셀 파일을 찾을 수 없습니다."
    }


@app.get("/download-file/{job_id}")
def download_file(job_id: str):
    if job_id not in jobs:
        return JSONResponse(status_code=404, content={"error": "job not found"})

    output_file = jobs[job_id]["output_file"]

    if not os.path.exists(output_file):
        return JSONResponse(status_code=404, content={"error": "file not found"})

    return FileResponse(
        path=output_file,
        filename=output_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )