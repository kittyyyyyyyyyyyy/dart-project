from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import subprocess
import os
import uuid
import json
import threading
import time
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = FastAPI()

API_KEY = os.getenv("DART_API_KEY")
jobs = {}

company_cache = {
    "loaded_at": 0,
    "names": []
}


def make_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http = make_session()


def load_all_company_names():
    """
    OpenDART corpCode.xml 기반 전체 회사명 로드
    12시간 캐시
    """
    now = time.time()
    if company_cache["names"] and (now - company_cache["loaded_at"] < 60 * 60 * 12):
        return company_cache["names"]

    if not API_KEY:
        return []

    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": API_KEY}

    try:
        res = http.get(url, params=params, timeout=(60, 180))
        z = zipfile.ZipFile(io.BytesIO(res.content))

        xml_filename = z.namelist()[0]
        xml_content = z.read(xml_filename)

        root = ET.fromstring(xml_content)

        names = set()
        for item in root.findall("list"):
            corp_name = item.findtext("corp_name", default="").strip()
            stock_code = item.findtext("stock_code", default="").strip()

            # 상장사만 추천
            if corp_name and stock_code:
                names.add(corp_name)

        result = sorted(names)
        company_cache["loaded_at"] = now
        company_cache["names"] = result
        return result

    except Exception as e:
        print("회사명 목록 로드 실패:", str(e))
        return company_cache["names"]


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
def company_suggestions(q: str = Query(...)):
    keyword = q.strip()
    if not keyword:
        return {"companies": []}

    all_names = load_all_company_names()
    q_lower = keyword.lower()

    starts = []
    contains = []

    for name in all_names:
        lower_name = name.lower()
        if lower_name.startswith(q_lower):
            starts.append(name)
        elif q_lower in lower_name:
            contains.append(name)

    matched = (starts + contains)[:20]

    return {
        "companies": matched,
        "count": len(matched)
    }


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


@app.post("/start-regular-download")
def start_regular_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = f"regular_meeting_result_{job_id}.xlsx"
    progress_file = f"progress_regular_{job_id}.json"

    args = [
        "python3",
        "generate_regular_meeting_excel.py",
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