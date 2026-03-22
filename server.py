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

COMPANY_CACHE_FILE = "company_names.json"

company_cache = {
    "loaded_at": 0,
    "names": [],          # 원본 회사명 리스트
    "pairs": [],          # [(원본, lower)]
    "prefix1": {},        # {'삼': [(원본, lower), ...]}
    "prefix2": {}         # {'삼성': [(원본, lower), ...]}
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


def build_company_index(names):
    pairs = []
    prefix1 = {}
    prefix2 = {}

    for name in names:
        lower = name.lower()
        pairs.append((name, lower))

        if len(lower) >= 1:
            key1 = lower[:1]
            prefix1.setdefault(key1, []).append((name, lower))

        if len(lower) >= 2:
            key2 = lower[:2]
            prefix2.setdefault(key2, []).append((name, lower))

    company_cache["names"] = names
    company_cache["pairs"] = pairs
    company_cache["prefix1"] = prefix1
    company_cache["prefix2"] = prefix2
    company_cache["loaded_at"] = time.time()


def save_company_names_to_file(names):
    with open(COMPANY_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False)


def load_company_names_from_file():
    if not os.path.exists(COMPANY_CACHE_FILE):
        return []

    try:
        with open(COMPANY_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def fetch_company_names_from_dart():
    if not API_KEY:
        return []

    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": API_KEY}

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

    return sorted(names)


def ensure_company_cache_loaded():
    # 이미 메모리에 인덱스까지 있으면 그대로 사용
    if company_cache["pairs"]:
        return company_cache["names"]

    # 파일 캐시 먼저 사용
    file_names = load_company_names_from_file()
    if file_names:
        build_company_index(file_names)
        return file_names

    # 파일도 없으면 DART에서 최초 1회 생성
    try:
        names = fetch_company_names_from_dart()
        build_company_index(names)
        save_company_names_to_file(names)
        return names
    except Exception as e:
        print("회사명 목록 초기 로드 실패:", str(e))
        return []


def refresh_company_cache_in_background():
    try:
        names = fetch_company_names_from_dart()
        if names:
            build_company_index(names)
            save_company_names_to_file(names)
            print(f"회사명 캐시 갱신 완료: {len(names)}개")
    except Exception as e:
        print("회사명 캐시 백그라운드 갱신 실패:", str(e))


@app.on_event("startup")
def startup_event():
    ensure_company_cache_loaded()

    t = threading.Thread(target=refresh_company_cache_in_background, daemon=True)
    t.start()


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
        return {"companies": [], "count": 0}

    ensure_company_cache_loaded()

    q_lower = keyword.lower()

    # 입력 길이에 따라 후보군 최소화
    if len(q_lower) >= 2:
        candidates = company_cache["prefix2"].get(q_lower[:2], [])
    else:
        candidates = company_cache["prefix1"].get(q_lower[:1], [])

    starts = []
    contains = []

    for original, lower_name in candidates:
        if lower_name.startswith(q_lower):
            starts.append(original)
        elif q_lower in lower_name:
            contains.append(original)

        if len(starts) + len(contains) >= 20:
            break

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

@app.post("/start-nps-download")
def start_nps_download(payload: DownloadRequest):
    try:
        job_id = str(uuid.uuid4())[:8]
        output_file = f"nps_vote_result_{job_id}.xlsx"
        progress_file = f"progress_nps_{job_id}.json"

        args = [
            "python3",
            "generate_nps_vote_excel.py",
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

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"start-nps-download failed: {str(e)}"}
        )

@app.get("/nps-test")
def nps_test():
    return {"ok": True, "message": "nps route alive"}

@app.post("/start-nps-download-test")
def start_nps_download_test():
    return {"job_id": "test1234"}

import hashlib

@app.get("/debug-version")
def debug_version():
    def file_info(path):
        if not os.path.exists(path):
            return {"exists": False, "md5": None, "size": None}
        with open(path, "rb") as f:
            data = f.read()
        return {
            "exists": True,
            "md5": hashlib.md5(data).hexdigest(),
            "size": len(data)
        }

    return {
        "server.py": file_info("server.py"),
        "index.html": file_info("index.html"),
        "generate_nps_vote_excel.py": file_info("generate_nps_vote_excel.py")
    }    