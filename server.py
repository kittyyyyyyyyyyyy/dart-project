from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import subprocess
import os
import sys
import uuid
import json
import threading
import time
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "index.html"
COMPANY_CACHE_FILE = BASE_DIR / "company_names.json"
LOCAL_CORP_XML = BASE_DIR / "corp_data" / "CORPCODE.xml"
API_KEY = os.getenv("DART_API_KEY")

app = FastAPI()
jobs = {}
company_cache = {
    "loaded_at": 0,
    "names": [],
    "source": None
}


def make_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http = make_session()


def save_company_names_to_file(names):
    COMPANY_CACHE_FILE.write_text(json.dumps(names, ensure_ascii=False), encoding="utf-8")


def load_company_names_from_file():
    if not COMPANY_CACHE_FILE.exists():
        return []
    try:
        return json.loads(COMPANY_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def load_company_names_from_local_xml():
    if not LOCAL_CORP_XML.exists():
        return []
    try:
        tree = ET.parse(str(LOCAL_CORP_XML))
        root = tree.getroot()
        names = set()
        for item in root.findall("list"):
            corp_name = item.findtext("corp_name", default="").strip()
            stock_code = item.findtext("stock_code", default="").strip()
            if corp_name and stock_code:
                names.add(corp_name)
        result = sorted(names)
        print(f"로컬 XML에서 회사명 {len(result)}개 로드 완료")
        return result
    except Exception as e:
        print(f"로컬 XML 파싱 실패: {e}")
        return []


def fetch_company_names_from_dart():
    if not API_KEY:
        return []
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": API_KEY}
    res = http.get(url, params=params, timeout=(20, 60))
    res.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(res.content))
    xml_filename = z.namelist()[0]
    xml_content = z.read(xml_filename)
    root = ET.fromstring(xml_content)
    names = set()
    for item in root.findall("list"):
        corp_name = item.findtext("corp_name", default="").strip()
        stock_code = item.findtext("stock_code", default="").strip()
        if corp_name and stock_code:
            names.add(corp_name)
    return sorted(names)


def ensure_company_cache_loaded():
    if company_cache["names"]:
        return company_cache["names"]

    file_names = load_company_names_from_file()
    if file_names:
        company_cache["names"] = file_names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "file"
        return file_names

    xml_names = load_company_names_from_local_xml()
    if xml_names:
        company_cache["names"] = xml_names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "local_xml"
        try:
            save_company_names_to_file(xml_names)
        except Exception:
            pass
        return xml_names

    try:
        names = fetch_company_names_from_dart()
        company_cache["names"] = names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "dart"
        try:
            save_company_names_to_file(names)
        except Exception:
            pass
        return names
    except Exception as e:
        print("회사명 목록 초기 로드 실패:", str(e))
        return []


class DownloadRequest(BaseModel):
    start_date: str
    end_date: str
    companies: list[str] = []


class KindDownloadRequest(BaseModel):
    start_date: str
    end_date: str


def monitor_process(job_id, proc):
    stdout, stderr = proc.communicate()
    jobs[job_id]["stdout"] = stdout
    jobs[job_id]["stderr"] = stderr
    jobs[job_id]["returncode"] = proc.returncode
    jobs[job_id]["finished_at"] = time.time()


def _spawn_job(job_id: str, output_file: Path, progress_file: Path, args: list[str]):
    proc = subprocess.Popen(
        args,
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    jobs[job_id] = {
        "process": proc,
        "output_file": str(output_file),
        "progress_file": str(progress_file),
        "stdout": "",
        "stderr": "",
        "returncode": None,
        "created_at": time.time(),
    }
    t = threading.Thread(target=monitor_process, args=(job_id, proc), daemon=True)
    t.start()


@app.get("/")
def root():
    return FileResponse(str(INDEX_HTML))


@app.get("/page")
def page():
    return FileResponse(str(INDEX_HTML))


@app.get("/health")
def health():
    return {"message": "server running"}


@app.get("/startup-debug")
def startup_debug():
    return {
        "cwd": os.getcwd(),
        "base_dir": str(BASE_DIR),
        "index_exists": INDEX_HTML.exists(),
        "corp_xml_exists": LOCAL_CORP_XML.exists(),
        "python": sys.executable,
    }


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

    all_names = ensure_company_cache_loaded()
    q_lower = keyword.lower()
    starts = []
    contains = []

    for name in all_names:
        lower_name = name.lower()
        if lower_name.startswith(q_lower):
            starts.append(name)
        elif q_lower in lower_name:
            contains.append(name)
        if len(starts) + len(contains) >= 20:
            break

    matched = (starts + contains)[:20]
    return {"companies": matched, "count": len(matched)}


@app.post("/start-download")
def start_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = BASE_DIR / f"filtered_result_{job_id}.xlsx"
    progress_file = BASE_DIR / f"progress_{job_id}.json"
    args = [
        sys.executable,
        "generate_filtered_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
        "--companies-json", json.dumps(payload.companies, ensure_ascii=False),
        "--output", str(output_file),
        "--progress-file", str(progress_file),
    ]
    _spawn_job(job_id, output_file, progress_file, args)
    return {"job_id": job_id}


@app.post("/start-regular-download")
def start_regular_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = BASE_DIR / f"regular_meeting_result_{job_id}.xlsx"
    progress_file = BASE_DIR / f"progress_regular_{job_id}.json"
    args = [
        sys.executable,
        "generate_regular_meeting_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
        "--companies-json", json.dumps(payload.companies, ensure_ascii=False),
        "--output", str(output_file),
        "--progress-file", str(progress_file),
    ]
    _spawn_job(job_id, output_file, progress_file, args)
    return {"job_id": job_id}


@app.post("/start-agm-notice-download")
def start_agm_notice_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = BASE_DIR / f"agm_notice_result_{job_id}.xlsx"
    progress_file = BASE_DIR / f"progress_agm_{job_id}.json"
    args = [
        sys.executable,
        "generate_agm_notice_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
        "--companies-json", json.dumps(payload.companies, ensure_ascii=False),
        "--output", str(output_file),
        "--progress-file", str(progress_file),
    ]
    _spawn_job(job_id, output_file, progress_file, args)
    return {"job_id": job_id}


@app.post("/start-kind-download")
def start_kind_download(payload: KindDownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = BASE_DIR / f"kind_institution_result_{job_id}.xlsx"
    progress_file = BASE_DIR / f"progress_kind_{job_id}.json"
    args = [
        sys.executable,
        "generate_kind_institution_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
        "--output", str(output_file),
        "--progress-file", str(progress_file),
    ]
    _spawn_job(job_id, output_file, progress_file, args)
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
        "total": 0,
    }
    if os.path.exists(job["progress_file"]):
        try:
            with open(job["progress_file"], "r", encoding="utf-8") as f:
                progress = json.load(f)
        except Exception:
            pass
    if proc.poll() is None:
        return {"job_id": job_id, "state": "running", "progress": progress}
    if job["returncode"] != 0:
        return {
            "job_id": job_id,
            "state": "error",
            "progress": progress,
            "stdout": job["stdout"],
            "stderr": job["stderr"],
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
                "total": progress.get("total", 0),
            },
            "download_url": f"/download-file/{job_id}",
        }
    return {
        "job_id": job_id,
        "state": "error",
        "progress": progress,
        "stdout": job["stdout"],
        "stderr": job["stderr"],
        "error": "엑셀 파일을 찾을 수 없습니다.",
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
        filename=Path(output_file).name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
