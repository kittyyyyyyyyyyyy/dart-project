from contextlib import asynccontextmanager
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: 회사명 캐시 로드
    ensure_company_cache_loaded()
    if company_cache.get("source") in ("file", "local_xml"):
        t = threading.Thread(target=refresh_company_cache_in_background, daemon=True)
        t.start()
    yield
    # shutdown: 별도 정리 불필요


app = FastAPI(lifespan=lifespan)

API_KEY = os.getenv("DART_API_KEY")
jobs = {}

COMPANY_CACHE_FILE = "company_names.json"
LOCAL_CORP_XML = "corp_data/CORPCODE.xml"

company_cache = {
    "loaded_at": 0,
    "names": [],
    "source": None   # "memory" | "file" | "local_xml" | "dart"
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


def load_company_names_from_local_xml():
    """
    레포에 포함된 corp_data/CORPCODE.xml을 파싱해 회사명 목록을 반환한다.
    DART API 호출 없이 즉시 로드 가능해 서버 시작 속도를 크게 개선한다.
    """
    if not os.path.exists(LOCAL_CORP_XML):
        return []
    try:
        tree = ET.parse(LOCAL_CORP_XML)
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

    res = http.get(url, params=params, timeout=(60, 180))
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

    # 1순위: company_names.json 파일 (이전 실행에서 저장된 최신 목록)
    file_names = load_company_names_from_file()
    if file_names:
        company_cache["names"] = file_names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "file"
        return file_names

    # 2순위: 레포에 포함된 로컬 CORPCODE.xml (DART API 불필요, 빠름)
    xml_names = load_company_names_from_local_xml()
    if xml_names:
        company_cache["names"] = xml_names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "local_xml"
        save_company_names_to_file(xml_names)
        return xml_names

    # 3순위: DART API 직접 호출 (느림, 최후 수단)
    try:
        names = fetch_company_names_from_dart()
        company_cache["names"] = names
        company_cache["loaded_at"] = time.time()
        company_cache["source"] = "dart"
        save_company_names_to_file(names)
        return names
    except Exception as e:
        print("회사명 목록 초기 로드 실패:", str(e))
        return []


def refresh_company_cache_in_background():
    """
    DART API에서 최신 회사 목록을 받아 캐시를 갱신한다.
    로컬 XML이나 파일로 로드된 경우 백그라운드에서 최신화한다.
    """
    try:
        names = fetch_company_names_from_dart()
        if names:
            company_cache["names"] = names
            company_cache["loaded_at"] = time.time()
            company_cache["source"] = "dart"
            save_company_names_to_file(names)
            print(f"회사명 캐시 갱신 완료: {len(names)}개")
    except Exception as e:
        print("회사명 캐시 백그라운드 갱신 실패:", str(e))


# startup logic은 lifespan 컨텍스트 매니저로 이동 (파일 상단 참조)


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
        return {"job_id": job_id, "state": "running", "progress": progress}

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


@app.post("/start-agm-notice-download")
def start_agm_notice_download(payload: DownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = f"agm_notice_result_{job_id}.xlsx"
    progress_file = f"progress_agm_{job_id}.json"

    args = [
        "python3",
        "generate_agm_notice_excel.py",
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


@app.post("/start-kind-download")
def start_kind_download(payload: KindDownloadRequest):
    job_id = str(uuid.uuid4())[:8]
    output_file = f"kind_institution_result_{job_id}.xlsx"
    progress_file = f"progress_kind_{job_id}.json"

    args = [
        "python3",
        "generate_kind_institution_excel.py",
        "--start-date", payload.start_date,
        "--end-date", payload.end_date,
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