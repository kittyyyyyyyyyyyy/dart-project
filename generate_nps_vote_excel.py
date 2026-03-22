import os
import io
import re
import json
import time
import shutil
import zipfile
import argparse
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. 환경 변수를 확인하세요.")


def make_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http = make_session()


def write_progress(progress_file, status, percent, message, current=0, total=0):
    if not progress_file:
        return
    data = {
        "status": status,
        "percent": percent,
        "message": message,
        "current": current,
        "total": total
    }
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def chunk_date_ranges(start_date_str, end_date_str, chunk_days=30):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    ranges = []
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        ranges.append((current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        current = chunk_end + timedelta(days=1)

    return ranges


def normalize_agenda_no(text: str):
    if not text:
        return ""
    text = str(text).strip()
    m = re.search(r"(\d+(?:-\d+)?)", text)
    return m.group(1) if m else ""


# -----------------------------
# 국민연금 목록 수집
# -----------------------------
def parse_go_detail_args(raw):
    """
    javascript:fnc_goDetail('','0095000','00010','20260320','1');
    -> ["", "0095000", "00010", "20260320", "1"]
    """
    m = re.search(r"fnc_goDetail\((.*?)\)", raw)
    if not m:
        return []

    inside = m.group(1)
    args = re.findall(r"'(.*?)'", inside)
    return args


def fetch_nps_vote_list(start_date, end_date, company_names=None, progress_file=None):
    """
    국민연금 목록 페이지 전체 페이지 순회
    """
    base_url = "https://fund.nps.or.kr/impa/edwmpblnt/getOHEF0007M0.do"
    company_set = set(company_names or [])
    all_rows = []

    date_ranges = chunk_date_ranges(start_date, end_date, chunk_days=30)
    total_steps = len(date_ranges)
    step = 0

    for s_date, e_date in date_ranges:
        step += 1
        base_percent = min(25, 5 + int((step / max(1, total_steps)) * 20))

        page_index = 1
        while True:
            write_progress(
                progress_file,
                "running",
                base_percent,
                f"국민연금 목록 조회 중... ({s_date}~{e_date}, page {page_index})",
                step,
                total_steps
            )

            params = {
                "menuId": "MN24000636",
                "searchFromDate": s_date,
                "searchToDate": e_date,
                "pageIndex": page_index
            }

            res = http.get(base_url, params=params, timeout=(30, 120))
            res.raise_for_status()

            soup = BeautifulSoup(res.text, "lxml")
            rows = soup.select("table tbody tr")

            if not rows:
                break

            page_items = 0

            for tr in rows:
                cols = [td.get_text(" ", strip=True) for td in tr.select("td")]
                if len(cols) < 5:
                    continue

                no = cols[0]
                corp_name = cols[1]
                stock_code = cols[2]
                meeting_date = cols[3]
                category = cols[4]

                if "정기주총" not in category:
                    continue

                if company_set and corp_name not in company_set:
                    continue

                link = tr.select_one("a")
                href = link.get("href", "") if link else ""
                onclick = link.get("onclick", "") if link else ""

                raw_js = href if href.startswith("javascript:") else onclick
                detail_args = parse_go_detail_args(raw_js)

                all_rows.append({
                    "no": no,
                    "corp_name": corp_name,
                    "stock_code": stock_code,
                    "meeting_date": meeting_date,
                    "category": category,
                    "detail_args": detail_args
                })
                page_items += 1

            # 국민연금 목록은 화면상 10건 단위 페이지 구조
            # 한 페이지에서 0건이면 종료
            if page_items == 0:
                break

            # 다음 페이지 존재 여부: 현재 페이지 숫자 링크/다음 텍스트 기준으로 추정
            page_text = soup.get_text(" ", strip=True)
            if "다음" not in page_text and page_items < 10:
                break

            if page_items < 10:
                break

            page_index += 1
            time.sleep(0.15)

    # 중복 제거
    dedup = {}
    for row in all_rows:
        key = (row["corp_name"], row["stock_code"], row["meeting_date"])
        dedup[key] = row

    return list(dedup.values())


# -----------------------------
# 국민연금 상세 수집
# -----------------------------
def fetch_nps_detail_table(item):
    """
    detail_args 예시:
    ["", "0095000", "00010", "20260320", "1"]

    사이트 패턴상 M0(list) -> M1(detail)로 넘어가는 구조를 우선 사용.
    """
    args = item.get("detail_args", [])
    if len(args) < 5:
        return []

    arg0, arg1, arg2, arg3, arg4 = args

    detail_url = "https://fund.nps.or.kr/impa/edwmpblnt/getOHEF0007M1.do"

    # 사이트 구조상 실제 파라미터명이 다를 수 있으나,
    # fnc_goDetail에서 넘기는 5개 값을 그대로 전달하는 방식으로 우선 구성
    params = {
        "menuId": "MN24000636",
        "arg0": arg0,
        "arg1": arg1,
        "arg2": arg2,
        "arg3": arg3,
        "arg4": arg4
    }

    res = http.get(detail_url, params=params, timeout=(30, 120))
    res.raise_for_status()

    return parse_nps_detail_html(res.text)


def parse_nps_detail_html(html_text):
    soup = BeautifulSoup(html_text, "lxml")

    tables = soup.select("table")
    if not tables:
        return []

    # 가장 큰 표를 상세표로 사용
    table = max(tables, key=lambda t: len(t.select("tr")))

    rows = []
    for tr in table.select("tr"):
        cols = [cell.get_text(" ", strip=True) for cell in tr.select("th, td")]
        if cols:
            rows.append(cols)

    if not rows or len(rows) < 2:
        return []

    header = rows[0]
    data_rows = rows[1:]

    normalized = []
    for idx, row in enumerate(data_rows, start=1):
        item = {"line_no": idx}
        for i, value in enumerate(row, start=1):
            item[f"nps_col_{i}"] = value

        joined = " ".join(row)
        item["agenda_no_norm"] = normalize_agenda_no(joined)
        normalized.append(item)

    return normalized


# -----------------------------
# DART 주주총회소집공고 수집
# -----------------------------
def fetch_dart_meeting_notice(corp_name, meeting_date):
    url = "https://opendart.fss.or.kr/api/list.json"

    dt = datetime.strptime(meeting_date.replace("/", "-"), "%Y-%m-%d")
    bgn = (dt - timedelta(days=60)).strftime("%Y%m%d")
    end = dt.strftime("%Y%m%d")

    found = []

    for corp_cls in ["Y", "K"]:
        page_no = 1
        while True:
            params = {
                "crtfc_key": API_KEY,
                "bgn_de": bgn,
                "end_de": end,
                "corp_cls": corp_cls,
                "page_no": page_no,
                "page_count": 50,
                "sort": "date",
                "sort_mth": "desc",
                "pblntf_ty": "E",
                "pblntf_detail_ty": "E006"
            }

            res = http.get(url, params=params, timeout=(30, 120))
            data = res.json()

            if data.get("status") != "000":
                break

            items = data.get("list", [])
            if not items:
                break

            for it in items:
                name = str(it.get("corp_name", ""))
                report_nm = str(it.get("report_nm", ""))

                if name != corp_name:
                    continue
                if "주주총회소집" not in report_nm:
                    continue

                found.append(it)

            if len(items) < 50:
                break

            page_no += 1
            time.sleep(0.12)

    return found[:1]


def download_dart_document(rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {
        "crtfc_key": API_KEY,
        "rcept_no": str(rcept_no)
    }

    response = http.get(url, params=params, timeout=(30, 180))

    folder_name = f"dart_notice_{rcept_no}"
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
    os.makedirs(folder_name, exist_ok=True)

    try:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(folder_name)
        return folder_name
    except zipfile.BadZipFile:
        return None


def find_main_file(folder_name):
    candidates = []
    for fname in os.listdir(folder_name):
        lower = fname.lower()
        if lower.endswith(".xml") or lower.endswith(".html") or lower.endswith(".htm"):
            candidates.append(os.path.join(folder_name, fname))

    if not candidates:
        return None

    candidates.sort(key=lambda x: os.path.getsize(x), reverse=True)
    return candidates[0]


def extract_dart_agendas(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    target = None
    for tag in soup.find_all(string=True):
        txt = tag.strip()
        if "부의안건" in txt:
            target = tag
            break

    if not target:
        return []

    parent = target.parent
    table = parent.find_next("table")
    if not table:
        return []

    rows = []
    for tr in table.find_all("tr"):
        cols = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if cols:
            rows.append(cols)

    if not rows:
        return []

    data_rows = rows[1:] if len(rows) > 1 else []
    result = []

    for row in data_rows:
        joined = " ".join(row)
        result.append({
            "dart_agenda_no_norm": normalize_agenda_no(joined),
            "dart_agenda_text": joined
        })

    return result


def match_nps_with_dart(nps_rows, dart_agendas):
    dart_map = {}
    for item in dart_agendas:
        key = item.get("dart_agenda_no_norm", "")
        if key and key not in dart_map:
            dart_map[key] = item

    merged = []
    for row in nps_rows:
        agenda_no = row.get("agenda_no_norm", "")
        dart_item = dart_map.get(agenda_no)

        out = dict(row)
        out["dart_agenda_no_norm"] = dart_item.get("dart_agenda_no_norm", "") if dart_item else ""
        out["dart_agenda_text"] = dart_item.get("dart_agenda_text", "") if dart_item else ""
        out["match_status"] = "matched" if dart_item else "unmatched"
        merged.append(out)

    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--companies-json", default="[]")
    parser.add_argument("--output", required=True)
    parser.add_argument("--progress-file", default="")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    output_file = args.output
    progress_file = args.progress_file

    try:
        company_names = json.loads(args.companies_json)
        if not isinstance(company_names, list):
            company_names = []
    except Exception:
        company_names = []

    write_progress(progress_file, "running", 2, "국민연금 목록 조회 시작")

    nps_list = fetch_nps_vote_list(start_date, end_date, company_names, progress_file=progress_file)

    total = len(nps_list)
    write_progress(progress_file, "running", 20, f"국민연금 대상 {total}건 조회 완료", 0, total)

    all_rows = []
    fail_rows = []

    if total == 0:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="data", index=False)
            pd.DataFrame(columns=["corp_name", "stock_code", "meeting_date", "reason"]).to_excel(
                writer, sheet_name="fail", index=False
            )
        write_progress(progress_file, "done", 100, "완료", 0, 0)
        return

    for idx, item in enumerate(nps_list, start=1):
        corp_name = item["corp_name"]
        stock_code = item["stock_code"]
        meeting_date = item["meeting_date"]

        percent = 20 + int((idx / total) * 70)
        write_progress(
            progress_file,
            "running",
            percent,
            f"{idx}/{total} 처리 중: {corp_name}",
            idx,
            total
        )

        try:
            nps_detail_rows = fetch_nps_detail_table(item)
            if not nps_detail_rows:
                fail_rows.append([corp_name, stock_code, meeting_date, "nps_detail_not_found"])
                continue

            notices = fetch_dart_meeting_notice(corp_name, meeting_date)
            dart_agendas = []
            dart_rcept_no = ""
            dart_report_nm = ""

            if notices:
                notice = notices[0]
                dart_rcept_no = notice.get("rcept_no", "")
                dart_report_nm = notice.get("report_nm", "")

                folder = download_dart_document(dart_rcept_no)
                if folder:
                    main_file = find_main_file(folder)
                    if main_file:
                        dart_agendas = extract_dart_agendas(main_file)
                    shutil.rmtree(folder, ignore_errors=True)

            merged_rows = match_nps_with_dart(nps_detail_rows, dart_agendas)

            for row in merged_rows:
                row["corp_name"] = corp_name
                row["stock_code"] = stock_code
                row["meeting_date"] = meeting_date
                row["nps_category"] = item["category"]
                row["dart_rcept_no"] = dart_rcept_no
                row["dart_report_nm"] = dart_report_nm
                all_rows.append(row)

        except Exception as e:
            fail_rows.append([corp_name, stock_code, meeting_date, f"error: {str(e)}"])

        time.sleep(0.12)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_rows, columns=["corp_name", "stock_code", "meeting_date", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    write_progress(progress_file, "done", 100, "엑셀 생성 완료", total, total)


if __name__ == "__main__":
    main()