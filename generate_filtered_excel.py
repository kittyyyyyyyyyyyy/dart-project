import requests
import zipfile
import io
import os
import time
import shutil
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import argparse
from datetime import datetime, timedelta
import json
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 또는 환경 변수를 확인하세요.")


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


def safe_get(url, params):
    return http.get(url, params=params, timeout=(20, 120))


def fetch_latest_reports(start_date, end_date, company_names=None, progress_file=None):
    url = "https://opendart.fss.or.kr/api/list.json"
    all_results = []

    markets = ["Y", "K"]
    company_names_set = set(company_names or [])

    ranges = chunk_date_ranges(start_date, end_date)
    total_steps = max(1, len(ranges) * len(markets))
    step = 0

    for bgn_de, end_de in ranges:
        for corp_cls in markets:
            step += 1
            approx_percent = min(40, 5 + int((step / total_steps) * 35))
            write_progress(
                progress_file,
                "running",
                approx_percent,
                f"공시 검색 중... ({bgn_de}~{end_de}, 시장 {corp_cls})",
                step,
                total_steps
            )

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

                res = safe_get(url, params)
                data = res.json()

                status = data.get("status")
                if status != "000":
                    break

                items = data.get("list", [])
                if not items:
                    break

                for item in items:
                    report_nm = str(item.get("report_nm", ""))
                    corp_name = str(item.get("corp_name", ""))

                    if "기업지배구조보고서" not in report_nm:
                        continue

                    if company_names_set and corp_name not in company_names_set:
                        continue

                    all_results.append(item)

                write_progress(
                    progress_file,
                    "running",
                    approx_percent,
                    f"공시 검색 중... ({bgn_de}~{end_de}, 시장 {corp_cls}, page {page_no})",
                    step,
                    total_steps
                )

                if len(items) < 100:
                    break

                page_no += 1
                time.sleep(0.12)

    dedup = {}
    for item in all_results:
        dedup[item["rcept_no"]] = item

    return list(dedup.values())


def download_report(rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {
        "crtfc_key": API_KEY,
        "rcept_no": str(rcept_no)
    }

    response = safe_get(url, params)

    folder_name = f"report_{rcept_no}"

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
            full_path = os.path.join(folder_name, fname)
            candidates.append(full_path)

    if not candidates:
        return None

    candidates.sort(key=lambda x: os.path.getsize(x), reverse=True)
    return candidates[0]


def extract_table_rows(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    target_text_node = None

    for tag in soup.find_all(string=True):
        text = tag.strip()
        if "표 1-2-2" in text or "주주총회 의결 내용" in text:
            target_text_node = tag
            break

    if not target_text_node:
        return []

    parent = target_text_node.parent
    next_table = parent.find_next("table")

    if not next_table:
        return []

    found_rows = []

    rows = next_table.find_all("tr")
    for tr in rows:
        cols = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if cols:
            found_rows.append(cols)

    return found_rows


def select_target_group_rows(table_rows):
    if not table_rows or len(table_rows) < 2:
        return []

    data_rows = table_rows[1:]
    if not data_rows:
        return []

    groups = []
    current_key = None
    current_rows = []

    for row in data_rows:
        if not row:
            continue

        key = row[0].strip() if len(row) > 0 else ""

        if current_key is None:
            current_key = key
            current_rows = [row]
        elif key == current_key:
            current_rows.append(row)
        else:
            groups.append((current_key, current_rows))
            current_key = key
            current_rows = [row]

    if current_rows:
        groups.append((current_key, current_rows))

    candidates = []

    for key, rows in groups:
        if "정기" not in key:
            continue

        m = re.search(r"(\d+)\s*기", key)
        period_num = int(m.group(1)) if m else -1

        candidates.append({
            "key": key,
            "rows": rows,
            "period_num": period_num
        })

    if not candidates:
        return []

    numeric_candidates = [c for c in candidates if c["period_num"] >= 0]
    if numeric_candidates:
        best = max(numeric_candidates, key=lambda x: x["period_num"])
        return best["rows"]

    return candidates[0]["rows"]


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

    write_progress(progress_file, "running", 1, "작업 시작 중...")

    reports = fetch_latest_reports(start_date, end_date, company_names, progress_file=progress_file)

    total_reports = len(reports)
    write_progress(progress_file, "running", 45, f"공시 {total_reports}건 조회 완료", 0, total_reports)

    all_rows = []
    fail_list = []

    if total_reports == 0:
        result_df = pd.DataFrame(all_rows)
        fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="data", index=False)
            fail_df.to_excel(writer, sheet_name="fail", index=False)

        write_progress(progress_file, "done", 100, "완료", 0, 0)
        return

    for seq, row in enumerate(reports, start=1):
        corp_name = str(row.get("corp_name", ""))
        stock_code = str(row.get("stock_code", ""))
        rcept_no = str(row.get("rcept_no", ""))
        report_nm = str(row.get("report_nm", ""))
        rcept_dt = str(row.get("rcept_dt", ""))

        percent = 45 + int((seq / total_reports) * 50)
        write_progress(
            progress_file,
            "running",
            percent,
            f"{seq}/{total_reports} 처리 중: {corp_name}",
            seq,
            total_reports
        )

        folder_name = download_report(rcept_no)
        if not folder_name:
            fail_list.append([corp_name, stock_code, rcept_no, "download_fail"])
            continue

        main_file = find_main_file(folder_name)
        if not main_file:
            fail_list.append([corp_name, stock_code, rcept_no, "main_file_not_found"])
            shutil.rmtree(folder_name, ignore_errors=True)
            continue

        try:
            table_rows = extract_table_rows(main_file)

            if not table_rows:
                fail_list.append([corp_name, stock_code, rcept_no, "table_not_found"])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            selected_rows = select_target_group_rows(table_rows)

            if not selected_rows:
                fail_list.append([corp_name, stock_code, rcept_no, "target_group_not_found"])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            for line_no, cols in enumerate(selected_rows, start=1):
                row_dict = {
                    "corp_name": corp_name,
                    "stock_code": stock_code,
                    "rcept_no": rcept_no,
                    "report_nm": report_nm,
                    "rcept_dt": rcept_dt,
                    "line_no": line_no
                }

                for i, value in enumerate(cols, start=1):
                    row_dict[f"col_{i}"] = value

                all_rows.append(row_dict)

        except Exception as e:
            fail_list.append([corp_name, stock_code, rcept_no, f"extract_error: {str(e)}"])

        finally:
            shutil.rmtree(folder_name, ignore_errors=True)

        time.sleep(0.08)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    write_progress(progress_file, "done", 100, "엑셀 생성 완료", total_reports, total_reports)


if __name__ == "__main__":
    main()