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

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 또는 환경 변수를 확인하세요.")


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


def fetch_latest_reports(start_date, end_date, company_keyword=""):
    """
    OpenDART에서 최신 공시 목록을 직접 조회
    """
    url = "https://opendart.fss.or.kr/api/list.json"
    all_results = []

    # 코스피(Y), 코스닥(K) 둘 다 조회
    markets = ["Y", "K"]

    for bgn_de, end_de in chunk_date_ranges(start_date, end_date):
        for corp_cls in markets:
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

                status = data.get("status")
                if status != "000":
                    break

                items = data.get("list", [])
                if not items:
                    break

                for item in items:
                    report_nm = str(item.get("report_nm", ""))
                    corp_name = str(item.get("corp_name", ""))

                    # 기업지배구조보고서만 대상으로
                    if "기업지배구조보고서" not in report_nm:
                        continue

                    # 회사명 필터가 있으면 적용
                    if company_keyword and company_keyword.lower() not in corp_name.lower():
                        continue

                    all_results.append(item)

                if len(items) < 100:
                    break

                page_no += 1
                time.sleep(0.2)

    # 중복 접수번호 제거
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

    response = requests.get(url, params=params, timeout=60)

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)   # YYYY-MM-DD
    parser.add_argument("--end-date", required=True)     # YYYY-MM-DD
    parser.add_argument("--company", default="")
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    company_keyword = args.company.strip()
    output_file = args.output

    print("최신 OpenDART 목록 조회 시작")
    reports = fetch_latest_reports(start_date, end_date, company_keyword)
    print("조회된 공시 수:", len(reports))

    all_rows = []
    fail_list = []

    for seq, row in enumerate(reports, start=1):
        corp_name = str(row.get("corp_name", ""))
        stock_code = str(row.get("stock_code", ""))
        rcept_no = str(row.get("rcept_no", ""))
        report_nm = str(row.get("report_nm", ""))
        rcept_dt = str(row.get("rcept_dt", ""))

        print("=" * 60)
        print(f"{seq}번째 처리 중: {corp_name} / {rcept_no}")

        folder_name = download_report(rcept_no)
        if not folder_name:
            fail_list.append([corp_name, stock_code, rcept_no, "download_fail"])
            continue

        main_file = find_main_file(folder_name)
        if not main_file:
            fail_list.append([corp_name, stock_code, rcept_no, "main_file_not_found"])
            continue

        try:
            table_rows = extract_table_rows(main_file)

            if not table_rows:
                fail_list.append([corp_name, stock_code, rcept_no, "table_not_found"])
                continue

            # 헤더 제거
            data_rows = table_rows[1:]
            if not data_rows:
                fail_list.append([corp_name, stock_code, rcept_no, "no_data_after_header"])
                continue

            # col_1 값이 바뀌기 전까지만 저장
            first_value = data_rows[0][0].strip()

            saved_count = 0
            for line_no, cols in enumerate(data_rows, start=1):
                if not cols:
                    continue

                current_value = cols[0].strip()
                if current_value != first_value:
                    break

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
                saved_count += 1

            if saved_count == 0:
                fail_list.append([corp_name, stock_code, rcept_no, "no_rows_saved"])

        except Exception as e:
            fail_list.append([corp_name, stock_code, rcept_no, f"extract_error: {str(e)}"])

        time.sleep(0.2)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    print("완료:", output_file)


if __name__ == "__main__":
    main()