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
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

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


def normalize_agenda_no(text: str):
    if not text:
        return ""
    text = str(text).strip()
    m = re.search(r"(\d+(?:-\d+)?)", text)
    return m.group(1) if m else ""


def safe_text(locator):
    try:
        return locator.text_content() or ""
    except Exception:
        return ""


# -----------------------------------
# 국민연금: Playwright로 목록 + 상세 수집
# -----------------------------------
def setup_nps_search(page, start_date, end_date, company_keyword=""):
    page.goto(
        "https://fund.nps.or.kr/impa/edwmpblnt/getOHEF0007M0.do?menuId=MN24000636",
        wait_until="domcontentloaded",
        timeout=120000
    )

    page.wait_for_timeout(1500)

    visible_inputs = page.locator("input:visible")
    count = visible_inputs.count()

    text_like_inputs = []
    for i in range(count):
        inp = visible_inputs.nth(i)
        t = (inp.get_attribute("type") or "").lower()
        if t in ("text", "search", "date", ""):
            text_like_inputs.append(inp)

    if len(text_like_inputs) < 3:
        raise RuntimeError("국민연금 검색 입력창을 찾지 못했습니다.")

    # 회사명, 시작일, 종료일 순으로 가정
    company_input = text_like_inputs[0]
    start_input = text_like_inputs[1]
    end_input = text_like_inputs[2]

    company_input.fill(company_keyword)
    start_input.fill(start_date.replace("-", ""))
    end_input.fill(end_date.replace("-", ""))

    search_button = page.get_by_text("검색", exact=True).first
    search_button.click()

    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(2000)


def get_total_count(page):
    body_text = page.locator("body").text_content() or ""
    m = re.search(r"총\s*([\d,]+)\s*건", body_text)
    if not m:
        return 0
    return int(m.group(1).replace(",", ""))


def go_to_page(page, target_page):
    if target_page == 1:
        return

    # 페이지 숫자 링크가 안 보이면 "다음"을 눌러 pager block 이동
    safety = 0
    while safety < 30:
        safety += 1
        page_links = page.locator("a")
        link_count = page_links.count()

        found = False
        for i in range(link_count):
            txt = safe_text(page_links.nth(i)).strip()
            if txt == str(target_page):
                page_links.nth(i).click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1200)
                found = True
                break

        if found:
            return

        # 없으면 다음 블록으로
        next_clicked = False
        for i in range(link_count):
            txt = safe_text(page_links.nth(i)).strip()
            if txt == "다음":
                page_links.nth(i).click()
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(1200)
                next_clicked = True
                break

        if not next_clicked:
            raise RuntimeError(f"국민연금 목록 {target_page}페이지로 이동하지 못했습니다.")


def collect_rows_on_current_page(page, company_names=None):
    company_set = set(company_names or [])
    items = []

    rows = page.locator("table tbody tr")
    row_count = rows.count()

    for i in range(row_count):
        tr = rows.nth(i)
        cols = tr.locator("td")
        col_count = cols.count()

        if col_count < 5:
            continue

        values = [safe_text(cols.nth(j)).strip() for j in range(col_count)]
        if len(values) < 5:
            continue

        no = values[0]
        corp_name = values[1]
        stock_code = values[2]
        meeting_date = values[3]
        category = values[4]

        if "정기주총" not in category:
            continue

        if company_set and corp_name not in company_set:
            continue

        # 회사명 링크
        link = tr.locator("a").first
        if link.count() == 0:
            continue

        items.append({
            "no": no,
            "corp_name": corp_name,
            "stock_code": stock_code,
            "meeting_date": meeting_date,
            "category": category,
            "row_index": i
        })

    return items


def parse_detail_table_from_page(page):
    tables = page.locator("table")
    table_count = tables.count()
    if table_count == 0:
        return []

    best_rows = []
    best_table = None

    for i in range(table_count):
        tb = tables.nth(i)
        rows = tb.locator("tr")
        rc = rows.count()
        if rc > len(best_rows):
            best_rows = list(range(rc))
            best_table = tb

    if best_table is None:
        return []

    rows = best_table.locator("tr")
    row_count = rows.count()
    if row_count < 2:
        return []

    parsed_rows = []
    for i in range(row_count):
        tr = rows.nth(i)
        cells = tr.locator("th, td")
        cell_count = cells.count()
        if cell_count == 0:
            continue

        cols = [safe_text(cells.nth(j)).strip() for j in range(cell_count)]
        if cols:
            parsed_rows.append(cols)

    if len(parsed_rows) < 2:
        return []

    # 첫 줄 헤더 제거
    data_rows = parsed_rows[1:]

    normalized = []
    for idx, row in enumerate(data_rows, start=1):
        item = {"line_no": idx}
        for i, value in enumerate(row, start=1):
            item[f"nps_col_{i}"] = value

        joined = " ".join(row)
        item["agenda_no_norm"] = normalize_agenda_no(joined)
        normalized.append(item)

    return normalized


def fetch_nps_vote_list_and_details(start_date, end_date, company_names=None, progress_file=None):
    results = []
    fail_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()

        write_progress(progress_file, "running", 3, "국민연금 목록 검색 중...")

        # 전부/선택에 따라 회사명 검색칸은 비워두고 전체 검색 후 필터링
        setup_nps_search(page, start_date, end_date, company_keyword="")

        total_count = get_total_count(page)
        total_pages = max(1, (total_count + 9) // 10)

        write_progress(
            progress_file,
            "running",
            8,
            f"국민연금 목록 {total_count}건 / {total_pages}페이지 확인",
            0,
            total_pages
        )

        all_items = []

        for page_no in range(1, total_pages + 1):
            write_progress(
                progress_file,
                "running",
                min(25, 8 + int((page_no / total_pages) * 17)),
                f"국민연금 목록 수집 중... ({page_no}/{total_pages}페이지)",
                page_no,
                total_pages
            )

            if page_no > 1:
                go_to_page(page, page_no)

            page.wait_for_timeout(700)
            page_items = collect_rows_on_current_page(page, company_names=company_names)
            all_items.extend(page_items)

        # 중복 제거
        dedup = {}
        for item in all_items:
            key = (item["corp_name"], item["stock_code"], item["meeting_date"])
            dedup[key] = item
        all_items = list(dedup.values())

        total_items = len(all_items)
        if total_items == 0:
            browser.close()
            return [], [["", "", "", "nps_list_empty"]]

        # 상세 수집
        for idx, item in enumerate(all_items, start=1):
            corp_name = item["corp_name"]
            stock_code = item["stock_code"]
            meeting_date = item["meeting_date"]

            percent = 25 + int((idx / total_items) * 35)
            write_progress(
                progress_file,
                "running",
                percent,
                f"국민연금 상세 수집 중... {idx}/{total_items} {corp_name}",
                idx,
                total_items
            )

            try:
                # 다시 검색 화면 진입 후 해당 페이지 이동
                setup_nps_search(page, start_date, end_date, company_keyword="")

                page.wait_for_timeout(1000)

                # 현재 아이템이 어느 페이지에 있는지 다시 찾아야 하므로 전 페이지를 탐색
                found = False
                for page_no in range(1, total_pages + 1):
                    if page_no > 1:
                        go_to_page(page, page_no)
                    page.wait_for_timeout(500)

                    rows = page.locator("table tbody tr")
                    row_count = rows.count()

                    for r in range(row_count):
                        tr = rows.nth(r)
                        cols = tr.locator("td")
                        if cols.count() < 5:
                            continue

                        vals = [safe_text(cols.nth(j)).strip() for j in range(cols.count())]
                        if len(vals) < 5:
                            continue

                        cur_name = vals[1]
                        cur_code = vals[2]
                        cur_date = vals[3]
                        cur_cat = vals[4]

                        if (
                            cur_name == corp_name and
                            cur_code == stock_code and
                            cur_date == meeting_date and
                            "정기주총" in cur_cat
                        ):
                            link = tr.locator("a").first
                            link.click()
                            page.wait_for_load_state("domcontentloaded")
                            page.wait_for_timeout(1200)

                            detail_rows = parse_detail_table_from_page(page)
                            if not detail_rows:
                                fail_rows.append([corp_name, stock_code, meeting_date, "nps_detail_not_found"])
                            else:
                                for row in detail_rows:
                                    row["corp_name"] = corp_name
                                    row["stock_code"] = stock_code
                                    row["meeting_date"] = meeting_date
                                    row["nps_category"] = cur_cat
                                    results.append(row)

                            found = True
                            break

                    if found:
                        break

                if not found:
                    fail_rows.append([corp_name, stock_code, meeting_date, "nps_row_not_found_again"])

            except PlaywrightTimeoutError:
                fail_rows.append([corp_name, stock_code, meeting_date, "nps_detail_timeout"])
            except Exception as e:
                fail_rows.append([corp_name, stock_code, meeting_date, f"nps_error: {str(e)}"])

        browser.close()

    return results, fail_rows


# -----------------------------------
# DART 주주총회소집공고 수집
# -----------------------------------
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

    write_progress(progress_file, "running", 2, "국민연금 목록/상세 수집 시작")

    nps_rows, fail_rows = fetch_nps_vote_list_and_details(
        start_date, end_date, company_names, progress_file=progress_file
    )

    if not nps_rows:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="data", index=False)
            pd.DataFrame(fail_rows, columns=["corp_name", "stock_code", "meeting_date", "reason"]).to_excel(
                writer, sheet_name="fail", index=False
            )
        write_progress(progress_file, "done", 100, "완료", 0, 0)
        return

    # 회사/주총일 단위로 묶어서 DART 매칭
    grouped = {}
    for row in nps_rows:
        key = (row["corp_name"], row["stock_code"], row["meeting_date"])
        grouped.setdefault(key, []).append(row)

    total_groups = len(grouped)
    merged_rows = []
    group_idx = 0

    for (corp_name, stock_code, meeting_date), rows in grouped.items():
        group_idx += 1
        percent = 65 + int((group_idx / total_groups) * 30)
        write_progress(
            progress_file,
            "running",
            percent,
            f"DART 의안 매칭 중... {group_idx}/{total_groups} {corp_name}",
            group_idx,
            total_groups
        )

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

        dart_map = {}
        for item in dart_agendas:
            key = item.get("dart_agenda_no_norm", "")
            if key and key not in dart_map:
                dart_map[key] = item

        for row in rows:
            agenda_no = row.get("agenda_no_norm", "")
            dart_item = dart_map.get(agenda_no)

            out = dict(row)
            out["dart_rcept_no"] = dart_rcept_no
            out["dart_report_nm"] = dart_report_nm
            out["dart_agenda_no_norm"] = dart_item.get("dart_agenda_no_norm", "") if dart_item else ""
            out["dart_agenda_text"] = dart_item.get("dart_agenda_text", "") if dart_item else ""
            out["match_status"] = "matched" if dart_item else "unmatched"

            merged_rows.append(out)

        if not notices:
            fail_rows.append([corp_name, stock_code, meeting_date, "dart_notice_not_found"])

    result_df = pd.DataFrame(merged_rows)
    fail_df = pd.DataFrame(fail_rows, columns=["corp_name", "stock_code", "meeting_date", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    write_progress(progress_file, "done", 100, "엑셀 생성 완료", total_groups, total_groups)


if __name__ == "__main__":
    main()