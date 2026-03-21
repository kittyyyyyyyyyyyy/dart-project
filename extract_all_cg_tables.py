import requests
import zipfile
import io
import os
import time
import shutil
import pandas as pd
from bs4 import BeautifulSoup

# =========================
# 사용자 설정
# =========================

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 파일을 확인하세요.")

INPUT_FILE = "cg_reports.xlsx"
OUTPUT_FILE = "all_table_1_2_2.xlsx"

# 금융회사 구간 건너뛰기
START_INDEX = 41

# 테스트용: True면 START_INDEX부터 TEST_COUNT건만 실행
# 전체 실행하려면 False로 바꾸세요.
TEST_MODE = True
TEST_COUNT = 5

# =========================
# 원문 다운로드
# =========================
def download_report(rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {
        "crtfc_key": API_KEY,
        "rcept_no": str(rcept_no)
    }

    response = requests.get(url, params=params, timeout=60)

    folder_name = f"report_{rcept_no}"

    # 기존 폴더가 있으면 삭제 후 다시 생성
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
    os.makedirs(folder_name, exist_ok=True)

    try:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(folder_name)
        return folder_name
    except zipfile.BadZipFile:
        return None

# =========================
# 본문 파일 찾기
# =========================
def find_main_file(folder_name):
    candidates = []

    for fname in os.listdir(folder_name):
        lower = fname.lower()
        if lower.endswith(".xml") or lower.endswith(".html") or lower.endswith(".htm"):
            full_path = os.path.join(folder_name, fname)
            candidates.append(full_path)

    if not candidates:
        return None

    # 가장 큰 파일을 본문 후보로 사용
    candidates.sort(key=lambda x: os.path.getsize(x), reverse=True)
    return candidates[0]

# =========================
# 표 1-2-2 추출
# =========================
def extract_table_rows(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    target_text_node = None

    # 문서 전체에서 "표 1-2-2" 또는 "주주총회 의결 내용" 찾기
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

# =========================
# 메인 실행
# =========================
def main():
    print("실행 시작")
    print("입력 파일:", INPUT_FILE)
    print("출력 파일:", OUTPUT_FILE)

    if not os.path.exists(INPUT_FILE):
        print("입력 파일이 없습니다:", INPUT_FILE)
        return

    df = pd.read_excel(INPUT_FILE)

    required_cols = ["corp_name", "stock_code", "rcept_no", "report_nm", "rcept_dt"]
    for col in required_cols:
        if col not in df.columns:
            print(f"필수 컬럼 없음: {col}")
            return

    if TEST_MODE:
        target_df = df.iloc[START_INDEX:START_INDEX + TEST_COUNT].copy()
        print(f"테스트 모드: {START_INDEX}번째 이후 {TEST_COUNT}건만 실행")
    else:
        target_df = df.iloc[START_INDEX:].copy()
        print(f"전체 모드: {START_INDEX}번째 이후 전체 실행")

    all_rows = []
    fail_list = []

    for seq, (_, row) in enumerate(target_df.iterrows(), start=1):
        corp_name = str(row["corp_name"])
        stock_code = str(row["stock_code"])
        rcept_no = str(row["rcept_no"])
        report_nm = str(row["report_nm"])
        rcept_dt = str(row["rcept_dt"])

        print("=" * 60)
        print(f"{seq}번째 처리 중: {corp_name} / {rcept_no}")

        folder_name = download_report(rcept_no)
        if not folder_name:
            print("원문 다운로드 실패")
            fail_list.append([corp_name, stock_code, rcept_no, "download_fail"])
            continue

        main_file = find_main_file(folder_name)
        if not main_file:
            print("본문 파일 찾기 실패")
            fail_list.append([corp_name, stock_code, rcept_no, "main_file_not_found"])
            continue

        print("본문 파일:", os.path.basename(main_file))

        try:
            table_rows = extract_table_rows(main_file)

            if not table_rows:
                print("표 1-2-2 찾기 실패")
                fail_list.append([corp_name, stock_code, rcept_no, "table_not_found"])
                continue

            print("원본 표 행 수:", len(table_rows))

            # 1) 첫 줄은 헤더라고 보고 제거 → 컬럼 제목 반복 저장 방지
            data_rows = table_rows[1:]

            if not data_rows:
                print("헤더 제거 후 데이터 없음")
                fail_list.append([corp_name, stock_code, rcept_no, "no_data_after_header"])
                continue

            # 2) 첫 데이터 행의 col_1을 기준값으로 삼음
            first_value = data_rows[0][0].strip()

            saved_count = 0

            for line_no, cols in enumerate(data_rows, start=1):
                if not cols:
                    continue

                current_value = cols[0].strip()

                # col_1 값이 바뀌면 그 회사 표 저장 중단
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

            print("저장된 행 수:", saved_count)

            if saved_count == 0:
                fail_list.append([corp_name, stock_code, rcept_no, "no_rows_saved"])

        except Exception as e:
            print("추출 중 오류:", str(e))
            fail_list.append([corp_name, stock_code, rcept_no, f"extract_error: {str(e)}"])

        time.sleep(0.2)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    print("=" * 60)
    print("완료!")
    print("성공 행 수:", len(result_df))
    print("실패 건 수:", len(fail_df))
    print("파일 생성:", OUTPUT_FILE)

if __name__ == "__main__":
    main()