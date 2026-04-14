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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
import re

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BEDROCK_MODEL_ID = "amazon.nova-lite-v1:0"

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 또는 환경 변수를 확인하세요.")


def make_session():
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http = make_session()


def create_bedrock_client():
    try:
        return boto3.client("bedrock-runtime", region_name=AWS_REGION)
    except Exception as e:
        print(f"Bedrock 클라이언트 생성 실패: {e}")
        return None


bedrock_client = create_bedrock_client()


def call_bedrock(prompt):
    """AWS Bedrock Amazon Nova Lite 호출. JSON 문자열 반환."""
    if not bedrock_client:
        raise RuntimeError("bedrock_client가 None입니다. IAM 권한을 확인하세요.")

    body = {
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {
            "max_new_tokens": 3000,
            "temperature": 0
        }
    }

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(body)
    )
    result = json.loads(response["body"].read())
    return result["output"]["message"]["content"][0]["text"]


def extract_json(text):
    """AI 응답 텍스트에서 JSON 객체를 추출한다."""
    try:
        return json.loads(text)
    except Exception:
        pass
    # 코드블록 안에 있는 경우 처리
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # 중괄호 범위로 추출
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass
    return None


def write_progress(progress_file, status, percent, message, current=0, total=0):
    if not progress_file:
        return
    data = {"status": status, "percent": percent, "message": message,
            "current": current, "total": total}
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def chunk_date_ranges(start_date_str, end_date_str, chunk_days=90):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    ranges = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        ranges.append((current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        current = chunk_end + timedelta(days=1)
    return ranges


def safe_get(url, params):
    return http.get(url, params=params, timeout=(20, 120))


# ─────────────────────────────────────────────
# 1. DART API에서 주주총회소집공고 보고서 목록 가져오기
# ─────────────────────────────────────────────

def fetch_agm_notice_reports(start_date, end_date, company_names=None, progress_file=None):
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
            write_progress(progress_file, "running", approx_percent,
                           f"공시 검색 중... ({bgn_de}~{end_de}, 시장 {corp_cls})", step, total_steps)

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
                if data.get("status") != "000":
                    break
                items = data.get("list", [])
                if not items:
                    break

                for item in items:
                    report_nm = str(item.get("report_nm", ""))
                    corp_name = str(item.get("corp_name", ""))
                    # 주주총회소집공고, [기재정정]주주총회소집공고 등 모두 포함
                    if "주주총회소집공고" not in report_nm:
                        continue
                    if company_names_set and corp_name not in company_names_set:
                        continue
                    all_results.append(item)

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
    params = {"crtfc_key": API_KEY, "rcept_no": str(rcept_no)}
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
            candidates.append(os.path.join(folder_name, fname))
    if not candidates:
        return None
    candidates.sort(key=lambda x: os.path.getsize(x), reverse=True)
    return candidates[0]


# ─────────────────────────────────────────────
# 2. 문서에서 핵심 섹션 텍스트 추출
# ─────────────────────────────────────────────

def extract_text_sections(file_path):
    """
    HTML 문서에서 두 섹션을 추출한다:
    - notice_text: 주주총회 소집공고 섹션 (일시, 결의사항 포함)
    - section2_text: 주주총회 목적사항별 기재사항 섹션 (안건 상세 내용)
    - full_text: 전체 텍스트 (notice_text가 짧을 경우 AI fallback용)
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    # 공백/빈줄 정리해서 텍스트 추출
    import re as _re
    full_text = soup.get_text('\n')
    full_text = _re.sub(r'\n{4,}', '\n\n', full_text)
    full_text = _re.sub(r'[ \t]{3,}', '  ', full_text)

    # 소집공고 섹션 시작점 찾기
    notice_start = 0
    for kw in ["주주총회 소집공고", "주주총회소집공고", "소 집 공 고",
               "주주총 회 소집공고", "주 주 총 회 소 집 공 고"]:
        idx = full_text.find(kw)
        if idx != -1:
            notice_start = idx
            break

    # 목적사항별 기재사항 섹션 시작점 찾기
    section2_start = -1
    for kw in ["주주총회 목적사항별 기재사항", "주주총회목적사항별기재사항",
               "목적사항별 기재사항", "목 적 사 항 별 기 재 사 항",
               "주주총회목적사항별기재사항"]:
        idx = full_text.find(kw, notice_start)
        if idx != -1:
            section2_start = idx
            break

    if section2_start == -1:
        notice_text = full_text[notice_start:notice_start + 8000]
        section2_text = ""
    else:
        notice_text = full_text[notice_start:section2_start]
        section2_text = full_text[section2_start:section2_start + 60000]

    return notice_text, section2_text, full_text


# ─────────────────────────────────────────────
# 3. AI로 소집공고 섹션 파싱 + G/H/I/J 분석 통합
# ─────────────────────────────────────────────

def parse_and_analyze_with_ai(notice_text, corp_name, full_text_fallback=""):
    """
    AWS Bedrock Claude Haiku를 사용해 소집공고 텍스트를 파싱·분석한다.
    반환: {meeting_date, agenda_items, error}
    """
    if not bedrock_client:
        return {"meeting_date": "", "agenda_items": [],
                "error": "bedrock_client_none: IAM 권한 또는 리전 설정 확인 필요"}

    # notice_text가 너무 짧으면 full_text 앞부분으로 fallback
    text_to_use = notice_text
    if len(notice_text.strip()) < 300 and full_text_fallback:
        text_to_use = full_text_fallback[:8000]

    text_excerpt = text_to_use[:7000]

    prompt = f"""다음은 "{corp_name}"의 주주총회소집공고 문서 내용입니다.

{text_excerpt}

아래 정보를 JSON으로 추출·분석하세요. 반드시 JSON만 반환하고 다른 텍스트는 포함하지 마세요.

1. meeting_date: 주주총회 일시. 예: "2026년 3월 28일(금) 오전 9시". 없으면 빈 문자열.

2. agenda_items: 결의사항의 안건 목록.

   [안건 추출 규칙]
   - 결의사항에 있는 안건만 추출 (보고사항 제외)
   - 재무제표 승인 안건도 포함
   - 하위 안건(제N-1호, 제N-2호 등)이 있는 경우:
       * 상위 안건 제목 + 첫 번째 하위 안건 제목 → 하나의 항목 (줄바꿈으로 연결)
       * 두 번째 하위 안건부터 → 각각 별도 항목
   - 원문 표현을 최대한 그대로 유지

   [각 항목 필드]
   - num: 안건번호. 예: "1", "2", "2-1", "2-2"
   - title: 안건 제목 원문 (하위안건 합친 경우 \\n으로 연결)
   - shareholder_proposal: 해당 안건 앞에 "(주주제안)" 명시 시 "Y", 아니면 "N"
   - proposer: shareholder_proposal이 "Y"이면 제안 주주명, 아니면 ""
   - category1: "재무제표승인" / "이사감사선임" / "정관변경" / "이사감사보수" / "자사주보유처분계획승인" / "기타" 중 하나
   - category2: category1이 "정관변경"일 때만 "이사 임기 유연화" / "이사 임기 연장" / "이사 정원 축소" / "자사주 보유" / "기타 개정 상법 반영" 중 하나, 나머지는 ""

반환 형식:
{{"meeting_date": "...", "agenda_items": [{{"num": "1", "title": "...", "shareholder_proposal": "N", "proposer": "", "category1": "...", "category2": ""}}]}}"""

    try:
        raw_text = call_bedrock(prompt)
        result = extract_json(raw_text)
        if result is None:
            return {"meeting_date": "", "agenda_items": [],
                    "error": f"json_parse_fail: {raw_text[:200]}"}
        result.setdefault("error", "")
        return result
    except Exception as e:
        err_msg = str(e)
        print(f"Bedrock 호출 오류 ({corp_name}): {err_msg}")
        return {"meeting_date": "", "agenda_items": [],
                "error": f"bedrock_exception:{err_msg[:200]}"}


# ─────────────────────────────────────────────
# 4. 목적사항별 기재사항에서 안건별 내용 추출 (F열)
# ─────────────────────────────────────────────

def build_section2_content_map(section2_text):
    """
    section2 텍스트에서 안건번호 → 상세 내용 매핑을 만든다.
    예: {"1": "...", "2": "...", "2-1": "..."}
    """
    if not section2_text:
        return {}

    import re
    # 섹션 헤더 패턴: □ 제N호, ■ 제N호, ㅁ 제N호 의안 등
    header_pattern = re.compile(
        r'(?:□|■|ㅁ|▣|◆|◇|▶|●|○)?\s*제\s*(\d+(?:-\d+)?)\s*호(?:\s*의\s*안)?',
        re.MULTILINE
    )
    headers = [(m.group(1).replace(' ', ''), m.start(), m.end())
               for m in header_pattern.finditer(section2_text)]

    if not headers:
        return {}

    content_map = {}
    for idx, (num, start, end) in enumerate(headers):
        next_start = headers[idx + 1][1] if idx + 1 < len(headers) else min(start + 8000, len(section2_text))
        raw_content = section2_text[end:next_start].strip()
        import re as _re
        raw_content = _re.sub(r'\n{3,}', '\n\n', raw_content)
        raw_content = _re.sub(r'[ \t]{2,}', ' ', raw_content)
        content_map[num] = raw_content.strip()

    return content_map


def is_financial_statement_item(title_text):
    """재무제표 관련 안건인지 확인 — F열 내용을 비워야 하는 안건"""
    for kw in ['재무제표', '연결재무제표', '이익잉여금처분']:
        if kw in title_text:
            return True
    return False


def get_agenda_content(content_map, agenda_num, agenda_title):
    """
    안건번호로 section2 내용을 찾아 반환한다.
    재무제표 안건은 빈 문자열 반환.
    """
    import re
    if is_financial_statement_item(agenda_title):
        return ""

    num_clean = agenda_num.replace(' ', '')
    if not num_clean:
        return ""

    # 직접 매칭
    if num_clean in content_map:
        return content_map[num_clean]

    # 하위 안건: 상위 안건 내용에서 해당 하위 부분 탐색
    if '-' in num_clean:
        base = num_clean.split('-')[0]
        if base in content_map:
            parent_content = content_map[base]
            m = re.search(r'제\s*' + re.escape(num_clean) + r'\s*호', parent_content)
            if m:
                sub_start = m.start()
                next_sub = re.search(
                    r'제\s*' + base + r'-\d+\s*호',
                    parent_content[sub_start + 5:]
                )
                if next_sub:
                    return parent_content[sub_start: sub_start + 5 + next_sub.start()].strip()
                return parent_content[sub_start:].strip()

    return ""


# ─────────────────────────────────────────────
# 5. 메인
# ─────────────────────────────────────────────

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

    reports = fetch_agm_notice_reports(start_date, end_date, company_names, progress_file=progress_file)
    total_reports = len(reports)
    write_progress(progress_file, "running", 45, f"공시 {total_reports}건 조회 완료", 0, total_reports)

    all_rows = []
    fail_list = []

    if total_reports == 0:
        columns = ["회사명", "시장분류", "공고일", "주총일", "안건 제목",
                   "안건 내용", "주주제안여부", "주주제안자", "안건분류1", "안건분류2"]
        result_df = pd.DataFrame(columns=columns)
        fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="data", index=False)
            fail_df.to_excel(writer, sheet_name="fail", index=False)
        write_progress(progress_file, "done", 100, "완료 (검색 결과 없음)", 0, 0)
        return

    for seq, report in enumerate(reports, start=1):
        corp_name = str(report.get("corp_name", ""))
        stock_code = str(report.get("stock_code", ""))
        rcept_no = str(report.get("rcept_no", ""))
        corp_cls = str(report.get("corp_cls", ""))
        rcept_dt = str(report.get("rcept_dt", ""))

        market_label = ""
        if corp_cls == "Y":
            market_label = "유가증권(코스피)"
        elif corp_cls == "K":
            market_label = "코스닥"

        percent = 45 + int((seq / total_reports) * 50)
        write_progress(progress_file, "running", percent,
                       f"{seq}/{total_reports} 처리 중: {corp_name}", seq, total_reports)

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
            # 소집공고 섹션 / 목적사항별 기재사항 섹션 분리 추출
            notice_text, section2_text, full_text = extract_text_sections(main_file)

            # AI로 D/E열 파싱 + G/H/I/J열 분석 (보고서 1건당 1회 호출)
            # notice_text가 짧으면 full_text를 fallback으로 전달
            parsed = parse_and_analyze_with_ai(notice_text, corp_name,
                                               full_text_fallback=full_text)
            meeting_date = parsed.get("meeting_date", "")
            agenda_items = parsed.get("agenda_items", [])
            ai_error = parsed.get("error", "")

            if not agenda_items:
                # 원인을 구체적으로 기록 (OPENAI_API_KEY 미설정인지, AI가 빈값 반환인지 등)
                reason = ai_error if ai_error else "ai_returned_no_agenda_items"
                fail_list.append([corp_name, stock_code, rcept_no, reason])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            # F열용 section2 내용 맵 구성
            content_map = build_section2_content_map(section2_text)

            for item in agenda_items:
                agenda_num = str(item.get("num", ""))
                agenda_title = str(item.get("title", ""))
                agenda_content = get_agenda_content(content_map, agenda_num, agenda_title)

                all_rows.append({
                    "회사명": corp_name,
                    "시장분류": market_label,
                    "공고일": rcept_dt,
                    "주총일": meeting_date,
                    "안건 제목": agenda_title,
                    "안건 내용": agenda_content,
                    "주주제안여부": str(item.get("shareholder_proposal", "")),
                    "주주제안자": str(item.get("proposer", "")),
                    "안건분류1": str(item.get("category1", "")),
                    "안건분류2": str(item.get("category2", "")),
                })

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
