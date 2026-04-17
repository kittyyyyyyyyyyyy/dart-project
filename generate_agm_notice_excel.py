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
BEDROCK_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 또는 환경 변수를 확인하세요.")


def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; AGMNoticeBot/1.0)"
    })
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


def call_bedrock(prompt, max_tokens=4096):
    """AWS Bedrock Claude 3 Haiku 호출. JSON 문자열 반환."""
    if not bedrock_client:
        raise RuntimeError("bedrock_client가 None입니다. IAM 권한을 확인하세요.")

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}]
    }

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(body)
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]


def extract_json(text):
    """
    AI 응답 텍스트에서 JSON 객체를 추출한다.
    코드블록(```json```) 제거 후 중괄호 depth 추적으로 정확하게 파싱한다.
    """
    # 1차: 코드블록 마커 제거 후 직접 파싱
    cleaned = re.sub(r'```(?:json)?\s*', '', text)
    cleaned = re.sub(r'```', '', cleaned).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # 2차: 중괄호 depth를 추적해 완전한 JSON 객체 범위를 정확히 찾아 파싱
    start = text.find('{')
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape_next = False

    for i, char in enumerate(text[start:], start):
        if escape_next:
            escape_next = False
            continue
        if char == '\\' and in_string:
            escape_next = True
            continue
        if char == '"':
            in_string = not in_string
        if not in_string:
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i + 1])
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


def safe_get(url, params, retries=3):
    """DART API GET 요청. ConnectTimeout 발생 시 지수 백오프로 재시도."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = http.get(url, params=params, timeout=(60, 180))
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            last_err = e
            wait = min(2 ** attempt, 30)
            print(f"[WARN] DART 요청 실패 ({attempt}/{retries}): {e} — {wait}초 후 재시도")
            time.sleep(wait)
    raise last_err


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

    # corp_code 기준으로 가장 최근 공고(rcept_no 최대값)만 남김
    # → 정정공고가 있으면 정정공고만, 없으면 원본만 엑셀에 포함
    dedup = {}
    for item in all_results:
        corp_code = item.get("corp_code", item["rcept_no"])
        existing = dedup.get(corp_code)
        if existing is None or item["rcept_no"] > existing["rcept_no"]:
            dedup[corp_code] = item
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

    ※ 정정공고의 경우 "주주총회 소집공고" 키워드가 정정비교표에 먼저 등장해
       실제 의안 목록이 아닌 비교표 텍스트를 읽는 오류를 방지하기 위해,
       "부의안건"이 실제로 존재하는 소집공고 절을 정확히 찾는다.
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    # 공백/빈줄 정리해서 텍스트 추출
    import re as _re
    full_text = soup.get_text('\n')
    full_text = _re.sub(r'\n{4,}', '\n\n', full_text)
    full_text = _re.sub(r'[ \t]{3,}', '  ', full_text)

    # ── 소집공고 섹션 시작점 찾기 ──
    # 전략: "부의안건" 키워드를 직접 찾아 역방향으로 올라가 실제 소집공고 시작점을 찾는다.
    # 이렇게 하면 정정공고 비교표에 "주주총회 소집공고"가 먼저 등장해도 영향 없음.
    notice_start = 0

    # "부의안건" 위치 탐색
    agenda_section_pats = [
        r'나\s*[\.．]\s*부\s*의\s*안\s*건',
        r'나\s*\.\s*부의안건',
        r'부\s*의\s*안\s*건',
    ]
    agenda_pos = -1
    for pat in agenda_section_pats:
        m = _re.search(pat, full_text)
        if m:
            agenda_pos = m.start()
            break

    if agenda_pos != -1:
        # "부의안건"보다 앞에서 가장 가까운 "주주총회 소집공고" 또는 "일 시" 등을 찾아
        # 그 위치를 notice_start로 사용 (최대 3000자 역방향 탐색)
        search_window = full_text[max(0, agenda_pos - 3000): agenda_pos]
        # 역방향에서 "주주총회 소집공고" 또는 "(제N기 정기)" 등 찾기
        back_patterns = [
            r'주\s*주\s*총\s*회\s*소\s*집\s*공\s*고',
            r'소\s*집\s*공\s*고',
            r'\(제\d+기\s*정기\)',
            r'[1１]\s*\.\s*일\s*시',
        ]
        best_back = None
        for bp in back_patterns:
            for bm in _re.finditer(bp, search_window):
                best_back = bm  # 마지막(가장 늦은) 매치를 사용
        if best_back is not None:
            notice_start = max(0, agenda_pos - 3000) + best_back.start()
        else:
            # 역방향 탐색 실패 → 부의안건 500자 앞부터
            notice_start = max(0, agenda_pos - 500)
    else:
        # fallback: "주주총회 소집공고" 첫 번째 위치
        for kw in ["주주총회 소집공고", "주주총회소집공고", "소 집 공 고"]:
            idx = full_text.find(kw)
            if idx != -1:
                notice_start = idx
                break

    # ── 목적사항별 기재사항 섹션 시작점 찾기 ──
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
        text_to_use = full_text_fallback[:20000]

    # "부의안건" 위치를 찾아 그 앞 500자 + 이후 15000자를 우선 사용
    # (notice_text 앞부분에 장소·일시 설명이 길어 의안 목록이 잘리는 것을 방지)
    import re as _re2
    agenda_section_pat = _re2.compile(r'나\s*[\.．]\s*부의안건|나\.\s*부\s*의\s*안\s*건|부의\s*안건')
    m = agenda_section_pat.search(text_to_use)
    if m and m.start() > 500:
        # 부의안건 섹션 500자 앞부터 15000자
        slice_start = max(0, m.start() - 500)
        text_excerpt = text_to_use[slice_start: slice_start + 15000]
    else:
        text_excerpt = text_to_use[:15000]

    prompt = f"""다음은 "{corp_name}"의 주주총회소집공고 문서 내용입니다.

{text_excerpt}

아래 정보를 JSON으로 추출·분석하세요. 반드시 JSON만 반환하고 다른 텍스트는 포함하지 마세요.

1. meeting_date: 주주총회 일시. 예: "2026년 3월 28일(금) 오전 9시". 없으면 빈 문자열.

2. agenda_items: 결의사항의 안건 목록.

   [안건 추출 규칙]
   - 결의사항(부의안건)에 있는 안건만 추출 (보고사항 제외)
   - 재무제표 승인 안건도 포함
   - 안건에 하위 안건이 있는 경우, 반드시 최하위 안건만 추출:
       * 2단계: 제2호 아래 제2-1호, 제2-2호가 있으면 → 제2-1호, 제2-2호만 추출 (제2호 제외)
       * 3단계: 제2-1호 아래 제2-1-1호, 제2-1-2호가 있으면 → 제2-1-1호, 제2-1-2호만 추출 (제2호, 제2-1호 모두 제외)
       * 즉, 하위 안건이 있는 상위 안건은 절대 포함하지 않음
   - 하위 안건이 없는 단독 안건(제1호, 제4호 등)은 그대로 포함
   - 원문 표현을 최대한 그대로 유지
   - ★ 중요: "가결될 경우에만 상정", "자동 폐기", "조건부" 등의 문구가 있는 안건도
     반드시 포함하여 추출할 것 (조건 여부와 무관하게 안건 목록에 기재된 것은 모두 추출)
   - ★ 중요: 주주제안 안건의 하위 항목도 일반 안건과 동일하게 각각 별도 항목으로 추출할 것

   [각 항목 필드]
   - num: 안건번호 (숫자만, 하이픈으로 연결). 예: "1", "2-1", "2-1-1", "2-1-2", "3-1", "6-1"
   - title: 안건 제목 원문 (하위안건 합친 경우 \\n으로 연결)
   - shareholder_proposal: 해당 안건 또는 그 상위 안건 앞에 "(주주제안)" 명시 시 "Y", 아니면 "N"
   - proposer: shareholder_proposal이 "Y"이면 제안 주주명, 아니면 ""
   - category1: "재무제표승인" / "이사감사선임" / "정관변경" / "이사감사보수" / "자사주보유처분계획승인" / "기타" 중 하나
   - category2: 빈 문자열("")로 반환 (정관변경 안건의 세부 분류는 별도 처리됨)

반환 규칙:
- 반드시 순수 JSON만 반환할 것
- 코드블록(```) 절대 사용 금지
- 설명 텍스트 없이 JSON 객체만 출력

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
        r'(?:□|■|ㅁ|▣|◆|◇|▶|●|○)?\s*제\s*(\d+(?:-\d+)*)\s*호(?:\s*의\s*안)?',
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

    # 하위 안건: 직계 부모 → 조부모 순으로 content_map 탐색 (3단계 대응)
    if '-' in num_clean:
        parts = num_clean.split('-')
        for depth in range(len(parts) - 1, 0, -1):
            ancestor = '-'.join(parts[:depth])
            if ancestor in content_map:
                parent_content = content_map[ancestor]
                m = re.search(r'제\s*' + re.escape(num_clean) + r'\s*호', parent_content)
                if m:
                    sub_start = m.start()
                    # 같은 레벨의 다음 형제 안건 위치 탐색
                    sibling_prefix = '-'.join(parts[:depth])
                    next_sub = re.search(
                        r'제\s*' + re.escape(sibling_prefix) + r'-\d+\s*호',
                        parent_content[sub_start + 5:]
                    )
                    if next_sub:
                        return parent_content[sub_start: sub_start + 5 + next_sub.start()].strip()
                    return parent_content[sub_start:].strip()

    return ""


# ─────────────────────────────────────────────
# 5. 정관변경 표 직접 파싱 (HTML → 열별 추출)
# ─────────────────────────────────────────────

def extract_charter_tables_from_html(file_path):
    """
    HTML에서 정관변경 표(변경전/변경후/목적 열)를 파싱한다.

    헤더 지원:
    - "변경전/변경후" 또는 "현행/개정안" 형태 모두 인식

    핵심: rowspan 처리 (한화갤러리아 패턴 대응)
    - 구분 열이 rowspan>1 merged cell인 경우 논리적 표를 재구성
    - row_span_tracker로 각 열의 남은 rowspan 추적
    - 논리적 행 기준으로 구분값 변화 감지 → 섹션 분리

    섹션 분리:
    - has_merged_구분=True (한화갤러리아 스타일):
        구분값이 변할 때마다 새 섹션 시작 → 각각 _pos_N 키로 저장
    - has_merged_구분=False (효성중공업 스타일):
        단일 섹션 → 표 직전 안건번호 키 또는 _pos_N

    빈 표 필터:
    - 변경전/변경후가 모두 대시(—) 또는 "해당사항 없음"인 섹션은 제외
    """
    def is_trivial(texts):
        combined = " ".join(texts)
        stripped = re.sub(r'[-─—\s]+', '', combined)
        return len(stripped) < 5 or '해당사항없음' in combined.replace(' ', '')

    def build_logical_rows(rows, header_row_idx, max_col_idx):
        """rowspan을 반영한 논리적 행 목록을 반환."""
        row_span_tracker = {}  # {col_idx: [remaining_count, value]}
        logical_rows = []

        for row in rows[header_row_idx + 1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            logical_row = {}
            cell_idx = 0

            for col_idx in range(max_col_idx + 1):
                if col_idx in row_span_tracker:
                    remaining, val = row_span_tracker[col_idx]
                    logical_row[col_idx] = val
                    if remaining - 1 > 0:
                        row_span_tracker[col_idx] = [remaining - 1, val]
                    else:
                        del row_span_tracker[col_idx]
                elif cell_idx < len(cells):
                    cell = cells[cell_idx]
                    val = cell.get_text(separator="\n", strip=True)
                    logical_row[col_idx] = val
                    span = int(cell.get("rowspan", 1))
                    if span > 1:
                        row_span_tracker[col_idx] = [span - 1, val]
                    cell_idx += 1
                else:
                    logical_row[col_idx] = ""

            logical_rows.append(logical_row)

        return logical_rows

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, "lxml")

        # 안건번호: "제N호", "제N-M호", "제N-M-L호" 매칭 (의안 유무 무관, 조문번호 제외)
        num_pattern = re.compile(r'제\s*(\d{1,2}(?:-\d+)*)\s*호(?:\s*의\s*안)?')

        result = {}
        pos_counter = 0

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if not rows:
                continue

            # ── 헤더 행 탐색: 변경전/변경후 또는 현행/개정안 ──
            header_row_idx = None
            col_map = {}
            for ri, row in enumerate(rows[:5]):
                cells = row.find_all(["th", "td"])
                headers = [c.get_text(separator=" ", strip=True) for c in cells]
                joined = " ".join(headers)
                has_before = ("변경전" in joined or "변경 전" in joined or "현행" in joined)
                has_after = ("변경후" in joined or "변경 후" in joined
                             or "개정안" in joined or "개정 안" in joined)
                if has_before and has_after:
                    for i, h in enumerate(headers):
                        if ("구분" in h and "변경" not in h and "목적" not in h
                                and "현행" not in h and "개정" not in h):
                            col_map["구분"] = i
                        elif "변경전" in h or "변경 전" in h or (
                                "현행" in h and "변경" not in h
                                and "목적" not in h and "개정" not in h):
                            col_map["변경전 내용"] = i
                        elif ("변경후" in h or "변경 후" in h
                              or "개정안" in h or "개정 안" in h):
                            col_map["변경후 내용"] = i
                        elif "목적" in h:
                            col_map["변경의 목적"] = i
                    header_row_idx = ri
                    break

            if header_row_idx is None or "변경전 내용" not in col_map:
                continue

            # ── 표 직전 역방향 안건번호 탐색 ──
            agenda_num_for_table = None
            division_from_heading = ""
            for prev in table.find_all_previous(True):
                if prev.find_parent("table"):
                    continue
                if prev.name == "table":
                    break
                if prev.name in ["script", "style", "head"]:
                    continue
                text = prev.get_text(separator=" ", strip=True)
                if text:
                    m = num_pattern.search(text)
                    if m:
                        num_str = m.group(1).replace(" ", "")
                        base_num = int(re.split(r'-', num_str)[0])
                        if base_num <= 20:
                            agenda_num_for_table = num_str
                            division_from_heading = text[:500]
                            break

            # ── 열 인덱스 ──
            구분_idx = col_map.get("구분", -1)
            bf_idx   = col_map.get("변경전 내용", -1)
            af_idx   = col_map.get("변경후 내용", -1)
            목적_idx  = col_map.get("변경의 목적", -1)
            has_구분_col = 구분_idx >= 0
            max_col_idx = max(구분_idx, bf_idx, af_idx, 목적_idx)

            # ── 구분 열이 rowspan merged cell인지 확인 ──
            has_merged_구분 = False
            if has_구분_col:
                data_rows = [r for r in rows[header_row_idx + 1:]
                             if r.find_all(["th", "td"])]
                if data_rows:
                    first_cells = data_rows[0].find_all(["th", "td"])
                    if 구분_idx < len(first_cells):
                        span_val = int(first_cells[구분_idx].get("rowspan", 1))
                        if span_val > 1:
                            has_merged_구분 = True

            # ── 논리적 행 빌드 (rowspan 반영) ──
            logical_rows = build_logical_rows(rows, header_row_idx, max_col_idx)

            # ── 섹션 분리 ──
            sections = []

            if has_구분_col and has_merged_구분:
                # 한화갤러리아 스타일: 구분값 변화로 섹션 경계 감지
                current_label = ""
                current_data = {"변경전 내용": [], "변경후 내용": [], "변경의 목적": []}

                for lrow in logical_rows:
                    구분_val = lrow.get(구분_idx, "").strip()
                    bf_val   = lrow.get(bf_idx, "").strip() if bf_idx >= 0 else ""
                    af_val   = lrow.get(af_idx, "").strip() if af_idx >= 0 else ""
                    목적_val  = lrow.get(목적_idx, "").strip() if 목적_idx >= 0 else ""

                    # 구분값이 바뀌면 새 섹션 시작
                    if 구분_val and 구분_val != current_label:
                        if any(current_data[k] for k in current_data):
                            sections.append({"label": current_label,
                                             "data": current_data})
                        current_label = 구분_val
                        current_data = {"변경전 내용": [], "변경후 내용": [],
                                        "변경의 목적": []}

                    if bf_val:
                        current_data["변경전 내용"].append(bf_val)
                    if af_val:
                        current_data["변경후 내용"].append(af_val)
                    if 목적_val:
                        current_data["변경의 목적"].append(목적_val)

                if any(current_data[k] for k in current_data):
                    sections.append({"label": current_label, "data": current_data})

            else:
                # 효성중공업 스타일: rowspan 없음, 단일 섹션으로 수집
                current_data = {"변경전 내용": [], "변경후 내용": [], "변경의 목적": []}

                for lrow in logical_rows:
                    bf_val  = lrow.get(bf_idx, "").strip() if bf_idx >= 0 else ""
                    af_val  = lrow.get(af_idx, "").strip() if af_idx >= 0 else ""
                    목적_val = lrow.get(목적_idx, "").strip() if 목적_idx >= 0 else ""

                    if bf_val:
                        current_data["변경전 내용"].append(bf_val)
                    if af_val:
                        current_data["변경후 내용"].append(af_val)
                    if 목적_val:
                        current_data["변경의 목적"].append(목적_val)

                if any(current_data[k] for k in current_data):
                    sections.append({"label": division_from_heading,
                                     "data": current_data})

            if not sections:
                continue

            def _extract_num_from_label(label, pattern=num_pattern):
                """구분 레이블 텍스트에서 안건번호(예: '2-1', '2')를 추출한다."""
                m = pattern.search(label)
                if m:
                    ns = m.group(1).replace(" ", "")
                    base = int(re.split(r'-', ns)[0])
                    if base <= 20:
                        return ns
                return None

            # ── 섹션 저장 ──
            if has_merged_구분 or len(sections) > 1:
                # 복수 섹션 (한화갤러리아/롯데지주 패턴)
                # → 구분 레이블에서 안건번호 추출 가능하면 그걸 키로, 아니면 _pos_N
                for sec in sections:
                    if (is_trivial(sec["data"].get("변경전 내용", []))
                            and is_trivial(sec["data"].get("변경후 내용", []))):
                        continue
                    entry = {
                        "구분": sec["label"],
                        "변경전 내용": "\n".join(sec["data"].get("변경전 내용", [])),
                        "변경후 내용": "\n".join(sec["data"].get("변경후 내용", [])),
                        "변경의 목적": "\n".join(sec["data"].get("변경의 목적", [])),
                    }
                    extracted = _extract_num_from_label(sec["label"])
                    if extracted and extracted not in result:
                        result[extracted] = entry
                    else:
                        result[f"_pos_{pos_counter}"] = entry
                        pos_counter += 1
            else:
                # 단일 섹션 (효성중공업/롯데지주 가-표 패턴)
                # → 구분 셀 또는 역방향 스캔 번호를 키로 사용
                sec = sections[0]
                if (is_trivial(sec["data"].get("변경전 내용", []))
                        and is_trivial(sec["data"].get("변경후 내용", []))):
                    continue

                # 단일 섹션에서 구분_idx가 있다면 실제 구분 셀 값 수집
                구분_label_from_cell = ""
                if has_구분_col:
                    for lrow in logical_rows:
                        gv = lrow.get(구분_idx, "").strip()
                        if gv:
                            구분_label_from_cell = gv
                            break

                entry = {
                    "구분": 구분_label_from_cell or division_from_heading,
                    "변경전 내용": "\n".join(sec["data"].get("변경전 내용", [])),
                    "변경후 내용": "\n".join(sec["data"].get("변경후 내용", [])),
                    "변경의 목적": "\n".join(sec["data"].get("변경의 목적", [])),
                }
                # 키 우선순위: ① 구분 셀에서 추출한 번호 ② 역방향 스캔 번호 ③ _pos_N
                key = (_extract_num_from_label(구분_label_from_cell)
                       or agenda_num_for_table)
                if key and key not in result:
                    result[key] = entry
                else:
                    result[f"_pos_{pos_counter}"] = entry
                    pos_counter += 1

        return result

    except Exception as e:
        print(f"정관변경 표 파싱 오류: {e}")
        import traceback
        traceback.print_exc()
        return {}


def classify_charter_category(before_text, after_text, purpose_text, agenda_title):
    """
    변경전/변경후/변경의 목적 텍스트를 바탕으로 안건분류2를 AI로 판단한다.
    반환: str (예: "이사 임기 유연화" 또는 "이사 정원 축소, 자사주 보유")
    """
    if not bedrock_client:
        return ""

    # 변경전/변경후 내용이 둘 다 비어있으면 AI 호출 불필요
    if not before_text.strip() and not after_text.strip():
        return ""

    combined = (
        f"[안건제목]\n{agenda_title}\n\n"
        f"[변경전 내용 전체 (표의 모든 행 포함)]\n{before_text}\n\n"
        f"[변경후 내용 전체 (표의 모든 행 포함)]\n{after_text}\n\n"
        f"[변경의 목적 전체]\n{purpose_text}"
    )

    prompt = f"""다음은 정관변경 안건의 변경 내용입니다. 표에 여러 행(조항)이 있을 수 있으며, 각 행을 모두 검토하세요.

{combined[:10000]}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[분류 지침]

아래 4가지 카테고리를 각각 독립적으로 체크하세요. 해당하면 포함, 아니면 제외.
"기타"는 4가지 중 하나도 해당하지 않을 때만 사용합니다.

허용 값(이 값만 사용, 다른 표현 금지):
  "이사 임기 유연화" | "이사 임기 연장" | "이사 정원 축소" | "자사주 보유" | "기타"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[카테고리별 판단 기준 및 예시]

① 이사 임기 유연화
   정의: 기존에 단일하게 고정된 임기가, 상한만 두고 그 이하의 기간도 모두 허용되는 구조로 바뀐 경우.
         즉, 변경 후에 임기를 1년, 2년, 3년 등 상한 이하의 다양한 기간으로 정할 수 있게 되어야 함.

   ✅ 해당하는 핵심 표현: "N년을 초과하지 못한다" / "N년을 초과하지 않는다" / "N년을 초과할 수 없다"
      → 이런 표현은 N년 이하면 1년이든 2년이든 모두 가능하다는 의미 = 유연화

   ✅ 해당 예시:
     - "이사의 임기는 3년으로 한다" → "이사의 임기는 3년을 초과하지 못한다"
       (3년 고정 → 1·2·3년 모두 가능 → 유연화 O. 숫자 동일이므로 이사 임기 연장은 아님)
     - "이사의 임기는 2년으로 한다" → "이사의 임기는 3년을 초과하지 않는 범위 내에서 주주총회에서 결정한다"
       (2년 고정 → 1·2·3년 모두 가능 + 숫자 증가 → 유연화 O, 이사 임기 연장 O)

   ❌ 비해당 — 유연화처럼 보여도 아닌 경우:
     - "취임 후 2년 내에 종료하는 정기주주총회 종결시까지" → "취임 후 3년 내에 종료하는..."
       (2년 고정 → 3년 고정. 임기 기준 연수가 2→3으로 증가했을 뿐, 다양한 기간 선택 불가 → 이사 임기 연장만)
     - "이사의 임기는 취임 후 2년 내의 최종 결산기 정기주주총회 종결시까지" → "이사의 임기는 3년 이내로 한다"
       (2년 고정 → 3년 고정. "이내로 한다"는 사실상 3년으로 고정된 표현이며 다양한 기간 선택 불가 → 이사 임기 연장만)
     - "N년 이내로 한다"는 표현은 일반적으로 N년으로 고정된 것과 동일하게 취급 → 유연화 아님
     - 감사위원회 조항에 이사 관련 내용이 있어도 → 이사 임기 유연화 아님
     - 이사회 의장 선임 방식 변경 → 이사 임기 유연화 아님
     - 부칙의 이사 임기 경과조치 → 이사 임기 유연화 아님

② 이사 임기 연장
   정의: 이사 임기의 연도 수(숫자) 자체가 늘어난 경우.
   핵심: 임기의 기준 연수 증가 (1년→2년, 2년→3년 등). 유연화와 동시에 해당할 수 있음.
   ✅ 해당 예시:
     - "이사의 임기는 2년으로 한다" → "이사의 임기는 3년으로 한다" (2→3)
     - "취임 후 2년 내에 종료하는..." → "취임 후 3년 내에 종료하는..." (2→3)
     - "취임 후 2년 내의 최종 결산기 정기주주총회 종결시까지" → "3년 이내로 한다" (2→3)
   ❌ 비해당 예시:
     - "이사의 임기는 3년으로 한다" → "이사의 임기는 3년을 초과하지 못한다" (3→3, 숫자 동일 → 연장 아님)
     - 부칙의 시행일 날짜(연도) 변경: 이사 임기와 무관
     - 부칙 경과조치의 이사 임기 언급: 임기 자체를 변경하는 것이 아님

③ 이사 정원 축소
   정의: 이사 정원의 최대 인원 수(상한)가 실제로 줄어든 경우.
   핵심: 이사 수 상한 숫자의 실질적 감소 (13명→7명, 16명→9명 등)
   ✅ 해당 예시:
     - "이사는 3명 이상 13명 이내" → "이사는 3명 이상 7명 이내" (13→7)
     - "이사는 3명 이상 16명 이내" → "이사는 3명 이상 9인 이하" (16→9)
   ❌ 비해당 — 아래는 이사 정원 축소 아님:
     - 사외이사/독립이사 비율(구성 비율) 변경: 예) "사외이사 이사총수의 1/4 이상" → "독립이사 이사총수의 1/3 이상"
       (인원 수 상한 변화 없음, 비율만 바뀜 → 이사 정원 축소 아님)
     - 사외이사 → 독립이사 명칭만 변경: 인원 수 변화 없음
     - 감사위원회 위원 구성·선임 방식 변경 (감사위원 수나 비율 변경 포함)
     - 이사 수 상한이 그대로이거나 늘어난 경우

④ 자사주 보유
   정의: 자기주식(자사주)의 보유 또는 처분에 관한 조항이 신설되거나 변경된 경우.
   핵심: "자기주식" 또는 "자사주" + 보유·처분 관련 조항. 소각은 해당 안 됨.
   ✅ 해당 예시:
     - 제9조의2(자기주식의 보유 또는 처분) 조항 신설
     - "회사는 ...자기주식을 보유·처분할 수 있다" 조항 추가
   ❌ 비해당 예시:
     - "주식의 소각" 조항 신설·변경·삭제 (소각은 보유·처분과 다름)
     - 의결권 대리행사 방법 변경 (서면 → 서면+전자문서 등)
     - 전자주주총회 개최 관련 조항
     - 정관 조문 번호 재정비 과정에서 자기주식 관련 조항 번호만 바뀐 경우

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[반드시 "기타"인 경우 — 아무리 이사·주식 관련처럼 보여도 아래는 전부 기타]

※ 아래 유형은 이사 임기, 이사 정원, 자사주와 관련된 단어가 포함되어 있어도 반드시 "기타":

  ▸ 집중투표제 배제 조항 삭제 또는 신설
  ▸ 사외이사 → 독립이사 명칭 변경이 주된 내용인 조항
      (예: "사외이사후보추천위원회" → "독립이사후보추천위원회", 감사위원회 구성 명칭 변경 등)
  ▸ 전자주주총회 도입 (원격 참석 방식 추가)
  ▸ 의결권 대리행사 방법 변경 (서면 → 서면+전자문서 등)
  ▸ 감사위원회 구성·선임 방식 변경 (분리선임 인원 변경, 의결권 제한 기준 변경, 위원 비율 변경 등)
  ▸ 이사회 의장 선임 방식 변경
  ▸ 부칙 신설 또는 변경 전반 (시행일 변경, 경과조치 신설 등 포함)
      — 부칙에 "이사의 임기" 관련 경과조치가 있어도 반드시 기타
      — 예: "종전 정관에 따라 선임된 이사의 임기는 종전 규정에 따른다" → 기타
  ▸ 정관 조문 번호 재정비 (예: 조항 번호 체계 수정, 제N조의1 → 제N조의2 등)
  ▸ 주식 소각 조항 신설·변경·삭제
  ▸ 공고방법 변경, 주주명부 폐쇄 기준일 변경, 서면결의 방식 변경 등 기타 상법 개정 반영

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[실제 판단 예시 9가지]

예시1) 자기주식 보유 신설
  변경전: <신설>
  변경후: 제9조의2(자기주식의 보유 또는 처분) 이 회사는 ...자기주식을 보유·처분할 수 있다.
  → {{"category2": "자사주 보유"}}

예시2) 이사 정원 축소 + 임기 연장
  변경전: 이사는 3명 이상 13명 이내 / 이사의 임기는 2년으로 한다
  변경후: 이사는 3명 이상 7명 이내 / 이사의 임기는 3년으로 한다
  → {{"category2": "이사 정원 축소, 이사 임기 연장"}}

예시3) 이사 정원 축소 + 임기 연장 + 유연화
  변경전: 이사는 3명 이상 16명 이내 / 취임 후 2년 내에 종료하는 정기주주총회 종결시까지
  변경후: 이사는 3명 이상 9인 이하 / 3년을 초과하지 않는 범위 내에서 주주총회에서 결정한다
  → {{"category2": "이사 정원 축소, 이사 임기 연장, 이사 임기 유연화"}}

예시4) 임기 유연화만 — "초과하지 못한다" 표현, 숫자 동일
  변경전: 이사 및 사외이사의 임기는 3년으로 한다
  변경후: 이사의 임기는 3년을 초과하지 못한다
  → {{"category2": "이사 임기 유연화"}}
  (※ 3→3 숫자 동일이므로 이사 임기 연장 아님. "초과하지 못한다" = 1·2·3년 모두 가능 → 유연화)

예시5) 이사 정원 축소 + 임기 연장만 — "이내로 한다"는 유연화 아님
  변경전: 이사는 3명 이상 13명 이내 / 이사의 임기는 취임 후 2년 내의 최종 결산기 정기주주총회 종결시까지
  변경후: 이사는 3명 이상 7명 이내 / 이사의 임기는 3년 이내로 한다
  → {{"category2": "이사 정원 축소, 이사 임기 연장"}}
  (※ "3년 이내로 한다"는 3년 고정과 동일. 다양한 기간 선택 불가 → 이사 임기 유연화 아님)

예시5-2) 사외이사/독립이사 비율 변경 → 이사 정원 축소 아님
  변경전: 이사는 3명 이상으로 하고, 사외이사는 이사총수의 4분의 1 이상으로 한다
  변경후: 이사는 3명 이상으로 하고, 독립이사는 이사총수의 3분의 1 이상으로 한다
  → {{"category2": "기타"}}
  (※ 인원 수 상한 변화 없음. 비율·명칭만 변경 → 이사 정원 축소 아님, 기타)

예시6) 집중투표제 배제 조항 삭제 → 기타
  변경전: 이 회사는 이사 선임 시 집중투표제를 적용하지 아니한다
  변경후: <삭제>
  → {{"category2": "기타"}}

예시7) 사외이사→독립이사 명칭 변경 + 감사위원회 구성 변경 → 기타
  변경전: 감사위원회 위원의 3분의 2 이상은 사외이사이어야 한다
  변경후: 감사위원회 위원의 3분의 2 이상은 독립이사이어야 한다
  → {{"category2": "기타"}}

예시8) 부칙 신설 (시행일 + 경과조치, 이사 임기 언급 포함) → 기타
  변경전: <신설>
  변경후: 부칙 제1조(시행일) 이 정관은 주주총회 승인일부터 시행한다. 제2조(이사의 임기 경과조치) 종전 정관에 따라 선임된 이사의 임기는 종전 규정에 따른다.
  → {{"category2": "기타"}}

예시9) 정관 조문 번호 재정비 → 기타
  변경전: 제8조의1(제1종 종류주식) 제10조의1(시가발행) 제14조의1(소집권자)
  변경후: 제8조의2(제1종 종류주식) 제10조의2(시가발행) 제14조의2(소집권자)  (조번호만 바뀌고 내용 동일)
  → {{"category2": "기타"}}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[출력 규칙]
- 표에 여러 행(조항)이 있으면 각 행마다 위 기준을 적용하고 해당 항목 모두 포함.
- 해당 카테고리가 2개 이상이면 쉼표로 구분: "이사 정원 축소, 이사 임기 연장"
- 4가지 중 하나도 해당 없으면: "기타"
- "기타"는 다른 카테고리와 함께 쓰지 않음.
- 허용 값 외의 표현(예: "임기 상한 설정", "이사 임기 제한" 등) 절대 사용 금지.

결과를 JSON으로만 반환. 다른 텍스트 없이 JSON만.
반환 형식: {{"category2": "..."}}"""

    ALLOWED = {"이사 임기 유연화", "이사 임기 연장", "이사 정원 축소", "자사주 보유", "기타"}

    try:
        # 출력은 {"category2": "..."} 수준이므로 max_tokens=100으로 충분
        raw_text = call_bedrock(prompt, max_tokens=100)
        result = extract_json(raw_text)
        if not result:
            return "기타"

        raw_cat = str(result.get("category2", "기타")).strip()

        # 쉼표 구분 리스트에서 허용 값만 남김
        parts = [p.strip() for p in raw_cat.split(",")]
        valid_parts = [p for p in parts if p in ALLOWED and p != "기타"]
        if not valid_parts:
            return "기타"
        return ", ".join(valid_parts)

    except Exception as e:
        print(f"안건분류2 분류 오류: {e}")
        return ""


# ─────────────────────────────────────────────
# 6. 안건번호 유틸
# ─────────────────────────────────────────────

def format_agenda_num(num_str):
    """안건번호를 "제N호 의안" 형식으로 변환. 예: "2-1" → "제2-1호 의안" """
    num_str = str(num_str).strip()
    if not num_str:
        return ""
    return f"제{num_str}호 의안"


def agenda_sort_key(num_str):
    """안건번호 정렬키: "1" → (1, 0), "2-1" → (2, 1), "2-2" → (2, 2)"""
    num_str = str(num_str).strip()
    parts = re.split(r'[-.]', num_str)
    try:
        return tuple(int(p) for p in parts)
    except Exception:
        return (999,)


# ─────────────────────────────────────────────
# 6. 메인
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
        columns = ["회사명", "시장분류", "공고일", "주총일", "안건번호", "안건 제목",
                   "주주제안여부", "주주제안자", "안건분류1", "안건분류2",
                   "[정관] 구분", "[정관] 변경전 내용", "[정관] 변경후 내용",
                   "[정관] 변경의 목적"]
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
            market_label = "유가증권"
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

            # ── 임시주주총회 제외: 소집공고 앞부분에 "임시주주총회" 표현 시 스킵 ──
            check_area = notice_text[:3000] if notice_text else full_text[:3000]
            if "임시주주총회" in check_area:
                fail_list.append([corp_name, stock_code, rcept_no, "임시주주총회_제외"])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            # AI로 D/E열 파싱 + G/H/I/J열 분석 (보고서 1건당 1회 호출)
            # notice_text가 짧으면 full_text를 fallback으로 전달
            parsed = parse_and_analyze_with_ai(notice_text, corp_name,
                                               full_text_fallback=full_text)
            meeting_date = parsed.get("meeting_date", "")
            agenda_items = parsed.get("agenda_items", [])
            ai_error = parsed.get("error", "")

            if not agenda_items:
                reason = ai_error if ai_error else "ai_returned_no_agenda_items"
                fail_list.append([corp_name, stock_code, rcept_no, reason])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            # ── 상위 안건 제거: 하위 안건이 있으면 모든 상위 레벨(조상) 제외 ──
            # 예: "2-1-1" 존재 → "2"와 "2-1" 모두 제거
            #     "2-1"   존재 → "2" 제거
            nums_with_subs = set()
            for item in agenda_items:
                num = str(item.get("num", "")).strip()
                parts = num.split('-')
                # 2단계(2-1): 조상 "2" 추가 / 3단계(2-1-1): 조상 "2", "2-1" 추가
                for depth in range(1, len(parts)):
                    nums_with_subs.add('-'.join(parts[:depth]))
            agenda_items = [
                item for item in agenda_items
                if str(item.get("num", "")).strip() not in nums_with_subs
            ]

            # ── 안건번호 순 정렬 ──
            agenda_items.sort(key=lambda x: agenda_sort_key(str(x.get("num", ""))))

            # F열용 section2 내용 맵 구성
            content_map = build_section2_content_map(section2_text)

            # 정관변경 안건이 있으면 HTML에서 표를 미리 파싱 (루프 밖에서 1회)
            has_charter = any(str(i.get("category1", "")) == "정관변경" for i in agenda_items)
            charter_tables_by_num = extract_charter_tables_from_html(main_file) if has_charter else {}

            for item in agenda_items:
                agenda_num = str(item.get("num", ""))
                agenda_title = str(item.get("title", ""))
                category1 = str(item.get("category1", ""))
                category2 = str(item.get("category2", ""))

                # ── 정관변경 안건: 안건번호로 표 매핑 ──
                charter_division = ""
                before_content = ""
                after_content = ""
                purpose = ""

                if category1 == "정관변경":
                    num_clean = agenda_num.replace(" ", "")
                    tbl = charter_tables_by_num.get(num_clean, {})

                    # 이미 사용된 키 목록 (Fallback 1/2 공통으로 사용)
                    used_keys = {
                        r.get("_charter_key", "") for r in all_rows
                        if r.get("회사명") == corp_name and r.get("공고일") == rcept_dt
                    }

                    # Fallback 1: 직계 부모 → 조부모 순으로 시도 (아직 미사용인 경우만)
                    if not tbl and "-" in num_clean:
                        parts = num_clean.split("-")
                        # 직계 부모부터 거슬러 올라가며 시도 (2-1-1 → 2-1 → 2)
                        for depth in range(len(parts) - 1, 0, -1):
                            ancestor = "-".join(parts[:depth])
                            candidate = charter_tables_by_num.get(ancestor, {})
                            if candidate and ancestor not in used_keys:
                                tbl = candidate
                                num_clean = ancestor
                                break

                    # Fallback 2: 미매핑 표(_pos_N 포함) 중 첫 번째 사용
                    if not tbl:
                        for k, v in charter_tables_by_num.items():
                            if k not in used_keys:
                                tbl = v
                                num_clean = k
                                break

                    charter_division = tbl.get("구분", "")
                    before_content = tbl.get("변경전 내용", "")
                    after_content = tbl.get("변경후 내용", "")
                    purpose = tbl.get("변경의 목적", "")

                    # AI로 category2만 분류 (변경전/변경후 내용이 둘 다 비어있으면 skip)
                    if before_content.strip() or after_content.strip():
                        category2 = classify_charter_category(
                            before_content, after_content, purpose, agenda_title
                        )
                    else:
                        category2 = ""

                all_rows.append({
                    "회사명": corp_name,
                    "시장분류": market_label,
                    "공고일": rcept_dt,
                    "주총일": meeting_date,
                    "안건번호": format_agenda_num(agenda_num),
                    "안건 제목": agenda_title,
                    "주주제안여부": str(item.get("shareholder_proposal", "")),
                    "주주제안자": str(item.get("proposer", "")),
                    "안건분류1": category1,
                    "안건분류2": category2,
                    "[정관] 구분": charter_division,
                    "[정관] 변경전 내용": before_content,
                    "[정관] 변경후 내용": after_content,
                    "[정관] 변경의 목적": purpose,
                    "_charter_key": num_clean if category1 == "정관변경" else "",
                })

        except Exception as e:
            fail_list.append([corp_name, stock_code, rcept_no, f"extract_error: {str(e)}"])

        finally:
            shutil.rmtree(folder_name, ignore_errors=True)

        time.sleep(0.08)

    # 내부 추적용 _charter_key 열 제거
    for row in all_rows:
        row.pop("_charter_key", None)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    write_progress(progress_file, "done", 100, "엑셀 생성 완료", total_reports, total_reports)


if __name__ == "__main__":
    main()
