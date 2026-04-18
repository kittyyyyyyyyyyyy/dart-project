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
BEDROCK_MODEL_ID_HAIKU35 = "us.anthropic.claude-3-5-haiku-20241022-v1:0"

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


def call_bedrock(prompt, max_tokens=4096, model_id=None):
    """AWS Bedrock Claude 호출. JSON 문자열 반환.
    model_id 미지정 시 기본 모델(Haiku 3) 사용.
    """
    if not bedrock_client:
        raise RuntimeError("bedrock_client가 None입니다. IAM 권한을 확인하세요.")

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}]
    }

    response = bedrock_client.invoke_model(
        modelId=model_id or BEDROCK_MODEL_ID,
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
    # UTF-8 우선 → EUC-KR(cp949) fallback (DART HTML은 보통 UTF-8이나 일부 EUC-KR)
    with open(file_path, "rb") as f:
        _raw = f.read()
    for _enc in ("utf-8", "euc-kr", "cp949"):
        try:
            content = _raw.decode(_enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        content = _raw.decode("utf-8", errors="ignore")

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

    # "부의안건" 또는 "목적사항" 위치 탐색
    # ★ 정정공고는 비교표 안에도 같은 키워드가 등장하므로 반드시 마지막(가장 뒤) 위치를 사용
    # ★ 호텔신라 등: "부의안건" 대신 "3. 회의 목적사항" 형태 사용
    # ★ "목적사항" 단독 패턴은 "이사회 의안에 대한 목적사항" 등 section I 내용을 잘못 잡을 수 있어
    #    최후 fallback은 "결의\s*사\s*항"으로 한정한다
    agenda_section_pats = [
        r'나\s*[\.．]\s*부\s*의\s*안\s*건',
        r'나\s*\.\s*부의안건',
        r'부\s*의\s*안\s*건',
        r'[2-7]\s*[\.．]\s*[가-힣\s]{0,10}목\s*적\s*사\s*항',  # "3. 회의 목적사항" 등 포함
        r'결\s*의\s*사\s*항',  # 최후 fallback: "결의사항"으로 한정 (section I 오탐 방지)
    ]
    agenda_pos = -1
    for pat in agenda_section_pats:
        matches = list(_re.finditer(pat, full_text))
        if matches:
            agenda_pos = matches[-1].start()  # 마지막 위치 사용
            break

    if agenda_pos != -1:
        # 부의안건/결의사항 앞에서 소집공고 시작점을 역방향으로 탐색 (최대 12000자)
        # 탐색 창을 더 넓게 잡고, 패턴 중 가장 늦은 위치의 매치를 선택
        search_window_start = max(0, agenda_pos - 12000)
        search_window = full_text[search_window_start: agenda_pos]
        back_patterns = [
            r'주\s*주\s*총\s*회\s*소\s*집\s*공\s*고',
            r'소\s*집\s*공\s*고',
            r'\(제\d+기\s*정기\)',
            r'가\s*\.\s*주\s*주\s*총\s*회\s*개\s*요',  # "가. 주주총회 개요" 바로 앞
            r'[1１]\s*\.\s*일\s*시',
            r'[1１]\s*\.\s*일\s*[\s]*자',
        ]
        # 각 패턴의 마지막 매치 위치 중 가장 큰 것(가장 늦은 것)을 사용
        # → section I 내 "소집공고" 언급이 있어도 실제 공고문 시작이 더 뒤에 있으면 그걸 사용
        best_pos_in_window = -1
        for bp in back_patterns:
            for bm in _re.finditer(bp, search_window):
                if bm.start() > best_pos_in_window:
                    best_pos_in_window = bm.start()
        if best_pos_in_window != -1:
            notice_start = search_window_start + best_pos_in_window
        else:
            notice_start = max(0, agenda_pos - 500)
    else:
        # ★ 넓은 범위 접근법: 모든 패턴 실패 → 문서 맨 앞부터 사용
        notice_start = 0

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
        # ★ 넓은 범위 접근법: section2가 없으면 notice_text를 더 길게 (최대 25000자)
        notice_text = full_text[notice_start:notice_start + 25000]
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
        text_to_use = full_text_fallback[:25000]

    # "부의안건" 또는 "목적사항" 위치를 찾아 그 앞 500자 + 이후 15000자를 우선 사용
    # ★ 정정공고는 마지막(가장 뒤) 위치 사용
    # ★ "회의 목적사항" 등 다양한 형태 포함
    import re as _re2
    agenda_section_pat = _re2.compile(
        r'나\s*[\.．]\s*부의안건'
        r'|나\.\s*부\s*의\s*안\s*건'
        r'|부의\s*안건'
        r'|[2-7]\s*[\.．]\s*[가-힣\s]{0,10}목\s*적\s*사\s*항'
        r'|결\s*의\s*사\s*항'   # "목적사항" 단독 대신 "결의사항"으로 한정
    )
    all_agenda_matches = list(agenda_section_pat.finditer(text_to_use))
    m = all_agenda_matches[-1] if all_agenda_matches else None

    if m and m.start() > 500:
        slice_start = max(0, m.start() - 500)
        text_excerpt = text_to_use[slice_start: slice_start + 15000]
    elif not m and full_text_fallback and text_to_use != full_text_fallback[:25000]:
        # ★ 넓은 범위 접근법: text_to_use에서도 못 찾으면 full_text로 재시도
        all_agenda_matches2 = list(agenda_section_pat.finditer(full_text_fallback))
        m2 = all_agenda_matches2[-1] if all_agenda_matches2 else None
        if m2:
            slice_start = max(0, m2.start() - 500)
            text_excerpt = full_text_fallback[slice_start: slice_start + 15000]
        else:
            text_excerpt = full_text_fallback[:25000]
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
   - ★ 주의: "I. 사외이사 등의 활동내역" 섹션에 있는 "이사회 의안에 대한 찬반여부" 표는
     이사회 내부 결의 목록이지 주주총회 안건이 아님 → 절대 추출하지 말 것
   - ★ 주의: 후보자 정보표(선임 후보자 약력/직업 테이블)도 안건 목록이 아님 → 제외
   - 안건에 하위 안건이 있는 경우, 반드시 최하위 안건만 추출:
       * 2단계: 제2호 아래 제2-1호, 제2-2호가 있으면 → 제2-1호, 제2-2호만 추출 (제2호 제외)
       * 3단계: 제2-1호 아래 제2-1-1호, 제2-1-2호가 있으면 → 제2-1-1호, 제2-1-2호만 추출 (제2호, 제2-1호 모두 제외)
       * 즉, 하위 안건이 있는 상위 안건은 절대 포함하지 않음
   - 하위 안건이 없는 단독 안건(제1호, 제4호 등)은 그대로 포함
   - 원문 표현을 최대한 그대로 유지
   - ★ 중요: "가결될 경우에만 상정", "자동 폐기", "조건부" 등의 문구가 있는 안건도
     반드시 포함하여 추출할 것 (조건 여부와 무관하게 안건 목록에 기재된 것은 모두 추출)
   - ★ 중요: 주주제안 안건의 하위 항목도 일반 안건과 동일하게 각각 별도 항목으로 추출할 것
   - ★ 중요: "(철회)", "(철 회)" 등 철회 표시가 있는 안건도 반드시 목록에 포함할 것. 안건 제목에 "(철회)" 문구를 그대로 포함하여 추출

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
# 5-A. 이사 선임 후보자 정보 추출 (HTML 테이블 파싱)
# ─────────────────────────────────────────────

def normalize_separate_election(text):
    """감사위원회 위원인 이사 분리선출 여부 → Y / N"""
    t = re.sub(r'[\s\u3000\xa0]+', '', str(text))
    if not t or t in ['-', '—', '─', '–', '×', '해당없음']:
        return 'N'
    # 부정어를 먼저 체크 ("분리선출아님" 같은 복합 표현 대응)
    negative_kws = ['아님', '미해당', '분리선출아님', '해당없음', '비해당', '해당사항없음', '×', '않음', '불해당', '없음']
    positive_kws = ['분리선출', '○', '예(Y)', '예', 'Y', '해당']
    for kw in negative_kws:
        if kw in t:
            return 'N'
    for kw in positive_kws:
        if kw in t:
            return 'Y'
    return text.strip()


def _build_logical_table(table_tag):
    """
    HTML table의 colspan/rowspan을 처리하여 논리적 2D 그리드를 반환한다.
    반환: list[list[str]] — 각 원소는 셀 텍스트
    """
    rows = table_tag.find_all('tr')
    grid = {}  # (row_idx, col_idx) -> text

    for row_idx, row in enumerate(rows):
        col_idx = 0
        for cell in row.find_all(['th', 'td']):
            # rowspan으로 이미 채워진 칸 건너뜀
            while (row_idx, col_idx) in grid:
                col_idx += 1

            text = re.sub(r'\s+', ' ', cell.get_text()).strip()
            cs = int(cell.get('colspan', 1))
            rs = int(cell.get('rowspan', 1))

            for r in range(rs):
                for c in range(cs):
                    if (row_idx + r, col_idx + c) not in grid:
                        grid[(row_idx + r, col_idx + c)] = text

            col_idx += cs

    if not grid:
        return []

    max_row = max(r for r, _ in grid)
    max_col = max(c for _, c in grid)

    return [
        [grid.get((r, c), '') for c in range(max_col + 1)]
        for r in range(max_row + 1)
    ]


def _grid_to_text(grid):
    """논리적 그리드를 읽기 쉬운 텍스트 형식으로 변환 (AI 입력용)"""
    return '\n'.join(' | '.join(row) for row in grid)


def _extract_candidates_with_ai(table_text):
    """
    AI(Bedrock)를 사용해 후보자 정보 테이블 텍스트에서 구조화된 데이터를 추출.
    HTML 테이블 구조가 복잡하거나 열 감지 실패 시 fallback으로 사용.
    """
    prompt = f"""다음은 주주총회 이사 선임 후보자 정보 테이블입니다. 각 후보자의 정보를 추출해주세요.

테이블:
{table_text}

각 후보자(데이터 행)에 대해 다음 정보를 추출하세요:
- 성명: 후보자 이름 (한국어 이름)
- 사외이사여부: "사외이사", "사내이사", "기타비상무이사" 중 해당하는 것 (또는 테이블에 기재된 값 그대로)
- 분리선출여부: 감사위원회 위원인 이사 분리선출 해당이면 "Y", 해당없으면 "N"
  (○, 해당, 분리선출 → "Y" / ×, -, 해당없음, 아님, 비해당 → "N")
- 주된직업: 현재 주된 직업 또는 직위

반환 형식 (JSON only, 다른 설명 없음):
{{
  "이름": {{
    "성명": "...",
    "사외이사여부": "...",
    "분리선출여부": "Y 또는 N",
    "주된직업": "..."
  }}
}}

주의사항:
- 헤더 행(성명, 구분, 여부 등 열 제목이 있는 행)은 제외
- 실제 후보자 데이터 행만 포함
- 이름이 없거나 빈 행은 건너뜀"""

    try:
        result = call_bedrock(prompt, max_tokens=2000)
        data = extract_json(result)
        if not isinstance(data, dict):
            return {}
        candidates = {}
        for name, info in data.items():
            if not isinstance(info, dict):
                continue
            name_key = re.sub(r'\s+', '', name)
            if not name_key or len(name_key) < 2:
                continue
            sep_val = normalize_separate_election(info.get("분리선출여부", "N"))
            candidates[name_key] = {
                "성명": info.get("성명", name),
                "사외이사여부": info.get("사외이사여부", ""),
                "분리선출여부": sep_val,
                "주된직업": info.get("주된직업", ""),
            }
        return candidates
    except Exception as e:
        print(f"  [AI 후보자 추출 실패] {e}")
        return {}


def _detect_candidate_columns(grid):
    """
    논리적 그리드에서 후보자 정보 관련 열 인덱스를 탐지한다.

    핵심 전략:
    1. 각 열에 대해 모든 헤더 행의 텍스트를 누적(concat)하여 합성 헤더 생성
       → 다중 행 헤더("사외이사" + "여부")를 단일 패턴으로 매칭 가능
    2. 광범위한 동의어 패턴으로 매칭
       → 회사마다 다른 열 이름 변형 처리

    반환: (name_col, outside_col, separate_col, occupation_col, data_start_row)
    모두 -1이면 감지 실패
    """
    if not grid:
        return -1, -1, -1, -1, 0

    n_cols = max(len(r) for r in grid)

    # 헤더 행 판별: 주요 헤더 키워드가 있는 행 vs 실제 이름(한글 2~5자)이 첫 셀인 행
    HEADER_KEYWORDS = {'후보자성명', '성명', '사외이사', '분리선출', '직업', '직위', '주된', '후보자'}
    # 헤더 키워드로 쓰이는 단어들 — 첫 셀이 이것이면 이름이 아닌 헤더
    HEADER_CELL_WORDS = {'후보자성명', '성명', '구분', '직위', '직업', '후보자', '이름'}
    DATA_NAME_PAT = re.compile(r'^[가-힣]{2,5}$')

    header_rows = []
    data_start_row = 0
    for row_idx, row in enumerate(grid):
        row_clean = [re.sub(r'[\s\u3000\xa0]+', '', cell) for cell in row]
        first_cell = row_clean[0] if row_clean else ''

        # 행 전체 텍스트에 헤더 키워드가 있으면 헤더 행으로 판별
        combined = ''.join(row_clean)
        is_header = any(kw in combined for kw in HEADER_KEYWORDS)

        if is_header:
            header_rows.append(row_idx)
            data_start_row = row_idx + 1
            continue

        # 첫 셀이 사람 이름처럼 보이면(한글 2~5자이고 헤더 단어가 아닌 경우) 데이터 행 → 스캔 종료
        if DATA_NAME_PAT.match(first_cell) and first_cell not in HEADER_CELL_WORDS:
            break

    if not header_rows:
        # fallback: 첫 행만 헤더로
        header_rows = [0]
        data_start_row = 1

    # 각 열에 대해 헤더 행들의 텍스트를 누적한 합성 헤더 생성
    combined_headers = []
    for col_idx in range(n_cols):
        parts = []
        for row_idx in header_rows:
            if col_idx < len(grid[row_idx]):
                cell_text = re.sub(r'[\s\u3000\xa0(（）)\-·]+', '', grid[row_idx][col_idx])
                if cell_text and cell_text not in parts:
                    parts.append(cell_text)
        combined_headers.append(''.join(parts))

    name_col = outside_col = separate_col = occupation_col = -1

    for col_idx, h in enumerate(combined_headers):
        # 후보자성명
        if name_col == -1 and ('후보자성명' in h or h == '성명'):
            name_col = col_idx
            continue
        # 사외이사 여부 — 광범위한 동의어
        if outside_col == -1 and col_idx != name_col:
            if ('사외이사' in h and ('여부' in h or '후보자' in h)) or h in ('사외이사여부', '사외여부'):
                outside_col = col_idx
                continue
        # 분리선출 여부
        if separate_col == -1 and '분리선출' in h:
            separate_col = col_idx
            continue

    # 주된직업 — 2단계 탐색 (강한 패턴 우선, 약한 패턴 fallback)
    # 1차: "주된직업", "주요직업", "현재직위", "주요경력" 등 명확한 패턴
    for col_idx, h in enumerate(combined_headers):
        if col_idx in (name_col, outside_col, separate_col):
            continue
        if '주된직업' in h or '주요직업' in h or '현재직위' in h or '주요경력' in h:
            occupation_col = col_idx
            break
    # 2차: "직업" 또는 "직위" 단독 (단, 감사/이사 관련 열 제외)
    if occupation_col == -1:
        for col_idx, h in enumerate(combined_headers):
            if col_idx in (name_col, outside_col, separate_col):
                continue
            if '직업' in h or ('직위' in h and '감사' not in h and '이사' not in h):
                occupation_col = col_idx
                break

    return name_col, outside_col, separate_col, occupation_col, data_start_row


def extract_director_candidates_from_html(file_path):
    """
    HTML에서 이사 선임 후보자 정보 테이블을 파싱한다.

    처리 전략:
    1. _build_logical_table()로 colspan/rowspan을 논리 그리드로 변환
    2. _detect_candidate_columns()로 다중 행 헤더 합성 + 광범위 동의어 매칭으로 열 탐지
    3. 탐지된 열로 데이터 행 파싱
    4. 중요 필드(사외이사여부, 주된직업)가 비어 있으면 AI fallback으로 재추출

    반환: {후보자성명(공백제거): {"성명": ..., "사외이사여부": ..., "분리선출여부": ..., "주된직업": ...}}
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        soup = BeautifulSoup(content, 'lxml')
    except Exception:
        return {}

    candidates = {}

    for table in soup.find_all('table'):
        # "후보자성명" 없는 테이블 스킵
        if '후보자성명' not in re.sub(r'\s+', '', table.get_text()):
            continue

        grid = _build_logical_table(table)
        if not grid:
            continue

        name_col, outside_col, separate_col, occupation_col, data_start_row = _detect_candidate_columns(grid)

        if name_col == -1:
            continue

        # 데이터 행 파싱
        table_candidates = {}
        for row in grid[data_start_row:]:
            if len(row) <= name_col:
                continue
            name_raw = row[name_col].strip()
            name_key = re.sub(r'\s+', '', name_raw)
            if not name_key or len(name_key) < 2:
                continue
            # 헤더 텍스트가 재등장하면 건너뜀
            if any(kw in name_key for kw in ['후보자성명', '성명', '이사', '감사', '후보자']):
                if not re.match(r'^[가-힣]{2,5}$', name_key):
                    continue

            outside_val = row[outside_col].strip() if 0 <= outside_col < len(row) else ""
            sep_raw = row[separate_col].strip() if 0 <= separate_col < len(row) else ""
            sep_val = normalize_separate_election(sep_raw)
            occ_val = row[occupation_col].strip() if 0 <= occupation_col < len(row) else ""

            table_candidates[name_key] = {
                "성명": name_raw,
                "사외이사여부": outside_val,
                "분리선출여부": sep_val,
                "주된직업": occ_val,
            }

        # 사외이사여부 유효값 집합 (이 값이 아니면 열 탐지가 잘못된 것)
        VALID_OUTSIDE_VALS = {
            '사외이사', '사내이사', '기타비상무이사', '사외', '사내',
            '해당', '미해당', '○', '×', 'O', 'X', 'Y', 'N',
            '사외이사해당', '사외이사미해당', '사내이사해당안됨',
        }

        def _is_valid_outside(val):
            """사외이사여부 열에서 나올 법한 값인지 의미론적으로 검증.
            '해당사항없음', '해당없음' 등 N/A 표시는 잘못된 열을 읽은 것으로 판단.
            """
            if not val:
                return False
            v = re.sub(r'[\s\u3000\xa0]+', '', val)
            if not v or v in ['-', '—', '─', '–']:
                return True  # 대시 계열은 "해당없음"으로 허용
            # N/A 표시 → 잘못된 열
            if '없음' in v or '해당사항' in v:
                return False
            # 단독 기호(○/×/O/X/Y/N)도 유효
            if v in ['○', '×', 'O', 'X', 'Y', 'N']:
                return True
            # 사외이사 구분에 쓰이는 실제 값 패턴
            return any(kw in v for kw in ['사외이사', '사내이사', '비상무', '미해당', '해당'])

        # AI fallback 조건:
        # 1) HTML 파싱으로 후보자를 전혀 못 찾은 경우
        # 2) 사외이사여부가 비어있거나 의미론적으로 잘못된 값인 경우 (열 탐지 실패)
        # 3) 주된직업 열 자체를 탐지 못했고(occupation_col==-1) 후보자가 있는 경우
        needs_ai = (
            not table_candidates
            or any(not _is_valid_outside(v["사외이사여부"]) for v in table_candidates.values())
            or (occupation_col == -1 and bool(table_candidates))
        )

        if needs_ai:
            table_text = _grid_to_text(grid)
            ai_result = _extract_candidates_with_ai(table_text)
            for name_key, ai_info in ai_result.items():
                if name_key not in table_candidates:
                    table_candidates[name_key] = ai_info
                else:
                    existing = table_candidates[name_key]
                    # 사외이사여부: AI 결과가 유효하면 교체 (기존 값이 잘못됐을 수 있음)
                    if ai_info.get("사외이사여부") and _is_valid_outside(ai_info["사외이사여부"]):
                        existing["사외이사여부"] = ai_info["사외이사여부"]
                    # 주된직업: 비어있으면 AI로 채움
                    if not existing["주된직업"] and ai_info.get("주된직업"):
                        existing["주된직업"] = ai_info["주된직업"]
                    # 분리선출: AI가 Y라고 판단하고 HTML 파싱이 N이면 AI 우선
                    if ai_info.get("분리선출여부") == "Y" and existing["분리선출여부"] == "N":
                        existing["분리선출여부"] = "Y"

        candidates.update(table_candidates)

    return candidates


def find_candidate_info(candidates_map, agenda_title):
    """
    안건 제목에서 후보자명을 추출하여 candidates_map에서 매칭한다.
    반환: (성명, 사외이사여부, 분리선출여부, 주된직업)
    """
    empty = ("", "", "", "")
    if not candidates_map:
        return empty

    title_no_sp = re.sub(r'\s+', '', agenda_title)

    # 1) 제목에서 이름 패턴 추출
    name_pats = [
        r'후보자[:：]([가-힣]{2,5})',
        r'[（(]([가-힣]{2,5})[）)]',
        r'([가-힣]{2,5})후보',
    ]
    for pat in name_pats:
        m = re.search(pat, title_no_sp)
        if m:
            extracted = m.group(1)
            if extracted in candidates_map:
                v = candidates_map[extracted]
                return v["성명"], v["사외이사여부"], v["분리선출여부"], v["주된직업"]
            for k, v in candidates_map.items():
                if extracted in k or k in extracted:
                    return v["성명"], v["사외이사여부"], v["분리선출여부"], v["주된직업"]

    # 2) candidates 키를 제목에서 직접 탐색
    for name_key, v in candidates_map.items():
        if name_key in title_no_sp:
            return v["성명"], v["사외이사여부"], v["분리선출여부"], v["주된직업"]

    # 3) 단일 후보자면 그냥 사용
    if len(candidates_map) == 1:
        v = list(candidates_map.values())[0]
        return v["성명"], v["사외이사여부"], v["분리선출여부"], v["주된직업"]

    return empty


# ─────────────────────────────────────────────
# 5-B. 이사보수한도 정보 추출 (텍스트 파싱)
# ─────────────────────────────────────────────

def extract_remuneration_from_html(file_path):
    """
    HTML에서 보수한도 테이블을 파싱하여 당기/전기 보수 정보를 추출한다.
    텍스트 파싱보다 먼저 시도하는 primary 방법.
    """
    empty = {
        "당기_이사수": "", "당기_사외이사수": "", "당기_보수총액또는최고한도액": "",
        "전기_이사수": "", "전기_사외이사수": "", "전기_실제지급보수총액": "", "전기_최고한도액": "",
    }
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        soup = BeautifulSoup(content, 'lxml')
    except Exception:
        return empty

    def _amt(text):
        """금액 텍스트 → 억원 문자열"""
        t = re.sub(r'[\s,]', '', str(text))
        if not t:
            return ""
        m = re.match(r'(\d+(?:\.\d+)?)억원?$', t)
        if m:
            return m.group(1) + '억원'
        m = re.match(r'(\d+(?:\.\d+)?)백만원?$', t)
        if m:
            val = float(m.group(1)) / 100
            return (str(int(val)) if val == int(val) else f"{val:.1f}") + '억원'
        m = re.match(r'(\d+)원?$', t)
        if m:
            val = int(m.group(1)) / 100_000_000
            return (str(int(val)) if val == int(val) else f"{val:.2f}") + '억원'
        # 원문 그대로 숫자+단위가 있으면 추출 시도
        m2 = re.search(r'([\d,]+)\s*억\s*원', text)
        if m2:
            return m2.group(1).replace(',', '') + '억원'
        return text.strip()

    def _count(text):
        """이사수(사외이사수) 패턴 → (이사수, 사외이사수)"""
        m = re.search(r'(\d{1,3})\s*[（(]\s*(\d{1,3})\s*[）)]', text)
        return (m.group(1), m.group(2)) if m else ("", "")

    for table in soup.find_all('table'):
        ttext = re.sub(r'\s+', '', table.get_text())
        if not ('보수총액' in ttext or '최고한도' in ttext):
            continue
        if not ('이사의수' in ttext or '이사수' in ttext):
            continue

        grid = _build_logical_table(table)
        if not grid:
            continue

        result = dict(empty)
        period = None  # 'cur' or 'prev'

        for row in grid:
            if not row:
                continue
            label = re.sub(r'\s+', '', row[0])
            val = row[1].strip() if len(row) > 1 else ""

            # 당기 / 전기 행 감지
            if re.search(r'당\s*기', row[0]):
                period = 'cur'
                tc, oc = _count(row[0] + val)
                if tc:
                    result["당기_이사수"] = tc
                    result["당기_사외이사수"] = oc
                continue
            if re.search(r'전\s*기', row[0]):
                period = 'prev'
                tc, oc = _count(row[0] + val)
                if tc:
                    result["전기_이사수"] = tc
                    result["전기_사외이사수"] = oc
                continue

            if period is None:
                continue

            # 이사수(사외이사수)
            if '이사의수' in label or '이사수' in label:
                tc, oc = _count(val)
                if tc:
                    if period == 'cur':
                        result["당기_이사수"] = tc
                        result["당기_사외이사수"] = oc
                    else:
                        result["전기_이사수"] = tc
                        result["전기_사외이사수"] = oc

            # 금액
            elif '보수총액' in label or '최고한도' in label or '실제지급' in label:
                amt = _amt(val) if val else ""
                if not amt:
                    continue
                if period == 'cur':
                    result["당기_보수총액또는최고한도액"] = amt
                else:
                    if '실제지급' in label:
                        result["전기_실제지급보수총액"] = amt
                    elif '최고한도' in label:
                        result["전기_최고한도액"] = amt
                    else:
                        # "보수총액" 레이블은 전기에서는 실제지급으로 매핑
                        if not result["전기_실제지급보수총액"]:
                            result["전기_실제지급보수총액"] = amt
                        else:
                            result["전기_최고한도액"] = amt

        if any(v for v in result.values()):
            return result

    return empty


def normalize_amount_to_억원(text):
    """금액 텍스트 → 억원 단위 문자열. 예: '11,200백만원' → '112억원'"""
    t = re.sub(r'[\s,]', '', str(text))
    m = re.match(r'(\d+(?:\.\d+)?)억원?$', t)
    if m:
        return m.group(1) + '억원'
    m = re.match(r'(\d+(?:\.\d+)?)백만원?$', t)
    if m:
        val = float(m.group(1)) / 100
        s = str(int(val)) if val == int(val) else f"{val:.1f}"
        return s + '억원'
    m = re.match(r'(\d+)원?$', t)
    if m:
        val = int(m.group(1)) / 100_000_000
        s = str(int(val)) if val == int(val) else f"{val:.2f}"
        return s + '억원'
    return text.strip()


def extract_remuneration_info(text):
    """
    보수한도 섹션 텍스트에서 당기/전기 이사수·사외이사수·금액을 추출한다.
    """
    empty = {
        "당기_이사수": "", "당기_사외이사수": "", "당기_보수총액또는최고한도액": "",
        "전기_이사수": "", "전기_사외이사수": "", "전기_실제지급보수총액": "", "전기_최고한도액": "",
    }
    if not text:
        return empty

    당기_m = re.search(r'당\s*기', text)
    전기_m = re.search(r'전\s*기', text)
    if not 당기_m:
        return empty

    당기_start = 당기_m.start()
    전기_start = 전기_m.start() if 전기_m else len(text)
    당기_text = text[당기_start:전기_start]
    전기_text = text[전기_start: min(전기_start + 3000, len(text))] if 전기_m else ""

    def parse_counts(section):
        """이사수(사외이사수) 패턴 예: '7( 4 )' """
        m = re.search(r'(\d{1,3})\s*[（(]\s*(\d{1,3})\s*[）)]', section)
        return (m.group(1), m.group(2)) if m else ("", "")

    def find_amounts(section):
        """억원 우선 → 백만원 → 원 순으로 금액 리스트 반환"""
        amts = []
        for m in re.finditer(r'(\d[\d,]*(?:\.\d+)?)\s*억\s*원', section):
            amts.append(m.group(1).replace(',', '') + '억원')
        if amts:
            return amts
        for m in re.finditer(r'(\d[\d,]*)\s*백만\s*원', section):
            amts.append(normalize_amount_to_억원(m.group(1) + '백만원'))
        if amts:
            return amts
        for m in re.finditer(r'(\d[\d,]{7,})\s*원', section):
            amts.append(normalize_amount_to_억원(m.group(1).replace(',', '') + '원'))
        return amts

    result = dict(empty)

    # 당기
    t1, t2 = parse_counts(당기_text)
    result["당기_이사수"] = t1
    result["당기_사외이사수"] = t2
    amts = find_amounts(당기_text)
    if amts:
        result["당기_보수총액또는최고한도액"] = amts[0]

    # 전기
    if 전기_text:
        t1, t2 = parse_counts(전기_text)
        result["전기_이사수"] = t1
        result["전기_사외이사수"] = t2

        실제_m = re.search(r'실제\s*지급[^\d억원]*?([\d,]+억원|[\d,]+백만원|[\d,]{7,}원)', 전기_text)
        한도_m = re.search(r'최고\s*한도[^\d억원]*?([\d,]+억원|[\d,]+백만원|[\d,]{7,}원)', 전기_text)

        if 실제_m:
            result["전기_실제지급보수총액"] = normalize_amount_to_억원(실제_m.group(1))
        if 한도_m:
            result["전기_최고한도액"] = normalize_amount_to_억원(한도_m.group(1))

        if not result["전기_실제지급보수총액"] or not result["전기_최고한도액"]:
            amts = find_amounts(전기_text)
            if not result["전기_실제지급보수총액"] and amts:
                result["전기_실제지급보수총액"] = amts[0]
            if not result["전기_최고한도액"] and len(amts) > 1:
                result["전기_최고한도액"] = amts[1]

    return result


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
        # 복잡한 규칙 준수가 필요하므로 Haiku 3.5 사용 (Haiku 3보다 지시 따르기 우수)
        raw_text = call_bedrock(prompt, max_tokens=100, model_id=BEDROCK_MODEL_ID_HAIKU35)
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
# 6. 주주총회결과 보고서 처리
# ─────────────────────────────────────────────

def normalize_num_for_match(num_str):
    """안건번호를 매칭용 정규화 키로 변환.
    '제2-1호 의안' → '2-1', '1' → '1', '제3호' → '3'
    """
    s = re.sub(r'\s+', '', str(num_str))
    # "제N호의안" / "제N-M호" 형태에서 숫자-숫자 부분만 추출
    m = re.search(r'제([\d][\d\-]*[\d]?)호', s)
    if m:
        return m.group(1)
    # 이미 "1", "2-1", "2-1-1" 형태
    m2 = re.match(r'^([\d]+(?:-[\d]+)*)$', s)
    if m2:
        return m2.group(1)
    return s


def fetch_agm_result_rcept_no(corp_code, bgn_de, end_de):
    """
    corp_code로 DART에서 정기주주총회결과 보고서를 검색하여 rcept_no를 반환한다.
    여러 건이 있으면 가장 최신(rcept_no 최대값) 반환.
    """
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": API_KEY,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "page_count": 20,
        "sort": "date",
        "sort_mth": "desc",
    }
    try:
        res = safe_get(url, params)
        data = res.json()
        if data.get("status") != "000":
            return ""
        best = ""
        for item in data.get("list", []):
            report_nm = str(item.get("report_nm", ""))
            if "주주총회결과" in report_nm:
                rno = str(item.get("rcept_no", ""))
                if rno > best:
                    best = rno
        return best
    except Exception as e:
        print(f"  [결과보고서 검색 실패] {corp_code}: {e}")
    return ""


def _decode_report_bytes(raw):
    for enc in ('utf-8', 'euc-kr', 'cp949'):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode('utf-8', errors='ignore')


def _iter_report_markup_files(path_or_folder):
    """결과 보고서 폴더/파일에서 XML/HTML 파일을 재귀적으로 순회한다."""
    if not path_or_folder:
        return
    if os.path.isdir(path_or_folder):
        for root, _, files in os.walk(path_or_folder):
            for fname in files:
                lower = fname.lower()
                if lower.endswith(('.xml', '.html', '.htm')):
                    yield os.path.join(root, fname)
    elif os.path.isfile(path_or_folder) and path_or_folder.lower().endswith(('.xml', '.html', '.htm')):
        yield path_or_folder


def _score_result_table(table, combined_headers, grid):
    """주주총회 안건 세부내역 결과표 후보에 점수를 부여한다."""
    text_nsp = re.sub(r'\s+', '', table.get_text())
    score = 0
    if '주주총회안건세부내역' in text_nsp:
        score += 100
    if '가결여부' in text_nsp:
        score += 20
    if '결의구분' in text_nsp:
        score += 20
    if '회의목적사항' in text_nsp or '목적사항' in text_nsp or '의안명' in text_nsp or '안건명' in text_nsp:
        score += 20
    score += min(30, len(grid))
    header_text = ''.join(combined_headers)
    if '찬성' in header_text:
        score += 10
    if '반대' in header_text:
        score += 10
    if '비고' in header_text:
        score += 5
    return score


def extract_agm_result_table(path_or_folder):
    """
    정기주주총회결과 보고서에서 '주주총회 안건 세부내역' 표를 파싱한다.
    zip을 풀어놓은 폴더 전체를 재귀적으로 훑어서, 삼성전자처럼 상세 표가
    별도 XML/HTML 첨부에 들어 있는 경우도 빠짐없이 잡는다.

    반환: {정규화번호: {"결의구분":, "회의목적사항":, "가결여부":,
                       "찬성률1":, "찬성률2":, "반대율":, "비고":}}
    """
    best_result = {}
    best_score = -1

    for file_path in _iter_report_markup_files(path_or_folder):
        try:
            with open(file_path, 'rb') as f:
                raw = f.read()
            content = _decode_report_bytes(raw)
            soup = BeautifulSoup(content, 'lxml')
        except Exception:
            continue

        for table in soup.find_all('table'):
            table_text_nsp = re.sub(r'\s+', '', table.get_text())
            if '가결여부' not in table_text_nsp:
                continue
            if not any(kw in table_text_nsp for kw in ['결의구분', '회의목적사항', '목적사항', '의안명', '안건명']):
                continue

            grid = _build_logical_table(table)
            if not grid or len(grid) < 2:
                continue

            HEADER_KWS = {'번호', '결의구분', '가결여부', '회의목적사항', '목적사항', '의안명', '안건명', '찬성', '반대', '비고'}
            header_rows = []
            data_start = 0
            for row_idx, row in enumerate(grid):
                combined = re.sub(r'\s+', '', ''.join(row))
                if any(kw in combined for kw in HEADER_KWS):
                    first = re.sub(r'\s+', '', row[0]) if row else ''
                    if not re.match(r'^\d', first):
                        header_rows.append(row_idx)
                        data_start = row_idx + 1

            if not header_rows:
                continue

            n_cols = max(len(r) for r in grid)
            combined_headers = []
            for col_idx in range(n_cols):
                parts = []
                for row_idx in header_rows:
                    if col_idx < len(grid[row_idx]):
                        t = re.sub(r'[\s　 \(\)①②③④⑤]+', '', grid[row_idx][col_idx])
                        if t and t not in parts:
                            parts.append(t)
                combined_headers.append(''.join(parts))

            def _find_col(keywords, exclude=()):
                for i, h in enumerate(combined_headers):
                    if i in exclude:
                        continue
                    if any(kw in h for kw in keywords):
                        return i
                return -1

            num_col = _find_col(['번호'])
            category_col = _find_col(['결의구분'], exclude=(num_col,))
            title_col = _find_col(['회의목적사항', '목적사항', '의안명', '안건명', '의안의내용'], exclude=(num_col, category_col))
            pass_col = _find_col(['가결여부', '결의결과'], exclude=(num_col, category_col, title_col))

            rate1_col = -1
            for i, h in enumerate(combined_headers):
                if i in (num_col, category_col, title_col, pass_col):
                    continue
                if ('발행주식' in h or '발생주식' in h or '발행주식총수' in h or '발생주식총수' in h) and '찬성' in h:
                    rate1_col = i
                    break
            if rate1_col == -1:
                rate1_col = _find_col(['찬성'], exclude=(num_col, category_col, title_col, pass_col))

            rate2_col = _find_col(['의결권', '찬성'], exclude=(num_col, category_col, title_col, pass_col, rate1_col))
            if rate2_col == -1:
                for i, h in enumerate(combined_headers):
                    if i in (num_col, category_col, title_col, pass_col, rate1_col):
                        continue
                    if ('찬성' in h and '%' in h) or ('의결권행사주식수기준찬성률' in h):
                        rate2_col = i
                        break

            against_col = _find_col(['반대기관', '반대·기관', '반대기관등', '반대', '기관'],
                                    exclude=(num_col, category_col, title_col, pass_col, rate1_col, rate2_col))
            note_col = _find_col(['비고'], exclude=(num_col,))

            if num_col == -1 or title_col == -1:
                continue

            candidate_result = {}
            for row in grid[data_start:]:
                if len(row) <= max(num_col, title_col):
                    continue
                num_raw = row[num_col].strip()
                title_raw = row[title_col].strip() if title_col < len(row) else ''
                if not num_raw or not re.search(r'\d', num_raw):
                    continue
                if not title_raw:
                    continue
                num_key = normalize_num_for_match(num_raw)
                if not num_key:
                    continue

                def _get(col):
                    return row[col].strip() if 0 <= col < len(row) else ""

                candidate_result[num_key] = {
                    "결의구분": _get(category_col),
                    "회의목적사항": _get(title_col),
                    "가결여부": _get(pass_col),
                    "찬성률1": _get(rate1_col),
                    "찬성률2": _get(rate2_col),
                    "반대율": _get(against_col),
                    "비고": _get(note_col),
                }

            if not candidate_result:
                continue

            score = _score_result_table(table, combined_headers, grid) + len(candidate_result) * 5
            if score > best_score:
                best_score = score
                best_result = candidate_result

    return best_result


# ─────────────────────────────────────────────
# 7. 국민연금 의결권 행사내역
# ─────────────────────────────────────────────

NPS_LIST_URL   = "https://fund.nps.or.kr/impa/edwmpblnt/empty/getOHEF0007M0.do"
NPS_DETAIL_URL = "https://fund.nps.or.kr/impa/edwmpblnt/getOHEF0010M0.do"
NPS_REQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Referer": "https://fund.nps.or.kr/impa/edwmpblnt/getOHEF0007M0.do?menuId=MN24000636",
    "Origin": "https://fund.nps.or.kr",
}


def normalize_corp_name(name):
    """회사명 정규화: 법인형태 접두·접미 제거 후 공백/특수문자 제거, 소문자화"""
    s = str(name)
    s = re.sub(r'주식회사|㈜|\(주\)|\( *주 *\)|co\.,?\s*ltd\.?|inc\.?|corp\.?',
               '', s, flags=re.IGNORECASE)
    s = re.sub(r'[\s\u3000\xa0\.\,\-\(\)\[\]]+', '', s)
    return s.lower()


def extract_date_yyyymmdd(date_str):
    """다양한 날짜 형식 → 'YYYYMMDD' 문자열"""
    s = re.sub(r'[-/]', '', str(date_str).strip())
    if re.match(r'^\d{8}$', s):
        return s
    m = re.match(r'(\d{4})(\d{1,2})(\d{1,2})', s)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    m = re.search(r'(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일', str(date_str))
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    return ""


def _nps_post(url, payload, retries=4, timeout=30):
    """JSON body POST → HTML response (text/html 반환)"""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, data=json.dumps(payload),
                                 headers=NPS_REQ_HEADERS, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(1.5 * attempt)
    raise last_err


def fetch_nps_list_page(page_index, gmos_start_dt, gmos_end_dt):
    payload = {
        "pageIndex": page_index,
        "issueInsNm": "",
        "gmosStartDt": gmos_start_dt,
        "gmosEndDt":   gmos_end_dt,
    }
    resp = _nps_post(NPS_LIST_URL, payload)
    return resp.text


def parse_nps_last_page(html):
    """마지막 페이지 번호 추출"""
    soup = BeautifulSoup(html, 'lxml')
    nums = []
    for a in soup.select("a[data-pagenum]"):
        v = a.get("data-pagenum", "").strip()
        if v.isdigit():
            nums.append(int(v))
    if not nums:
        for a in soup.select(".pagination a, #paginationInfo a, ul.pager a"):
            t = re.sub(r'\s+', '', a.get_text())
            if t.isdigit():
                nums.append(int(t))
    return max(nums) if nums else 1


def parse_nps_list_rows(html):
    """목록 HTML에서 정기주총 행만 추출하고 상세 호출 파라미터 반환"""
    soup = BeautifulSoup(html, 'lxml')
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        row_text = ' '.join(td.get_text(strip=True) for td in tds)
        if '정기주총' not in row_text:
            continue

        # fnc_goDetail 파라미터: tr 전체 HTML에서 추출
        tr_html = str(tr)
        m = re.search(
            r"fnc_goDetail\s*\(\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\)",
            tr_html
        )
        if not m:
            continue

        a_tag = tr.find("a")
        company_name = a_tag.get_text(strip=True) if a_tag else tds[1].get_text(strip=True)

        rows.append({
            "회사명":          company_name,
            "edwmVtrtUseSn":   m.group(1),
            "dataPvsnInstCdVl": m.group(2),
            "pblcnInstCdVl":   m.group(3),
            "gmosYmd":         m.group(4),
            "gmosKindCd":      m.group(5),
        })
    return rows


def fetch_nps_detail_html(item, gmos_start_dt, gmos_end_dt):
    payload = {
        "edwmVtrtUseSn":    item["edwmVtrtUseSn"],
        "dataPvsnInstCdVl": item["dataPvsnInstCdVl"],
        "pblcnInstCdVl":    item["pblcnInstCdVl"],
        "gmosYmd":          item["gmosYmd"],
        "gmosKindCd":       item["gmosKindCd"],
        "issueInsNm":       "",
        "gmosStartDt":      gmos_start_dt,
        "gmosEndDt":        gmos_end_dt,
    }
    resp = _nps_post(NPS_DETAIL_URL, payload)
    return resp.text


def parse_nps_detail_votes(detail_html):
    """
    상세 HTML → {normalize_num_for_match(의안번호): {의안번호, 의안내용, 행사내용, 반대시 사유, 근거조항}}
    """
    soup = BeautifulSoup(detail_html, 'lxml')
    result = {}

    for table in soup.find_all('table'):
        table_text_nsp = re.sub(r'\s+', '', table.get_text())
        if '의안번호' not in table_text_nsp or '행사내용' not in table_text_nsp:
            continue

        grid = _build_logical_table(table)
        if not grid or len(grid) < 2:
            continue

        # 헤더 행 탐지
        header_rows_idx = []
        data_start = 0
        for row_idx, row in enumerate(grid):
            combined = re.sub(r'\s+', '', ''.join(row))
            if '의안번호' in combined or '행사내용' in combined:
                first = re.sub(r'\s+', '', row[0]) if row else ''
                if not re.match(r'^\d', first):
                    header_rows_idx.append(row_idx)
                    data_start = row_idx + 1

        if not header_rows_idx:
            continue

        # 열별 합성 헤더
        n_cols = max(len(r) for r in grid)
        combined_headers = []
        for col_idx in range(n_cols):
            parts = []
            for row_idx in header_rows_idx:
                if col_idx < len(grid[row_idx]):
                    t = re.sub(r'[\s\u3000\xa0\(\)①②③④⑤]+', '', grid[row_idx][col_idx])
                    if t and t not in parts:
                        parts.append(t)
            combined_headers.append(''.join(parts))

        def _fcol(keywords, excl=()):
            for i, h in enumerate(combined_headers):
                if i in excl:
                    continue
                hn = re.sub(r'\s+', '', h)
                if any(kw in hn for kw in keywords):
                    return i
            return -1

        num_col     = _fcol(['의안번호'])
        content_col = _fcol(['의안내용'], excl=(num_col,))
        action_col  = _fcol(['행사내용', '행사결과', '찬반'], excl=(num_col, content_col))
        reason_col  = _fcol(['반대시사유', '반대사유', '반대이유', '사유'],
                             excl=(num_col, content_col, action_col))
        basis_col   = _fcol(['근거조항', '근거'],
                             excl=(num_col, content_col, action_col, reason_col))

        if num_col == -1:
            continue

        for row in grid[data_start:]:
            if len(row) <= num_col:
                continue
            num_raw = row[num_col].strip()
            if not num_raw or not re.search(r'\d', num_raw):
                continue

            def _get_nps(col):
                return row[col].strip() if 0 <= col < len(row) else ""

            num_key = normalize_num_for_match(num_raw)
            if num_key:
                result[num_key] = {
                    "의안번호":    num_raw,
                    "의안내용":   _get_nps(content_col),
                    "행사내용":   _get_nps(action_col),
                    "반대시 사유": _get_nps(reason_col),
                    "근거조항":   _get_nps(basis_col),
                }

    return result


def fetch_all_nps_votes(meeting_dates):
    """
    meeting_dates: set of "YYYYMMDD" strings
    반환: {(normalized_corp_name, yyyymmdd): {num_key: vote_data}}
    """
    nps_votes = {}

    for yyyymmdd in sorted(meeting_dates):
        gmos_dt = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
        print(f"  [NPS] {gmos_dt} 조회 중...")

        try:
            first_html = fetch_nps_list_page(1, gmos_dt, gmos_dt)
            last_page = parse_nps_last_page(first_html)
        except Exception as e:
            print(f"  [NPS 목록 오류] {gmos_dt}: {e}")
            continue

        all_items = []
        for page in range(1, last_page + 1):
            try:
                html = first_html if page == 1 else fetch_nps_list_page(page, gmos_dt, gmos_dt)
                items = parse_nps_list_rows(html)
                all_items.extend(items)
                if page > 1:
                    time.sleep(0.3)
            except Exception as e:
                print(f"  [NPS 목록 페이지 오류] {page}: {e}")
                break

        print(f"  [NPS] {gmos_dt}: 정기주총 {len(all_items)}건")

        for item in all_items:
            try:
                detail_html = fetch_nps_detail_html(item, gmos_dt, gmos_dt)
                votes = parse_nps_detail_votes(detail_html)
                norm_name = normalize_corp_name(item["회사명"])
                nps_votes[(norm_name, yyyymmdd)] = votes
                time.sleep(0.2)
            except Exception as e:
                print(f"  [NPS 상세 오류] {item['회사명']}: {e}")

    return nps_votes


def _match_nps_vote(nps_votes, corp_name, meeting_date_yyyymmdd, num_key):
    """
    회사명+주총일+안건번호로 NPS 데이터 매칭.
    1) 정확 매칭, 2) 퍼지 회사명 매칭(같은 날짜 한정) 순으로 시도.
    """
    norm_name = normalize_corp_name(corp_name)
    # 1) 정확 매칭
    exact = nps_votes.get((norm_name, meeting_date_yyyymmdd), {})
    if num_key in exact:
        return exact[num_key]

    # 2) 같은 날짜 내에서 퍼지 매칭
    best_score = 0.0
    best_votes = {}
    for (nname, ndate), votes in nps_votes.items():
        if ndate != meeting_date_yyyymmdd:
            continue
        # 포함 관계 또는 앞/뒤 3글자 일치
        if nname == norm_name:
            score = 1.0
        elif nname in norm_name or norm_name in nname:
            shorter = min(len(nname), len(norm_name))
            longer  = max(len(nname), len(norm_name))
            score   = shorter / longer if longer > 0 else 0
        elif (len(nname) >= 3 and len(norm_name) >= 3 and
              (nname[:3] == norm_name[:3] or nname[-3:] == norm_name[-3:])):
            shorter = min(len(nname), len(norm_name))
            longer  = max(len(nname), len(norm_name))
            score   = (shorter / longer) * 0.85
        else:
            score = 0.0

        if score > best_score:
            best_score = score
            best_votes = votes

    if best_score >= 0.6 and num_key in best_votes:
        return best_votes[num_key]

    return {}


# ─────────────────────────────────────────────
# 8. 메인
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
                   "[정관] 구분", "[정관] 변경전 내용", "[정관] 변경후 내용", "[정관] 변경의 목적",
                   "[선임] 후보자성명", "[선임] 사외이사 후보자여부",
                   "[선임] 감사위원회 위원인 이사 분리선출 여부", "[선임] 주된직업",
                   "[당기보수] 이사수", "[당기보수] 사외이사수", "[당기보수] 보수총액 또는 최고한도액",
                   "[전기보수] 이사수", "[전기보수] 사외이사수",
                   "[전기보수] 실제 지급된 보수총액", "[전기보수] 최고한도액",
                   "[자사주승인]"]
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
        corp_code = str(report.get("corp_code", ""))

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

            # 이사감사선임 안건이 있으면 후보자 테이블 미리 파싱 (루프 밖에서 1회)
            has_선임 = any(str(i.get("category1", "")) == "이사감사선임" for i in agenda_items)
            candidates_map = extract_director_candidates_from_html(main_file) if has_선임 else {}

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

                # ── 이사감사선임 안건: [선임] 정보 추출 ──
                선임_성명 = 선임_사외이사여부 = 선임_분리선출여부 = 선임_주된직업 = ""
                if category1 == "이사감사선임":
                    선임_성명, 선임_사외이사여부, 선임_분리선출여부, 선임_주된직업 = find_candidate_info(
                        candidates_map, agenda_title
                    )

                # ── 이사감사보수 안건: [보수] 정보 추출 ──
                당기_이사수 = 당기_사외이사수 = 당기_보수총액 = ""
                전기_이사수 = 전기_사외이사수 = 전기_실제지급 = 전기_최고한도 = ""
                if category1 == "이사감사보수":
                    # 1) HTML 테이블 직접 파싱 (primary — section2 위치/길이 제한 없음)
                    remu = extract_remuneration_from_html(main_file)
                    # 2) HTML 파싱 실패시 텍스트 파싱 fallback
                    if not any(v for v in remu.values()):
                        remu_text = get_agenda_content(content_map, agenda_num, agenda_title)
                        if not remu_text or len(remu_text.strip()) < 50:
                            remu_text = section2_text
                        # section2_text에 없으면 full_text에서 보수 섹션 직접 탐색
                        if not remu_text or len(remu_text.strip()) < 50:
                            bm = re.search(r'이사\s*보수\s*한도|보수\s*한도\s*승인', full_text)
                            if bm:
                                remu_text = full_text[bm.start(): bm.start() + 3000]
                        remu = extract_remuneration_info(remu_text)
                    당기_이사수 = remu["당기_이사수"]
                    당기_사외이사수 = remu["당기_사외이사수"]
                    당기_보수총액 = remu["당기_보수총액또는최고한도액"]
                    전기_이사수 = remu["전기_이사수"]
                    전기_사외이사수 = remu["전기_사외이사수"]
                    전기_실제지급 = remu["전기_실제지급보수총액"]
                    전기_최고한도 = remu["전기_최고한도액"]

                # ── 자사주보유처분계획승인 안건: 관련 내용 전부 추출 ──
                자사주승인_내용 = ""
                if category1 == "자사주보유처분계획승인":
                    자사주승인_내용 = get_agenda_content(content_map, agenda_num, agenda_title)
                    # content_map에 없으면 full_text에서 자기주식 섹션 탐색
                    if not 자사주승인_내용:
                        zm = re.search(r'자기\s*주식\s*(?:보유|처분)|자사주\s*(?:보유|처분)', full_text)
                        if zm:
                            z_start = max(0, zm.start() - 200)
                            자사주승인_내용 = full_text[z_start: zm.start() + 5000]

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
                    "[선임] 후보자성명": 선임_성명,
                    "[선임] 사외이사 후보자여부": 선임_사외이사여부,
                    "[선임] 감사위원회 위원인 이사 분리선출 여부": 선임_분리선출여부,
                    "[선임] 주된직업": 선임_주된직업,
                    "[당기보수] 이사수": 당기_이사수,
                    "[당기보수] 사외이사수": 당기_사외이사수,
                    "[당기보수] 보수총액 또는 최고한도액": 당기_보수총액,
                    "[전기보수] 이사수": 전기_이사수,
                    "[전기보수] 사외이사수": 전기_사외이사수,
                    "[전기보수] 실제 지급된 보수총액": 전기_실제지급,
                    "[전기보수] 최고한도액": 전기_최고한도,
                    "[자사주승인]": 자사주승인_내용,
                    "_charter_key": num_clean if category1 == "정관변경" else "",
                    "_corp_code": corp_code,
                })

        except Exception as e:
            fail_list.append([corp_name, stock_code, rcept_no, f"extract_error: {str(e)}"])

        finally:
            shutil.rmtree(folder_name, ignore_errors=True)

        time.sleep(0.08)

    # 내부 추적용 _charter_key 열 제거
    for row in all_rows:
        row.pop("_charter_key", None)

    # ── 주주총회결과 보고서 fetch & 매칭 ──
    # 결과보고서는 소집공고 기간 이후에 올라오므로 +90일 범위로 검색
    # 날짜 형식 정규화: "2026-02-25" → "20260225"
    def _norm_date(d):
        return d.replace("-", "").replace("/", "").strip()

    end_date_norm = _norm_date(end_date)
    start_date_norm = _norm_date(start_date)
    result_end_date = (datetime.strptime(end_date_norm, "%Y%m%d") + timedelta(days=90)).strftime("%Y%m%d")
    result_start_date = start_date_norm  # 소집공고 시작일부터

    write_progress(progress_file, "running", 96, "주주총회결과 보고서 검색 중...")

    # 회사별 결과 테이블 수집 (corp_code → {num_key → result_data})
    corp_result_map = {}
    seen_corp_codes = {}  # corp_code → corp_name (중복 fetch 방지)
    for row in all_rows:
        cc = row.get("_corp_code", "")
        cn = row.get("회사명", "")
        if cc and cc not in seen_corp_codes:
            seen_corp_codes[cc] = cn

    for corp_code_r, corp_name_r in seen_corp_codes.items():
        result_rcept = fetch_agm_result_rcept_no(corp_code_r, result_start_date, result_end_date)
        if not result_rcept:
            continue
        rfolder = download_report(result_rcept)
        if not rfolder:
            continue
        corp_result_map[corp_code_r] = extract_agm_result_table(rfolder)
        if not corp_result_map[corp_code_r]:
            print(f"  [결과표 파싱 실패] {corp_name_r}")
        shutil.rmtree(rfolder, ignore_errors=True)
        time.sleep(0.08)

    # [결과] 열 데이터를 각 행에 매핑 (FULL OUTER JOIN)
    # ─ 소집공고에 있고 결과에도 있으면: 같은 행에 결과 채움
    # ─ 소집공고에 있고 결과에 없으면: 결과 열 비워둠
    # ─ 소집공고에 없고 결과에만 있으면: 새 행 추가 (소집공고 열 비움)
    RESULT_COL_NAMES = [
        "[결과] 번호",
        "[결과] 결의구분",
        "[결과] 회의목적사항",
        "[결과] 가결여부",
        "[결과] 발생주식총수 기준 찬성률(1)",
        "[결과] (1)중 의결권 행사 주식수 기준 찬성률",
        "[결과] (1)중 의결권 행사 주식수 기준 반대·기관 등 비율(%)",
        "[결과] 비고",
    ]
    RESULT_FIELD_MAP = {
        "[결과] 번호":                                    None,   # num_key 자체
        "[결과] 결의구분":                                "결의구분",
        "[결과] 회의목적사항":                            "회의목적사항",
        "[결과] 가결여부":                                "가결여부",
        "[결과] 발생주식총수 기준 찬성률(1)":              "찬성률1",
        "[결과] (1)중 의결권 행사 주식수 기준 찬성률":      "찬성률2",
        "[결과] (1)중 의결권 행사 주식수 기준 반대·기관 등 비율(%)": "반대율",
        "[결과] 비고":                                    "비고",
    }

    # 알림 컬럼: 한쪽에만 있을 때 표시
    NOTICE_ONLY_MARK  = ""   # 소집공고에만 있을 때 [결과] 번호 값 (빈 값으로 둠)
    RESULT_ONLY_NOTICE_COLS = [   # 결과-only 행에서 비울 소집공고 관련 열들
        "주주제안여부", "주주제안자", "안건분류1", "안건분류2",
        "[정관] 구분", "[정관] 변경전 내용", "[정관] 변경후 내용", "[정관] 변경의 목적",
        "[선임] 후보자성명", "[선임] 사외이사 후보자여부",
        "[선임] 감사위원회 위원인 이사 분리선출 여부", "[선임] 주된직업",
        "[당기보수] 이사수", "[당기보수] 사외이사수", "[당기보수] 보수총액 또는 최고한도액",
        "[전기보수] 이사수", "[전기보수] 사외이사수",
        "[전기보수] 실제 지급된 보수총액", "[전기보수] 최고한도액",
        "[자사주승인]",
    ]

    def _fill_result_cols(row, num_key, result_data):
        for col_name in RESULT_COL_NAMES:
            field = RESULT_FIELD_MAP[col_name]
            if field is None:
                row[col_name] = num_key if result_data else ""
            else:
                row[col_name] = result_data.get(field, "")

    # Step 1: 소집공고 행에 결과 매핑 + 매칭된 결과 키 추적
    used_result_keys = set()  # (corp_code, num_key)

    for row in all_rows:
        cc = row.get("_corp_code", "")
        num_key = normalize_num_for_match(row.get("안건번호", ""))
        result_data = corp_result_map.get(cc, {}).get(num_key, {})
        _fill_result_cols(row, num_key, result_data)
        if result_data:
            used_result_keys.add((cc, num_key))

    # Step 2: 결과보고서에만 있는 항목 → 새 행으로 추가
    # corp_code별 대표 메타데이터 수집 (회사명, 시장분류, 공고일, 주총일)
    corp_meta = {}
    for row in all_rows:
        cc = row.get("_corp_code", "")
        if cc and cc not in corp_meta:
            corp_meta[cc] = {
                "회사명":   row.get("회사명", ""),
                "시장분류": row.get("시장분류", ""),
                "공고일":   row.get("공고일", ""),
                "주총일":   row.get("주총일", ""),
                "_corp_code": cc,
            }

    extra_rows = []
    for cc, result_table in corp_result_map.items():
        meta = corp_meta.get(cc)
        if not meta:
            continue
        for num_key, result_data in result_table.items():
            if (cc, num_key) in used_result_keys:
                continue
            # 결과-only 새 행: 소집공고 열은 비우고 결과 열만 채움
            new_row = {
                "회사명":   meta["회사명"],
                "시장분류": meta["시장분류"],
                "공고일":   meta["공고일"],
                "주총일":   meta["주총일"],
                # 소집공고에 없으므로 안건번호/제목은 결과에서 가져옴
                "안건번호": format_agenda_num(num_key),
                "안건 제목": result_data.get("회의목적사항", ""),
                "_corp_code": cc,
            }
            for col in RESULT_ONLY_NOTICE_COLS:
                new_row[col] = ""
            _fill_result_cols(new_row, num_key, result_data)
            extra_rows.append(new_row)

    all_rows.extend(extra_rows)

    # Step 3: 회사명 → 공고일 → 안건번호 순 정렬
    def _row_sort_key(row):
        corp  = row.get("회사명", "")
        date  = row.get("공고일", "") or row.get("주총일", "")
        # 안건번호에서 정렬키 추출 (결과-only 행도 format_agenda_num으로 채웠으므로 동일 함수 사용)
        num   = normalize_num_for_match(row.get("안건번호", ""))
        return (corp, date, agenda_sort_key(num))

    all_rows.sort(key=_row_sort_key)

    # ── 국민연금 의결권 행사내역 fetch & 매칭 ──
    write_progress(progress_file, "running", 97, "국민연금 의결권 행사내역 조회 중...")

    # 고유 주총일(YYYYMMDD) 수집
    meeting_dates_set = set()
    for row in all_rows:
        mt = extract_date_yyyymmdd(row.get("주총일", ""))
        if mt:
            meeting_dates_set.add(mt)

    nps_votes = {}
    if meeting_dates_set:
        try:
            nps_votes = fetch_all_nps_votes(meeting_dates_set)
        except Exception as e:
            print(f"  [NPS 전체 오류] {e}")

    NPS_COL_NAMES = [
        "[국민연금] 의안번호",
        "[국민연금] 의안내용",
        "[국민연금] 행사내용",
        "[국민연금] 반대시 사유",
        "[국민연금] 근거조항",
    ]
    NPS_FIELD_MAP = {
        "[국민연금] 의안번호":   "의안번호",
        "[국민연금] 의안내용":   "의안내용",
        "[국민연금] 행사내용":   "행사내용",
        "[국민연금] 반대시 사유": "반대시 사유",
        "[국민연금] 근거조항":   "근거조항",
    }

    for row in all_rows:
        corp_name_r   = row.get("회사명", "")
        meeting_date_r = extract_date_yyyymmdd(row.get("주총일", ""))
        num_key_r      = normalize_num_for_match(row.get("안건번호", ""))

        vote_data = _match_nps_vote(nps_votes, corp_name_r, meeting_date_r, num_key_r)

        for col_name in NPS_COL_NAMES:
            row[col_name] = vote_data.get(NPS_FIELD_MAP[col_name], "")

    # 내부 추적용 _corp_code 열 제거
    for row in all_rows:
        row.pop("_corp_code", None)

    result_df = pd.DataFrame(all_rows)
    fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result_df.to_excel(writer, sheet_name="data", index=False)
        fail_df.to_excel(writer, sheet_name="fail", index=False)

    write_progress(progress_file, "done", 100, "엑셀 생성 완료", total_reports, total_reports)


if __name__ == "__main__":
    main()
