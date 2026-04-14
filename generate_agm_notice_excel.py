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
from openai import OpenAI

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

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
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


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
# 2. 문서 파싱
# ─────────────────────────────────────────────

def get_full_text(soup):
    """줄바꿈 보존하며 전체 텍스트 추출"""
    lines = []
    for elem in soup.find_all(["p", "div", "td", "th", "li", "span", "br"]):
        text = elem.get_text(" ", strip=True)
        if text:
            lines.append(text)
    return "\n".join(lines)


def extract_meeting_date(full_text):
    """
    주총일 (일시) 추출.
    DART 문서에서 '1. 일시', '일 시', '일시' 뒤에 오는 날짜/시간을 찾는다.
    """
    # 패턴: "일시 : 2026년 3월 28일 오전 9시" 또는 "1. 일 시\n2026년..."
    patterns = [
        r'일\s*시\s*[:\：]\s*([\d]{4}\s*년\s*[\d]+\s*월\s*[\d]+\s*일[^\n]{0,80})',
        r'[①1]\.\s*일\s*시[^\n]*\n\s*([\d]{4}\s*년[^\n]{0,80})',
        r'일\s{0,2}시\s*\n\s*([\d]{4}\s*년[^\n]{0,80})',
        r'([\d]{4}\s*년\s*[\d]+\s*월\s*[\d]+\s*일\s*\([월화수목금토일]\)[^\n]{0,50}(?:오전|오후|AM|PM)?[^\n]{0,20})',
    ]
    for pat in patterns:
        m = re.search(pat, full_text)
        if m:
            return re.sub(r'\s+', ' ', m.group(1)).strip()
    return ""


def extract_resolution_block(full_text):
    """
    결의사항 섹션 텍스트 블록을 추출한다.
    """
    # "결의사항" 찾기
    idx = -1
    for keyword in ["나. 결의사항", "결 의 사 항", "결의사항"]:
        found = full_text.find(keyword)
        if found != -1:
            idx = found
            break
    if idx == -1:
        return ""

    chunk = full_text[idx: idx + 4000]

    # 다음 주요 섹션에서 자르기
    end_keywords = ["주주총회 목적사항", "목적사항별", "첨부서류", "붙임", "※ 기타"]
    end_idx = len(chunk)
    for ek in end_keywords:
        pos = chunk.find(ek, 20)
        if 0 < pos < end_idx:
            end_idx = pos
    return chunk[:end_idx]


def parse_agenda_items_from_block(resolution_block):
    """
    결의사항 텍스트에서 개별 안건 항목을 추출한다.

    반환: list of dict {num, text, is_sub}
      - num: "1", "2", "2-1", "2-2" 등
      - text: 안건 텍스트 전체 (해당 줄/단락)
      - is_sub: True if 하위 안건(N-M 형식)
    """
    if not resolution_block:
        return []

    # 안건 번호 패턴 찾기
    # 예: "제1호 의안", "제2-1호", "(주주제안) 제3호", "○ 제4호 의안"
    item_pattern = re.compile(
        r'(?:(?:○|●|◎|▶|·|•|□|■|ㅁ)\s*)?'
        r'(?:\(주주제안\)\s*)?'
        r'제\s*(\d+(?:-\d+)?)\s*호(?:\s*의\s*안)?\s*[:\：]?\s*'
        r'(.{0,200}?)(?=\n|제\s*\d+(?:-\d+)?\s*호|$)',
        re.MULTILINE | re.DOTALL
    )

    # 줄 단위로 처리하는 방식이 더 안정적
    lines = resolution_block.split('\n')
    items = []
    current_item = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        m = re.search(
            r'(?:(?:○|●|◎|▶|·|•|□|■|ㅁ)\s*)?(?:\(주주제안\)\s*)?'
            r'제\s*(\d+(?:-\d+)?)\s*호(?:\s*의\s*안)?',
            line
        )
        if m:
            if current_item:
                items.append(current_item)
            num = m.group(1).replace(' ', '')
            is_sub = '-' in num
            current_item = {
                'num': num,
                'text': line,
                'is_sub': is_sub
            }
        elif current_item:
            # 같은 안건 설명이 여러 줄에 걸친 경우 합치기
            # (단, 새로운 주요 섹션 시작이면 중단)
            if re.search(r'^(?:가\.|나\.|다\.|라\.|[①-⑩]|\d+\.)', line):
                items.append(current_item)
                current_item = None
            else:
                current_item['text'] += ' ' + line

    if current_item:
        items.append(current_item)

    return items


def merge_agenda_items(items_raw):
    """
    안건 합치기 규칙 적용:
    - 상위 안건(N)에 하위 안건(N-1, N-2, ...)이 있을 경우:
        * 상위 + 첫 번째 하위 → 한 셀
        * 두 번째 하위부터 → 각각 별도 셀
    - 하위 안건 없는 일반 안건 → 그대로 한 셀
    """
    if not items_raw:
        return []

    merged = []
    i = 0
    while i < len(items_raw):
        item = items_raw[i]

        if item['is_sub']:
            # 부모 없이 등장하는 하위 안건 → 그냥 추가
            merged.append(item['text'])
            i += 1
            continue

        base_num = item['num']

        # 다음 항목이 이 안건의 하위 항목인지 확인
        if i + 1 < len(items_raw) and items_raw[i + 1]['is_sub']:
            next_item = items_raw[i + 1]
            next_base = next_item['num'].split('-')[0]

            if next_base == base_num:
                # 상위 + 첫 번째 하위 합치기
                combined = item['text'] + "\n    " + next_item['text']
                merged.append(combined)
                i += 2

                # 나머지 하위 항목들은 각각 별도 셀
                while i < len(items_raw) and items_raw[i]['is_sub']:
                    sub = items_raw[i]
                    sub_base = sub['num'].split('-')[0]
                    if sub_base == base_num:
                        merged.append("    " + sub['text'])
                        i += 1
                    else:
                        break
                continue

        merged.append(item['text'])
        i += 1

    return merged


def extract_section2_text(full_text):
    """
    '주주총회 목적사항별 기재사항' 섹션 텍스트를 추출한다.
    """
    keywords = [
        "주주총회 목적사항별 기재사항",
        "주주총회목적사항별기재사항",
        "목적사항별 기재사항",
        "2. 주주총회 목 적 사 항 별 기 재 사 항",
    ]
    for kw in keywords:
        idx = full_text.find(kw)
        if idx != -1:
            # 넉넉하게 50000자
            return full_text[idx: idx + 50000]
    return ""


def build_section2_content_map(section2_text):
    """
    section2 텍스트에서 안건번호 → 내용 매핑을 만든다.
    예: {"1": "...", "2": "...", "2-1": "..."}
    """
    if not section2_text:
        return {}

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
        # 공백 정리 (너무 많은 공백/줄바꿈 정리)
        raw_content = re.sub(r'\n{3,}', '\n\n', raw_content)
        raw_content = re.sub(r'[ \t]{2,}', ' ', raw_content)
        content_map[num] = raw_content.strip()

    return content_map


def is_financial_statement_item(title_text):
    """재무제표 관련 안건인지 확인"""
    fin_keywords = ['재무제표', '연결재무제표', '재 무 제 표', '이익잉여금처분계산서']
    for kw in fin_keywords:
        if kw in title_text:
            return True
    return False


def get_agenda_content(content_map, agenda_num, agenda_title):
    """
    안건번호로 section2 내용을 찾는다.
    재무제표 안건은 빈 문자열 반환.
    """
    if is_financial_statement_item(agenda_title):
        return ""

    num_clean = agenda_num.replace(' ', '')

    # 직접 매칭
    if num_clean in content_map:
        return content_map[num_clean]

    # 하위 안건의 경우 상위 안건 내용에서 해당 하위 부분 탐색
    if '-' in num_clean:
        base = num_clean.split('-')[0]
        if base in content_map:
            parent_content = content_map[base]
            sub_pattern = re.compile(
                r'제\s*' + re.escape(num_clean) + r'\s*호',
                re.IGNORECASE
            )
            m = sub_pattern.search(parent_content)
            if m:
                # 해당 하위 안건 시작부터 다음 하위 안건까지
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
# 3. AI 분석 (G, H, I, J 열)
# ─────────────────────────────────────────────

def analyze_agendas_with_ai(agenda_items_data, resolution_text):
    """
    안건 목록 전체를 한 번의 AI 호출로 분석한다.
    반환: list of {shareholder_proposal, proposer, category1, category2}
    """
    if not openai_client:
        return [{"shareholder_proposal": "", "proposer": "", "category1": "", "category2": ""}
                for _ in agenda_items_data]

    items_text = ""
    for i, item in enumerate(agenda_items_data, 1):
        content_preview = (item['content'] or "")[:400]
        items_text += f"[안건 {i}] 번호: {item['num']}\n제목: {item['title']}\n내용요약: {content_preview}\n\n"

    prompt = f"""다음은 주주총회 소집공고 문서의 결의사항과 안건 목록입니다.

【결의사항 전체 텍스트】
{resolution_text[:3000]}

【안건 목록 (총 {len(agenda_items_data)}개)】
{items_text}

각 안건에 대해 아래 형식의 JSON을 반환하세요. 반드시 안건 순서와 동일하게 {len(agenda_items_data)}개를 반환해야 합니다.

분석 항목:
1. shareholder_proposal: 해당 안건이 주주제안인지 여부
   - "Y": 결의사항 텍스트에서 해당 안건 앞에 "(주주제안)" 표시가 있거나 주주제안으로 명시된 경우만
   - "N": 그 외 모든 경우
   ※ 주의: 특정 호 안건만 주주제안인 경우를 정확히 판단할 것 (전체가 주주제안이 아닌 이상 해당 안건만 Y)

2. proposer: shareholder_proposal이 "Y"인 경우 제안 주주명. "N"이면 빈 문자열.

3. category1: 안건 제목과 내용을 보고 다음 중 하나로 분류:
   - "재무제표승인": 재무제표, 연결재무제표, 이익잉여금처분 승인
   - "이사감사선임": 이사/감사/감사위원 선임·선출
   - "정관변경": 정관 일부 또는 전부 변경
   - "이사감사보수": 이사·감사 보수한도 승인, 퇴직금 지급규정 등
   - "자사주보유처분계획승인": 자기주식 취득·처분·신탁 관련
   - "기타": 위에 해당 없는 경우

4. category2: category1이 "정관변경"인 경우에만 다음 중 하나로 분류, 나머지는 빈 문자열:
   - "이사 임기 유연화": 이사 임기 유연화 조항 도입
   - "이사 임기 연장": 이사 임기 기간 연장
   - "이사 정원 축소": 이사 정원(수) 감소
   - "자사주 보유": 자기주식 관련 정관 변경
   - "기타 개정 상법 반영": 상법 개정에 따른 정관 변경

반환 형식(반드시 이 형식만):
{{"results": [{{"shareholder_proposal": "N", "proposer": "", "category1": "이사감사선임", "category2": ""}}]}}"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=3000
        )
        result = json.loads(response.choices[0].message.content)
        results_list = result.get("results", [])

        # 개수가 부족하면 빈 값으로 채움
        while len(results_list) < len(agenda_items_data):
            results_list.append({"shareholder_proposal": "", "proposer": "", "category1": "", "category2": ""})

        return results_list[:len(agenda_items_data)]

    except Exception as e:
        print(f"AI 분석 오류: {e}")
        return [{"shareholder_proposal": "", "proposer": "", "category1": "", "category2": ""}
                for _ in agenda_items_data]


# ─────────────────────────────────────────────
# 4. 문서 전체 파싱 통합 함수
# ─────────────────────────────────────────────

def parse_agm_document(file_path):
    """
    DART 주주총회소집공고 문서를 파싱해 구조화된 데이터를 반환한다.
    반환: {
        meeting_date: str,
        agenda_items: [{num, title, content}],
        resolution_text: str
    }
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")
    full_text = get_full_text(soup)

    meeting_date = extract_meeting_date(full_text)
    resolution_block = extract_resolution_block(full_text)
    items_raw = parse_agenda_items_from_block(resolution_block)
    merged_titles = merge_agenda_items(items_raw)

    section2_text = extract_section2_text(full_text)
    content_map = build_section2_content_map(section2_text)

    # 안건별 데이터 구성
    agenda_items = []
    for title_text in merged_titles:
        num_match = re.search(r'제\s*(\d+(?:-\d+)?)\s*호', title_text)
        agenda_num = num_match.group(1).replace(' ', '') if num_match else ""
        item_content = get_agenda_content(content_map, agenda_num, title_text)

        agenda_items.append({
            'num': agenda_num,
            'title': title_text,
            'content': item_content
        })

    return {
        'meeting_date': meeting_date,
        'agenda_items': agenda_items,
        'resolution_text': resolution_block
    }


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
        result_df = pd.DataFrame(all_rows, columns=columns)
        fail_df = pd.DataFrame(fail_list, columns=["corp_name", "stock_code", "rcept_no", "reason"])
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="data", index=False)
            fail_df.to_excel(writer, sheet_name="fail", index=False)
        write_progress(progress_file, "done", 100, "완료", 0, 0)
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
            parsed = parse_agm_document(main_file)
            meeting_date = parsed['meeting_date']
            agenda_items = parsed['agenda_items']
            resolution_text = parsed['resolution_text']

            if not agenda_items:
                fail_list.append([corp_name, stock_code, rcept_no, "no_agenda_items_found"])
                shutil.rmtree(folder_name, ignore_errors=True)
                continue

            # AI 분석 (G, H, I, J열): 보고서 1건당 1회 호출
            ai_results = analyze_agendas_with_ai(agenda_items, resolution_text)

            for item, ai_result in zip(agenda_items, ai_results):
                all_rows.append({
                    "회사명": corp_name,
                    "시장분류": market_label,
                    "공고일": rcept_dt,
                    "주총일": meeting_date,
                    "안건 제목": item['title'],
                    "안건 내용": item['content'],
                    "주주제안여부": ai_result.get("shareholder_proposal", ""),
                    "주주제안자": ai_result.get("proposer", ""),
                    "안건분류1": ai_result.get("category1", ""),
                    "안건분류2": ai_result.get("category2", ""),
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
