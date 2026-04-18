#!/usr/bin/env python3
"""
generate_kind_institution_excel.py

KIND 의결권행사공시 페이지에서 국내기관 의결권 행사 데이터를 수집해 엑셀로 저장한다.
브라우저 없이 requests + BeautifulSoup으로 동작한다.

흐름:
  1. KIND 검색 페이지 GET → 세션 쿠키 + hidden 파라미터 수집
  2. 기간 조건으로 POST 검색 → 결과 목록 파싱 (페이지네이션 포함)
  3. 각 공시 상세 페이지 GET → 첨부서류 목록에서 '의결권 행사 및 불행사 세부내용' 링크 추출
  4. 첨부서류 뷰어 페이지 GET → 엑셀 다운로드 링크 추출
  5. 엑셀 다운로드 → 열 이름 정규화 → 통합 저장
"""

from __future__ import annotations

import re
import sys
import json
import time
import io
import argparse
import traceback
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── 상수 ─────────────────────────────────────────────────────────────────────

KIND_BASE   = "https://kind.krx.co.kr"
KIND_VOTE_URL = f"{KIND_BASE}/disclosure/disclosurebyvote.do"

# 브라우저처럼 보이는 헤더
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

# ── 타겟 열 정의 ───────────────────────────────────────────────────────────────

TARGET_COLUMNS = [
    "의결권대상법인",
    "시장구분",
    "관계",
    "주주총회일자",
    "의안번호",
    "의안유형",
    "의안명",
    "보유주식수(주)",
    "지분비율(%)",
    "찬성주식수",
    "반대주식수",
    "불행사주식수",
    "중립행사주식수",
    "행사 및 불행사 사유",
]

COLUMN_ALIASES: dict[str, list[str]] = {
    "의결권대상법인":     ["의결권대상법인", "대상법인", "회사명", "법인명", "발행회사", "기업명"],
    "시장구분":          ["시장구분", "시장", "소속부", "상장시장", "거래소"],
    "관계":              ["관계"],
    "주주총회일자":      ["주주총회일자", "주총일", "주주총회일", "총회일자", "의결권행사일", "주주총회 일자"],
    "의안번호":          ["의안번호", "안건번호", "의안 번호", "번호"],
    "의안유형":          ["의안유형", "의안 종류", "안건유형", "의안분류", "유형"],
    "의안명":            ["의안명", "의안내용", "안건명", "의안"],
    "보유주식수(주)":    ["보유주식수(주)", "보유주식수", "보유주식수(단위:주)", "보유주식"],
    "지분비율(%)":       ["지분비율(%)", "지분비율", "보유비율", "지분율", "지분율(%)"],
    "찬성주식수":        ["찬성주식수", "찬성", "찬성 주식수", "찬성주식", "찬성(주)"],
    "반대주식수":        ["반대주식수", "반대", "반대 주식수", "반대주식", "반대(주)"],
    "불행사주식수":      ["불행사주식수", "불행사", "불행사 주식수", "불행사주식", "불행사(주)"],
    "중립행사주식수":    ["중립행사주식수", "중립", "중립행사", "중립 행사 주식수", "중립주식수", "기권"],
    "행사 및 불행사 사유": ["행사 및 불행사 사유", "사유", "행사사유", "불행사사유",
                            "행사불행사사유", "의결권행사사유"],
}

# ── 진행 상태 파일 ─────────────────────────────────────────────────────────────

def write_progress(pf: str, status: str, pct: int, msg: str,
                   current: int = 0, total: int = 0) -> None:
    try:
        with open(pf, "w", encoding="utf-8") as f:
            json.dump({"status": status, "percent": pct, "message": msg,
                       "current": current, "total": total}, f, ensure_ascii=False)
    except Exception:
        pass


# ── HTTP 세션 ─────────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    retry = Retry(total=4, backoff_factor=1.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ── 엑셀 정규화 ───────────────────────────────────────────────────────────────

def _clean_col(c: str) -> str:
    return re.sub(r"[\n\r\t]+", " ", str(c)).strip()


def _find_col(df: pd.DataFrame, target: str) -> str | None:
    cands = COLUMN_ALIASES.get(target, [target])
    for cand in cands:
        if cand in df.columns:
            return cand
    for col in df.columns:
        col_ns = re.sub(r"\s+", "", col)
        for cand in cands:
            if re.sub(r"\s+", "", cand) in col_ns or col_ns in re.sub(r"\s+", "", cand):
                return col
    return None


def normalize_excel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean_col(c) for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)

    # 열이름이 Unnamed인 경우 내부에서 실제 헤더 행 탐색
    if sum("Unnamed" in str(c) for c in df.columns) >= max(1, len(df.columns) // 2):
        kws = ["의결권", "의안", "찬성", "반대", "주주총회", "법인"]
        for i in range(min(8, len(df))):
            rs = " ".join(str(v) for v in df.iloc[i].values)
            if any(k in rs for k in kws):
                df.columns = [_clean_col(str(v)) for v in df.iloc[i].values]
                df = df.iloc[i + 1:].reset_index(drop=True)
                break

    out = pd.DataFrame()
    for target in TARGET_COLUMNS:
        col = _find_col(df, target)
        out[target] = df[col].reset_index(drop=True) if col else ""

    mask = out["의결권대상법인"].astype(str).str.strip().isin(
        ["", "nan", "None", "의결권대상법인", "NaN"]
    )
    return out[~mask].reset_index(drop=True)


def read_excel_bytes(data: bytes) -> pd.DataFrame:
    """바이트 데이터로부터 엑셀을 읽어 정규화된 DataFrame 반환."""
    collected = []
    kws = ["의결권", "의안", "찬성", "반대", "주주총회"]

    try:
        xls = pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
        for sheet in xls.sheet_names:
            for hdr in range(10):
                try:
                    df = pd.read_excel(io.BytesIO(data), sheet_name=sheet,
                                       header=hdr, dtype=str, engine="openpyxl")
                    cols_str = " ".join(str(c) for c in df.columns)
                    if any(k in cols_str for k in kws):
                        normed = normalize_excel(df)
                        if len(normed) > 0:
                            collected.append(normed)
                        break
                except Exception:
                    continue
    except Exception as e:
        print(f"  [엑셀 읽기 오류] {e}")

    return (pd.concat(collected, ignore_index=True)
            if collected else pd.DataFrame(columns=TARGET_COLUMNS))


# ── KIND 스크래핑 ─────────────────────────────────────────────────────────────

def _abs_url(href: str) -> str:
    """상대 URL을 절대 URL로 변환."""
    if not href or href.startswith("javascript:") or href == "#":
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return KIND_BASE + href
    return KIND_BASE + "/" + href


def _extract_rcp_no(text: str) -> str:
    """onclick 속성 등에서 rcpNo(접수번호) 패턴 추출. 14자리 숫자."""
    m = re.search(r"['\"](\d{14})['\"]", text)
    return m.group(1) if m else ""


def _init_session(session: requests.Session) -> dict:
    """KIND 메인 페이지를 GET해 세션 쿠키와 hidden 입력값을 수집한다."""
    try:
        resp = session.get(KIND_VOTE_URL,
                           params={"method": "searchDisclosurebyVoteMain"},
                           timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        hidden = {}
        for form in soup.find_all("form"):
            for inp in form.find_all("input"):
                t = (inp.get("type") or "").lower()
                if t in ("hidden", ""):
                    name = inp.get("name")
                    if name:
                        hidden[name] = inp.get("value", "")
        print(f"  [KIND init] hidden params: {list(hidden.keys())}")
        return hidden
    except Exception as e:
        print(f"  [KIND init 오류] {e}")
        return {}


def _search_page(session: requests.Session, hidden: dict,
                 start_yyyymmdd: str, end_yyyymmdd: str,
                 page_index: int = 1) -> str:
    """검색 결과 HTML 반환. 여러 method 이름 패턴을 순서대로 시도한다."""
    method_candidates = [
        "searchDisclosurebyVote",
        "searchDisclosurebyVoteList",
        "searchDisclosurebyVoteMain",
    ]
    date_param_sets = [
        {"searchFromDate": start_yyyymmdd, "searchToDate": end_yyyymmdd},
        {"startDate": start_yyyymmdd, "endDate": end_yyyymmdd},
        {"fromDate": start_yyyymmdd, "toDate": end_yyyymmdd},
        {"searchStartDate": start_yyyymmdd, "searchEndDate": end_yyyymmdd},
    ]

    for method in method_candidates:
        for date_params in date_param_sets:
            data = {**hidden, "method": method, "pageIndex": page_index, **date_params}
            try:
                resp = session.post(KIND_VOTE_URL, data=data, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 500:
                    # 검색 결과가 있는지 간단히 확인
                    if "<table" in resp.text and "<tr" in resp.text:
                        print(f"  [KIND 검색] method={method}, dates={date_params}")
                        return resp.text
            except Exception as e:
                print(f"  [KIND 검색 오류] method={method}: {e}")
                continue
    return ""


def _parse_result_rows(html: str) -> tuple[list[dict], bool]:
    """검색결과 HTML에서 공시 링크 목록과 다음 페이지 여부를 반환."""
    soup = BeautifulSoup(html, "lxml")
    items: list[dict] = []

    for tr in soup.select("table tbody tr"):
        a = tr.find("a")
        if not a:
            continue
        href    = a.get("href", "")
        onclick = a.get("onclick", "") or ""
        text    = a.get_text(strip=True)

        # 공시가 아닌 UI 링크 제외 (예: 다음, 이전, 검색 등)
        if not text or text in ("다음", "이전", "처음", "마지막", "검색", "조회"):
            continue

        full_href = _abs_url(href)
        rcp_no    = _extract_rcp_no(onclick) or _extract_rcp_no(href)

        # 회사명: 가장 긴 td 텍스트 사용
        company = text
        for td in tr.find_all("td"):
            t = td.get_text(strip=True)
            if len(t) > len(company) and not re.match(r"^\d+$", t):
                company = t

        items.append({
            "company":  company,
            "href":     full_href,
            "onclick":  onclick,
            "rcp_no":   rcp_no,
        })

    # 다음 페이지 버튼 존재 여부
    next_sels = ["a.next", ".paging a.next", ".pagination .next",
                 "a[title='다음']", "a:contains('다음')"]
    has_next = False
    for sel in next_sels:
        try:
            el = soup.select_one(sel)
            if el and "disabled" not in (el.get("class") or []):
                has_next = True
                break
        except Exception:
            pass

    return items, has_next


def _get_detail_html(session: requests.Session, item: dict) -> str:
    """공시 상세 페이지 HTML을 가져온다."""
    rcp_no   = item.get("rcp_no", "")
    href     = item.get("href", "")

    # href가 유효한 URL이면 직접 GET
    if href:
        try:
            resp = session.get(href, timeout=30)
            if resp.status_code == 200 and len(resp.text) > 200:
                return resp.text
        except Exception as e:
            print(f"    [상세 GET 오류] href={href}: {e}")

    # rcpNo가 있으면 알려진 패턴으로 시도
    if rcp_no:
        url_patterns = [
            f"{KIND_VOTE_URL}?method=viewDetail&rcpNo={rcp_no}",
            f"{KIND_VOTE_URL}?method=searchDisclosurebyVoteDetail&rcpNo={rcp_no}",
            f"{KIND_BASE}/disclosure/disclosurebyvote.do?method=viewDetail&rcp_no={rcp_no}",
        ]
        for url in url_patterns:
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 200:
                    return resp.text
            except Exception:
                continue

    return ""


def _find_attach_url(detail_html: str) -> str:
    """상세 페이지에서 '의결권 행사 및 불행사 세부내용' 첨부 링크 URL을 반환."""
    soup = BeautifulSoup(detail_html, "lxml")

    keywords = [
        "의결권 행사 및 불행사 세부내용",
        "의결권행사및불행사세부내용",
        "의결권 행사 및 불행사",
        "행사 및 불행사 세부내용",
        "세부내용",
    ]

    for kw in keywords:
        for a in soup.find_all("a"):
            t = a.get_text(strip=True)
            if kw in t:
                href = a.get("href", "")
                onclick = a.get("onclick", "") or ""
                full = _abs_url(href)
                if full:
                    return full
                # onclick에서 URL 패턴 추출
                m = re.search(r"['\"]([^'\"]*\.do[^'\"]*)['\"]", onclick)
                if m:
                    return _abs_url(m.group(1))
                # rcpNo 추출 후 뷰어 URL 구성
                rcp = _extract_rcp_no(onclick) or _extract_rcp_no(href)
                if rcp:
                    return f"{KIND_BASE}/common/disclsviewer.do?rcpNo={rcp}"

    return ""


def _find_excel_url(viewer_html: str, viewer_url: str = "") -> str:
    """뷰어 페이지 HTML에서 엑셀 다운로드 URL을 반환."""
    soup = BeautifulSoup(viewer_html, "lxml")

    # 직접 .xls/.xlsx 링크 탐색
    for a in soup.find_all("a"):
        href = a.get("href", "")
        text = a.get_text(strip=True).lower()
        href_lower = href.lower()
        if (href_lower.endswith(".xls") or href_lower.endswith(".xlsx")
                or "excel" in href_lower or "exl" in href_lower
                or "엑셀" in text or "excel" in text.lower()):
            return _abs_url(href)

    # onclick에서 엑셀 다운로드 함수 탐색
    for a in soup.find_all("a"):
        onclick = a.get("onclick", "") or ""
        if "excel" in onclick.lower() or "엑셀" in onclick:
            m = re.search(r"['\"]([^'\"]*excel[^'\"]*)['\"]", onclick, re.I)
            if m:
                return _abs_url(m.group(1))
            # 파라미터에서 URL 구성
            rcp = _extract_rcp_no(onclick)
            if rcp:
                return f"{KIND_BASE}/common/downloadExcel.do?rcpNo={rcp}"

    # button 태그도 탐색
    for btn in soup.find_all("button"):
        onclick = btn.get("onclick", "") or ""
        text = btn.get_text(strip=True).lower()
        if "excel" in onclick.lower() or "엑셀" in text or "excel" in text:
            m = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if m:
                return _abs_url(m.group(1))

    return ""


def _download_excel(session: requests.Session, url: str) -> bytes | None:
    """엑셀 파일을 다운로드해 bytes로 반환."""
    try:
        resp = session.get(url, timeout=60, stream=True)
        if resp.status_code != 200:
            print(f"    [다운로드 실패] {url}: HTTP {resp.status_code}")
            return None
        content_type = resp.headers.get("Content-Type", "").lower()
        # 엑셀 파일 여부 확인
        is_excel = (
            "spreadsheet" in content_type
            or "excel" in content_type
            or "octet-stream" in content_type
            or url.lower().endswith((".xls", ".xlsx"))
        )
        if not is_excel:
            print(f"    [다운로드 오류] 엑셀이 아닌 응답: {content_type}")
            return None
        data = b"".join(resp.iter_content(chunk_size=65536))
        return data if data else None
    except Exception as e:
        print(f"    [다운로드 예외] {url}: {e}")
        return None


def scrape_kind_votes(start_date: str, end_date: str,
                      progress_file: str) -> list[pd.DataFrame]:
    """
    KIND 의결권행사공시를 스크래핑해 DataFrame 목록을 반환.
    start_date / end_date: 'YYYY-MM-DD' 형식
    """
    s_plain = start_date.replace("-", "")
    e_plain = end_date.replace("-", "")
    results: list[pd.DataFrame] = []

    session = make_session()
    session.headers.update({"Referer": KIND_VOTE_URL + "?method=searchDisclosurebyVoteMain"})

    write_progress(progress_file, "running", 5, "KIND 세션 초기화 중...")
    hidden = _init_session(session)

    write_progress(progress_file, "running", 8, "검색 결과 수집 중...")

    # ── 전체 공시 목록 수집 (페이지네이션) ────────────────────────────────────
    all_items: list[dict] = []
    page = 1
    while True:
        html = _search_page(session, hidden, s_plain, e_plain, page)
        if not html:
            print(f"  [페이지 {page}] 결과 없음 또는 검색 실패")
            break
        rows, has_next = _parse_result_rows(html)
        print(f"  [페이지 {page}] {len(rows)}건")
        all_items.extend(rows)
        if not rows or not has_next:
            break
        page += 1
        time.sleep(0.5)

    total = len(all_items)
    write_progress(progress_file, "running", 15,
                   f"총 {total}건 공시 처리 시작...", 0, total)
    print(f"  [합계] 총 {total}건 발견")

    if total == 0:
        # 디버그: 실제 응답 HTML 일부 출력
        debug_html = _search_page(session, hidden, s_plain, e_plain, 1)
        print(f"  [DEBUG] 검색 응답 앞 2000자:\n{debug_html[:2000]}")

    # ── 개별 공시 처리 ─────────────────────────────────────────────────────────
    for idx, item in enumerate(all_items, 1):
        pct = 15 + int((idx / max(total, 1)) * 82)
        write_progress(progress_file, "running", pct,
                       f"처리 중 ({idx}/{total}): {item.get('company', '')}",
                       idx, total)

        company = item.get("company", "")
        try:
            # 1. 상세 페이지
            detail_html = _get_detail_html(session, item)
            if not detail_html:
                print(f"  [{idx}/{total}] {company}: 상세 페이지 없음")
                continue

            # 2. 첨부서류 URL
            attach_url = _find_attach_url(detail_html)
            if not attach_url:
                print(f"  [{idx}/{total}] {company}: 세부내용 첨부 없음")
                continue

            # 3. 뷰어 페이지
            time.sleep(0.3)
            try:
                viewer_resp = session.get(attach_url, timeout=30)
                viewer_html = viewer_resp.text
            except Exception as e:
                print(f"  [{idx}/{total}] {company}: 뷰어 로드 실패 {e}")
                continue

            # 4. 엑셀 URL
            excel_url = _find_excel_url(viewer_html, attach_url)
            if not excel_url:
                print(f"  [{idx}/{total}] {company}: 엑셀 링크 없음")
                continue

            # 5. 엑셀 다운로드
            time.sleep(0.3)
            excel_bytes = _download_excel(session, excel_url)
            if not excel_bytes:
                continue

            # 6. 파싱
            df = read_excel_bytes(excel_bytes)
            if len(df) > 0:
                results.append(df)
                print(f"  [{idx}/{total}] {company}: {len(df)}행 추출")
            else:
                print(f"  [{idx}/{total}] {company}: 엑셀 파싱 결과 없음")

        except Exception as e:
            print(f"  [{idx}/{total}] {company}: 처리 오류 → {e}")

        time.sleep(0.4)

    return results


# ── 진입점 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="KIND 의결권행사공시 → 엑셀 변환기")
    parser.add_argument("--start-date",    required=True,  help="YYYY-MM-DD")
    parser.add_argument("--end-date",      required=True,  help="YYYY-MM-DD")
    parser.add_argument("--output",        default="kind_institution_result.xlsx")
    parser.add_argument("--progress-file", default="progress_kind.json")
    args = parser.parse_args()

    pf = args.progress_file
    write_progress(pf, "running", 2, "작업 시작 중...")

    try:
        dfs = scrape_kind_votes(args.start_date, args.end_date, pf)

        write_progress(pf, "running", 97, "엑셀 저장 중...")

        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            non_empty = [
                c for c in final_df.columns
                if not final_df[c].replace("", pd.NA).isna().all()
            ]
            final_df = final_df[non_empty] if non_empty else final_df
        else:
            final_df = pd.DataFrame(columns=TARGET_COLUMNS)

        final_df.to_excel(args.output, index=False)
        write_progress(pf, "done", 100, f"완료: {len(final_df)}행")
        print(f"저장 완료: {args.output} ({len(final_df)}행)")

    except Exception as e:
        write_progress(pf, "error", 0, f"오류: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
