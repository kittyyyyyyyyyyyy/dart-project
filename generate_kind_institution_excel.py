#!/usr/bin/env python3
"""
generate_kind_institution_excel.py

KIND 의결권행사공시 페이지에서 국내기관 의결권 행사 데이터를 수집해 엑셀로 저장한다.

흐름:
  1. KIND 검색 페이지에 기간 입력 → 검색
  2. 결과 목록(전 페이지) 순회 → 공시 URL 수집
  3. 각 공시 상세 페이지 → 첨부서류 "의결권 행사 및 불행사 세부내용" 클릭
  4. 문서 뷰어 팝업 → 엑셀 링크 클릭 → 다운로드
  5. 다운로드된 엑셀 정규화 → 통합 엑셀 저장

의존: playwright (headless chromium), pandas, openpyxl, xlrd
"""

import os
import re
import sys
import json
import time
import shutil
import tempfile
import argparse
import traceback
import pandas as pd
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

# ── 상수 ─────────────────────────────────────────────────────────────────────

KIND_URL = (
    "https://kind.krx.co.kr/disclosure/disclosurebyvote.do"
    "?method=searchDisclosurebyVoteMain"
)

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

# 원본 엑셀에서 열 이름이 다를 때를 대비한 후보 목록
COLUMN_ALIASES: dict[str, list[str]] = {
    "의결권대상법인":     ["의결권대상법인", "대상법인", "회사명", "법인명", "발행회사", "기업명"],
    "시장구분":          ["시장구분", "시장", "소속부", "상장시장", "거래소"],
    "관계":              ["관계"],
    "주주총회일자":      ["주주총회일자", "주총일", "주주총회일", "총회일자", "의결권행사일", "주주총회 일자"],
    "의안번호":          ["의안번호", "안건번호", "의안 번호", "번호"],
    "의안유형":          ["의안유형", "의안 종류", "안건유형", "의안분류", "유형"],
    "의안명":            ["의안명", "의안내용", "안건명", "의안", "의안 명"],
    "보유주식수(주)":    ["보유주식수(주)", "보유주식수", "보유주식수(단위:주)", "보유주식", "보유 주식수"],
    "지분비율(%)":       ["지분비율(%)", "지분비율", "보유비율", "지분율", "지분율(%)"],
    "찬성주식수":        ["찬성주식수", "찬성", "찬성 주식수", "찬성주식", "찬성(주)"],
    "반대주식수":        ["반대주식수", "반대", "반대 주식수", "반대주식", "반대(주)"],
    "불행사주식수":      ["불행사주식수", "불행사", "불행사 주식수", "불행사주식", "불행사(주)"],
    "중립행사주식수":    ["중립행사주식수", "중립", "중립행사", "중립 행사 주식수", "중립주식수", "기권", "중립(주)"],
    "행사 및 불행사 사유": ["행사 및 불행사 사유", "사유", "행사사유", "불행사사유",
                            "행사불행사사유", "의결권행사사유", "행사 및\n불행사 사유"],
}

KIND_BASE = "https://kind.krx.co.kr"

# ── 진행상태 파일 ─────────────────────────────────────────────────────────────

def write_progress(progress_file: str, status: str, percent: int,
                   message: str, current: int = 0, total: int = 0) -> None:
    try:
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(
                {"status": status, "percent": percent, "message": message,
                 "current": current, "total": total},
                f, ensure_ascii=False,
            )
    except Exception:
        pass


# ── 엑셀 정규화 ───────────────────────────────────────────────────────────────

def _clean_col(c: str) -> str:
    """열 이름에서 개행·탭·연속 공백을 정리한다."""
    return re.sub(r"[\n\r\t]+", " ", str(c)).strip()


def _find_col(df: pd.DataFrame, target: str) -> str | None:
    """COLUMN_ALIASES를 사용해 df에서 target에 대응하는 열 이름을 반환."""
    candidates = COLUMN_ALIASES.get(target, [target])

    # 1차: 정확 일치
    for cand in candidates:
        if cand in df.columns:
            return cand

    # 2차: 공백 무시 부분 일치
    for col in df.columns:
        col_ns = re.sub(r"\s+", "", col)
        for cand in candidates:
            cand_ns = re.sub(r"\s+", "", cand)
            if cand_ns == col_ns or cand_ns in col_ns or col_ns in cand_ns:
                return col

    return None


def normalize_excel(df: pd.DataFrame) -> pd.DataFrame:
    """원본 DataFrame → TARGET_COLUMNS 기준 정규화 DataFrame."""
    df = df.copy()
    df.columns = [_clean_col(c) for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)

    # 헤더가 데이터 안에 숨어있는 경우 탐색 (Unnamed 열이 많을 때)
    if sum("Unnamed" in str(c) for c in df.columns) >= len(df.columns) // 2:
        header_kws = ["의결권", "의안", "찬성", "반대", "주주총회", "법인"]
        for i in range(min(8, len(df))):
            row_str = " ".join(str(v) for v in df.iloc[i].values)
            if any(kw in row_str for kw in header_kws):
                df.columns = [_clean_col(str(v)) for v in df.iloc[i].values]
                df = df.iloc[i + 1:].reset_index(drop=True)
                break

    out = pd.DataFrame()
    for target in TARGET_COLUMNS:
        col = _find_col(df, target)
        out[target] = df[col].reset_index(drop=True) if col else ""

    # 의결권대상법인이 비어있는 행 제거
    mask = out["의결권대상법인"].astype(str).str.strip().isin(
        ["", "nan", "None", "의결권대상법인", "NaN"]
    )
    return out[~mask].reset_index(drop=True)


def read_kind_excel(filepath: str) -> pd.DataFrame:
    """KIND 다운로드 엑셀(xls/xlsx)을 읽어 정규화된 DataFrame 반환."""
    collected: list[pd.DataFrame] = []
    header_kws = ["의결권", "의안", "찬성", "반대", "주주총회"]

    try:
        xls = pd.ExcelFile(filepath)
        for sheet in xls.sheet_names:
            for hdr in range(10):       # 헤더 행 위치를 0~9행까지 탐색
                try:
                    df = pd.read_excel(filepath, sheet_name=sheet,
                                       header=hdr, dtype=str)
                    cols_str = " ".join(str(c) for c in df.columns)
                    if any(kw in cols_str for kw in header_kws):
                        normed = normalize_excel(df)
                        if len(normed) > 0:
                            collected.append(normed)
                        break
                except Exception:
                    continue
    except Exception as e:
        print(f"  [엑셀 읽기 오류] {filepath}: {e}")

    return (pd.concat(collected, ignore_index=True)
            if collected else pd.DataFrame(columns=TARGET_COLUMNS))


# ── Playwright 헬퍼 ───────────────────────────────────────────────────────────

def _fmt_dot(iso_date: str) -> str:
    """YYYY-MM-DD → YYYY.MM.DD"""
    return iso_date.replace("-", ".")


def _fill_date(page, iso_date: str, is_start: bool) -> bool:
    """날짜 입력 필드를 탐색해 값을 채운다. 성공하면 True."""
    fmt_dot   = _fmt_dot(iso_date)
    fmt_plain = iso_date.replace("-", "")

    name_cands = (
        ["searchStartDate", "fromDate", "startDate", "startDt", "dateFrom", "strtDt"]
        if is_start else
        ["searchEndDate", "toDate", "endDate", "endDt", "dateTo", "endDt"]
    )

    for name in name_cands:
        for sel in [f"input[name='{name}']", f"#{name}"]:
            try:
                loc = page.locator(sel)
                if loc.count() == 0:
                    continue
                el = loc.first
                # readonly 속성이면 JS로 강제 주입
                if el.get_attribute("readonly") is not None:
                    page.evaluate(
                        f"""document.querySelector("{sel}").value = "{fmt_dot}";"""
                    )
                else:
                    el.triple_click()
                    el.fill(fmt_dot)
                print(f"    날짜 입력: {sel} = {fmt_dot}")
                return True
            except Exception:
                continue

    # fallback: placeholder·name에 날짜 키워드가 있는 text input 탐색
    try:
        for inp in page.locator("input[type='text']").all():
            ph = (inp.get_attribute("placeholder") or "").lower()
            nm = (inp.get_attribute("name") or "").lower()
            kw = ["from", "start", "시작", "from"] if is_start else ["to", "end", "종료"]
            if any(k in ph + nm for k in kw):
                inp.triple_click()
                inp.fill(fmt_dot)
                return True
    except Exception:
        pass

    return False


def _click_search(page) -> bool:
    """검색 버튼을 클릭한다. 성공하면 True."""
    for sel in [
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('조회')",
        "button:has-text('검색')",
        "a:has-text('조회')",
        "a:has-text('검색')",
        ".btnSearch",
        ".btn-search",
        "#btnSearch",
        "input[value='조회']",
        "input[value='검색']",
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click()
                return True
        except Exception:
            continue
    return False


def _collect_links(page) -> list[dict]:
    """현재 검색결과 페이지에서 공시 항목(href/onclick 문자열)을 수집한다."""
    items: list[dict] = []
    try:
        for row in page.locator("table tbody tr").all():
            tds = row.locator("td").all()
            if len(tds) < 2:
                continue
            link = row.locator("a").first
            if link.count() == 0:
                continue

            href    = link.get_attribute("href") or ""
            onclick = link.get_attribute("onclick") or ""
            text    = link.inner_text().strip()

            if not text and not href and not onclick:
                continue

            # 절대 URL 구성
            full_href = ""
            if href and href not in ("#", "") and not href.startswith("javascript:"):
                if href.startswith("http"):
                    full_href = href
                elif href.startswith("/"):
                    full_href = KIND_BASE + href
                else:
                    full_href = KIND_BASE + "/" + href

            # onclick에서 rcpNo 추출 시도 (예: fn_searchDetail('20240301900001', ...))
            rcp_no = ""
            m = re.search(r"['\"](\d{14})['\"]", onclick)
            if m:
                rcp_no = m.group(1)

            # 회사명: 링크 텍스트 혹은 가장 긴 td
            company = text
            if not company:
                for td in tds:
                    t = td.inner_text().strip()
                    if len(t) > 2 and not t.isdigit():
                        company = t
                        break

            items.append({
                "company":  company,
                "href":     full_href,
                "onclick":  onclick,
                "rcp_no":   rcp_no,
                "text":     text,
            })
    except Exception as e:
        print(f"  [링크 수집 오류] {e}")

    return items


def _try_next_page(page) -> bool:
    """다음 페이지 버튼을 클릭. 없거나 disabled면 False 반환."""
    selectors = [
        "a.next:not(.disabled)",
        ".paging a.next",
        ".pagination a:has-text('다음')",
        "a:has-text('다음 페이지')",
        "a[title='다음']",
        "a img[alt='다음']",
        ".pgBtn.next",
        "#paging .next",
        "a.pNext",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            cls = loc.first.get_attribute("class") or ""
            if any(w in cls for w in ["disabled", "dim", "inactive", "off"]):
                return False
            loc.first.click()
            time.sleep(1.5)
            page.wait_for_load_state("domcontentloaded")
            return True
        except Exception:
            continue
    return False


def _find_attach_link(pg):
    """'의결권 행사 및 불행사 세부내용' 첨부 링크를 반환. 없으면 None."""
    patterns = [
        "의결권 행사 및 불행사 세부내용",
        "의결권행사및불행사세부내용",
        "의결권 행사 및 불행사",
        "행사 및 불행사 세부내용",
        "세부내용",
    ]
    for pat in patterns:
        loc = pg.locator(f"a:has-text('{pat}')")
        if loc.count() > 0:
            return loc.first
    return None


def _find_excel_link(pg):
    """엑셀 다운로드 링크를 반환. 없으면 None."""
    for sel in [
        "a:has-text('엑셀')",
        "a:has-text('EXCEL')",
        "a:has-text('Excel')",
        "a[href$='.xls']",
        "a[href$='.xlsx']",
        "a[href*='excel']",
        "a[href*='Excel']",
        ".btn-excel",
        "#btnExcel",
        "button:has-text('엑셀')",
    ]:
        try:
            loc = pg.locator(sel)
            if loc.count() > 0:
                return loc.first
        except Exception:
            continue
    return None


def _open_detail(ctx, item: dict):
    """공시 상세 페이지를 새 탭으로 열어 반환. 실패하면 None."""
    href    = item.get("href", "")
    onclick = item.get("onclick", "")
    rcp_no  = item.get("rcp_no", "")

    # href가 유효하면 직접 이동
    if href:
        pg = ctx.new_page()
        pg.set_default_timeout(30000)
        try:
            pg.goto(href, wait_until="domcontentloaded", timeout=30000)
            time.sleep(1)
            return pg
        except Exception as e:
            print(f"    [상세 열기 오류] href={href}: {e}")
            pg.close()

    # rcpNo가 있으면 알려진 패턴 URL 시도
    if rcp_no:
        for url_tmpl in [
            f"{KIND_BASE}/disclosure/disclosurebyvote.do?method=viewDetail&rcp_no={rcp_no}",
            f"{KIND_BASE}/disclosure/disclosurebyvote.do?method=searchDisclosurebyVoteDetail&rcp_no={rcp_no}",
        ]:
            pg = ctx.new_page()
            pg.set_default_timeout(30000)
            try:
                pg.goto(url_tmpl, wait_until="domcontentloaded", timeout=30000)
                time.sleep(1)
                # 페이지에 내용이 있으면 성공
                if len(pg.content()) > 500:
                    return pg
                pg.close()
            except Exception:
                pg.close()
                continue

    return None


def _process_one(ctx, item: dict, dl_dir: Path, idx: int) -> pd.DataFrame | None:
    """단일 공시를 처리해 정규화된 DataFrame 또는 None을 반환."""
    company = item.get("company", "")
    detail_pg = None
    body_pg   = None

    try:
        # ── 1. 상세 페이지 열기 ────────────────────────────────────────────
        detail_pg = _open_detail(ctx, item)
        if detail_pg is None:
            print(f"    [{idx}] {company}: 상세 페이지 열기 실패")
            return None

        # ── 2. 첨부서류 링크 클릭 ─────────────────────────────────────────
        attach = _find_attach_link(detail_pg)
        if attach is None:
            print(f"    [{idx}] {company}: 첨부서류(세부내용) 없음")
            return None

        try:
            with ctx.expect_page(timeout=15000) as pw:
                attach.click()
            body_pg = pw.value
            body_pg.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            # 팝업이 아닌 경우 현재 탭에서 처리
            attach.click()
            time.sleep(2)
            body_pg = detail_pg

        time.sleep(1.5)

        # ── 3. 엑셀 링크 다운로드 ─────────────────────────────────────────
        excel_link = _find_excel_link(body_pg)
        if excel_link is None:
            print(f"    [{idx}] {company}: 엑셀 링크 없음")
            return None

        ext_suffix = ".xlsx"
        try:
            with body_pg.expect_download(timeout=60000) as dl_info:
                excel_link.click()
            download = dl_info.value
            fname = download.suggested_filename or f"kind_{idx}.xlsx"
            if fname.lower().endswith(".xls") and not fname.lower().endswith(".xlsx"):
                ext_suffix = ".xls"
            save_path = dl_dir / f"kind_{idx}_{int(time.time())}{ext_suffix}"
            download.save_as(str(save_path))
            print(f"    [{idx}] {company}: 다운로드 완료 → {save_path.name}")
        except Exception as e:
            print(f"    [{idx}] {company}: 다운로드 오류 → {e}")
            return None

        # ── 4. 파싱 ───────────────────────────────────────────────────────
        df = read_kind_excel(str(save_path))
        if len(df) > 0:
            print(f"    [{idx}] {company}: {len(df)}행 추출")
        return df if len(df) > 0 else None

    except Exception as e:
        print(f"    [{idx}] {company}: 처리 오류 → {e}")
        return None

    finally:
        for pg in [body_pg, detail_pg]:
            if pg is not None:
                try:
                    pg.close()
                except Exception:
                    pass


# ── 메인 스크랩 함수 ──────────────────────────────────────────────────────────

def scrape_kind_votes(start_date: str, end_date: str,
                      progress_file: str) -> list[pd.DataFrame]:
    """
    KIND 의결권행사공시를 스크랩해 DataFrame 목록을 반환한다.

    start_date / end_date: 'YYYY-MM-DD' 형식
    """
    start_fmt = _fmt_dot(start_date)
    end_fmt   = _fmt_dot(end_date)
    results: list[pd.DataFrame] = []
    dl_dir = Path(tempfile.mkdtemp(prefix="kind_dl_"))

    write_progress(progress_file, "running", 5, "KIND 브라우저 시작 중...")

    with sync_playwright() as pw_inst:
        browser = pw_inst.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1280,900",
            ],
        )
        ctx = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        main_pg = ctx.new_page()
        main_pg.set_default_timeout(30000)

        try:
            # ── 검색 ──────────────────────────────────────────────────────
            write_progress(progress_file, "running", 8,
                           f"KIND 페이지 로딩 중 ({start_fmt} ~ {end_fmt})...")
            main_pg.goto(KIND_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)

            # 날짜 입력
            ok_s = _fill_date(main_pg, start_date, is_start=True)
            ok_e = _fill_date(main_pg, end_date,   is_start=False)
            if not ok_s or not ok_e:
                print("  [경고] 날짜 입력 실패 → URL 파라미터로 재시도")
                s_plain = start_date.replace("-", "")
                e_plain = end_date.replace("-", "")
                main_pg.goto(
                    f"{KIND_URL}&searchStartDate={s_plain}&searchEndDate={e_plain}",
                    wait_until="domcontentloaded", timeout=60000,
                )
                time.sleep(2)

            _click_search(main_pg)
            time.sleep(2)
            main_pg.wait_for_load_state("domcontentloaded")

            # ── 전체 목록 수집 (페이지네이션) ─────────────────────────────
            write_progress(progress_file, "running", 12, "검색 결과 수집 중...")
            all_items: list[dict] = []
            p_num = 1
            while True:
                rows = _collect_links(main_pg)
                print(f"  [페이지 {p_num}] {len(rows)}건")
                all_items.extend(rows)
                if not rows or not _try_next_page(main_pg):
                    break
                p_num += 1

            total = len(all_items)
            write_progress(progress_file, "running", 15,
                           f"총 {total}건 공시 처리 시작...", 0, total)
            print(f"  [합계] 총 {total}건 발견")

            # ── 개별 공시 처리 ─────────────────────────────────────────────
            for idx, item in enumerate(all_items, 1):
                pct = 15 + int((idx / max(total, 1)) * 82)
                write_progress(progress_file, "running", pct,
                               f"처리 중 ({idx}/{total}): {item.get('company','')}",
                               idx, total)
                df = _process_one(ctx, item, dl_dir, idx)
                if df is not None and len(df) > 0:
                    results.append(df)
                time.sleep(0.4)

        finally:
            browser.close()

    try:
        shutil.rmtree(str(dl_dir), ignore_errors=True)
    except Exception:
        pass

    return results


# ── 진입점 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="KIND 의결권행사공시 → 엑셀 변환기"
    )
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
            # 완전히 비어있는 열 제거
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
