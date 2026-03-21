import requests
import pandas as pd
import time

import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 파일을 확인하세요.")

def get_reports(start, end, corp_cls):
    url = "https://opendart.fss.or.kr/api/list.json"
    page_no = 1
    all_results = []

    while True:
        params = {
            "crtfc_key": API_KEY,
            "bgn_de": start,
            "end_de": end,
            "corp_cls": corp_cls,   # Y=코스피, K=코스닥
            "page_no": page_no,
            "page_count": 100
        }

        res = requests.get(url, params=params)
        data = res.json()

        status = data.get("status")
        items = data.get("list", [])

        print(f"[{corp_cls}] {start}~{end} / page {page_no} / 건수 {len(items)}")

        if status != "000" or len(items) == 0:
            break

        filtered_count = 0

        for item in items:
            report_nm = item.get("report_nm", "")
            if "기업지배구조보고서" in report_nm:
                all_results.append(item)
                filtered_count += 1

        print("현재 페이지 필터 후 건수:", filtered_count)
        print("-" * 40)

        if len(items) < 100:
            break

        page_no += 1
        time.sleep(0.2)

    return all_results


all_results = []

periods = [
    ("20250101", "20250331"),
    ("20250401", "20250630"),
    ("20250701", "20250930"),
    ("20251001", "20251231"),
]

for start, end in periods:
    all_results += get_reports(start, end, "Y")
    all_results += get_reports(start, end, "K")

df = pd.DataFrame(all_results)
print("최종 저장 건수:", len(df))

df.to_excel("cg_reports.xlsx", index=False)
df.to_csv("cg_reports.csv", index=False, encoding="utf-8-sig")

print("완료! cg_reports.xlsx 생성됨")