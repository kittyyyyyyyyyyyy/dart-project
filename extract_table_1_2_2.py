print("실행 시작")

from bs4 import BeautifulSoup
import pandas as pd
import os

# 여기를 본인 값으로 바꾸세요
FOLDER_NAME = "report_20250602800223"
FILE_NAME = "20250602800223.xml"

file_path = os.path.join(FOLDER_NAME, FILE_NAME)

print("읽는 파일:", file_path)

if not os.path.exists(file_path):
    print("파일이 없습니다. FOLDER_NAME 또는 FILE_NAME을 확인하세요.")
    raise SystemExit

with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

print("파일 길이:", len(content))

soup = BeautifulSoup(content, "lxml")

target_text_node = None

for tag in soup.find_all(string=True):
    text = tag.strip()
    if "표 1-2-2" in text or "주주총회 의결 내용" in text:
        target_text_node = tag
        break

found_rows = []

if target_text_node:
    print("표 제목 텍스트를 찾았습니다.")
    parent = target_text_node.parent
    next_table = parent.find_next("table")

    if next_table:
        print("제목 다음 table을 찾았습니다.")
        rows = next_table.find_all("tr")

        for tr in rows:
            cols = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
            if cols:
                found_rows.append(cols)

if found_rows:
    df = pd.DataFrame(found_rows)
    df.to_excel("table_1_2_2.xlsx", index=False)
    print("완료! table_1_2_2.xlsx 생성됨")
else:
    print("표를 찾지 못했습니다.")
