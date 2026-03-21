import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd

API_KEY = "6407c0f8de0092ecfb83f23d01c12da28f7cfd61"

url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}"

response = requests.get(url)

# ZIP 파일 풀기
z = zipfile.ZipFile(io.BytesIO(response.content))
z.extractall("corp_data")

# XML 읽기
tree = ET.parse("corp_data/CORPCODE.xml")
root = tree.getroot()

data = []

for company in root.findall('list'):
    corp_code = company.find('corp_code').text
    corp_name = company.find('corp_name').text
    stock_code = company.find('stock_code').text

    if stock_code.strip() != "":
        data.append([corp_code, corp_name, stock_code])

# 데이터프레임 생성
df = pd.DataFrame(data, columns=["corp_code", "corp_name", "stock_code"])

# 엑셀 저장
df.to_excel("companies.xlsx", index=False)

print("완료! companies.xlsx 생성됨")