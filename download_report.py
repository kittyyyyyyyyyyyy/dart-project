import requests
import zipfile
import io
import os

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DART_API_KEY")

if not API_KEY:
    raise ValueError("DART_API_KEY가 없습니다. .env 파일을 확인하세요.")

RCEPT_NO = "20250602800223"

def download_report(rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"

    params = {
        "crtfc_key": API_KEY,
        "rcept_no": rcept_no
    }

    response = requests.get(url, params=params)

    folder_name = f"report_{rcept_no}"
    os.makedirs(folder_name, exist_ok=True)

    try:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(folder_name)
        print(f"완료: {folder_name} 폴더 생성됨")
    except zipfile.BadZipFile:
        print("ZIP 파일이 아닙니다. 응답을 확인하세요.")

download_report(RCEPT_NO)