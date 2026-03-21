from fastapi import FastAPI
from fastapi.responses import FileResponse
import subprocess
import os

app = FastAPI()

FILE_NAME = "all_table_1_2_2.xlsx"

# 1️⃣ 기본 확인용 (서버 살아있는지)
@app.get("/")
def root():
    return {"message": "server running"}

# 2️⃣ 웹페이지 (버튼 화면)
@app.get("/page")
def page():
    return FileResponse("index.html")

# 3️⃣ 엑셀 다운로드 API
@app.get("/download")
def download_excel():
    # 기존 파일 있으면 삭제
    if os.path.exists(FILE_NAME):
        os.remove(FILE_NAME)

    # 데이터 생성 실행
    subprocess.run(["python3", "extract_all_cg_tables.py"], check=True)

    # 생성 실패 체크
    if not os.path.exists(FILE_NAME):
        return {"error": "excel generation failed"}

    # 파일 다운로드
    return FileResponse(
        path=FILE_NAME,
        filename=FILE_NAME,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )