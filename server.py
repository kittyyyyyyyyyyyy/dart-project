from fastapi import FastAPI
from fastapi.responses import FileResponse
import subprocess
import os

app = FastAPI()

FILE_NAME = "all_table_1_2_2.xlsx"

@app.get("/")
def root():
    return {"message": "서버 실행 중"}

@app.get("/download")
def download_excel():
    print("엑셀 생성 시작")

    # 기존 파일 삭제
    if os.path.exists(FILE_NAME):
        os.remove(FILE_NAME)

    # 기존 코드 실행
    subprocess.run(["python", "extract_all_cg_tables.py"])

    # 파일 확인
    if not os.path.exists(FILE_NAME):
        return {"error": "엑셀 생성 실패"}

    print("엑셀 생성 완료")

    return FileResponse(
        path=FILE_NAME,
        filename=FILE_NAME,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )