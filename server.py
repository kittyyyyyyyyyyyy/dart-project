from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import subprocess
import os

app = FastAPI()

FILE_NAME = "all_table_1_2_2.xlsx"

@app.get("/")
def root():
    return {"message": "server running"}

@app.get("/page")
def page():
    return FileResponse("index.html")

@app.get("/download")
def download_excel():
    try:
        # 기존 파일 삭제
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)

        # 데이터 생성 실행
        result = subprocess.run(
            ["python3", "extract_all_cg_tables.py"],
            capture_output=True,
            text=True
        )

        # 실행 실패 시 에러 내용 반환
        if result.returncode != 0:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "extract_all_cg_tables.py 실행 실패",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            )

        # 파일 생성 확인
        if not os.path.exists(FILE_NAME):
            return JSONResponse(
                status_code=500,
                content={
                    "error": "엑셀 파일 생성 실패",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            )

        return FileResponse(
            path=FILE_NAME,
            filename=FILE_NAME,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )