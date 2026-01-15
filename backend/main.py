from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List
import shutil
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from google.oauth2 import id_token
from google.auth.transport import requests
import uvicorn

from database import get_db, engine
import models
import schemas

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "your-google-client-id")
UPLOAD_FOLDER = Path("[2]_Drop_xlsx_here")
OUTPUT_FOLDER = Path("[4]_output_csv_files")

UPLOAD_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)


def verify_google_token(token: str):
    try:
        idinfo = id_token.verify_oauth2_token(
            token, requests.Request(), GOOGLE_CLIENT_ID
        )
        return idinfo
    except ValueError:
        return None


@app.post("/api/auth/google")
async def google_auth(auth_data: schemas.GoogleAuth, db: Session = Depends(get_db)):
    user_info = verify_google_token(auth_data.token)
    
    if not user_info:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = db.query(models.User).filter(models.User.email == user_info["email"]).first()
    
    if not user:
        user = models.User(
            email=user_info["email"],
            name=user_info.get("name", ""),
            google_id=user_info["sub"]
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    
    return {"user": {"id": user.id, "email": user.email, "name": user.name}}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")
    
    file_path = UPLOAD_FOLDER / file.filename
    
    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    upload_record = models.Upload(
        filename=file.filename,
        upload_date=datetime.now(),
        status="processing"
    )
    db.add(upload_record)
    db.commit()
    db.refresh(upload_record)
    
    return {"message": "File uploaded successfully", "upload_id": upload_record.id}


@app.get("/api/uploads")
async def get_uploads(db: Session = Depends(get_db)):
    uploads = []
    
    if OUTPUT_FOLDER.exists():
        for folder in sorted(OUTPUT_FOLDER.iterdir(), reverse=True):
            if folder.is_dir() and folder.name.startswith("Upload-"):
                upload_id = folder.name.split("-")[1].split("_")[0]
                
                balance_file = folder / "6_layer_output.csv"
                equity_file = folder / "8_layer_output.csv"
                
                uploads.append({
                    "id": upload_id,
                    "folder_name": folder.name,
                    "has_balance": balance_file.exists(),
                    "has_equity": equity_file.exists(),
                    "created_at": datetime.fromtimestamp(folder.stat().st_mtime).isoformat()
                })
    
    return {"uploads": uploads}


@app.get("/api/uploads/{upload_id}/balance")
async def get_balance_data(upload_id: str):
    folder = OUTPUT_FOLDER / f"Upload-{upload_id}_ID"
    csv_file = folder / "6_layer_output.csv"
    
    if not csv_file.exists():
        raise HTTPException(status_code=404, detail="Balance data not found")
    
    df = pd.read_csv(csv_file)
    
    return {
        "columns": df.columns.tolist(),
        "data": df.to_dict(orient="records")
    }


@app.get("/api/uploads/{upload_id}/equity")
async def get_equity_data(upload_id: str):
    folder = OUTPUT_FOLDER / f"Upload-{upload_id}_ID"
    csv_file = folder / "8_layer_output.csv"
    
    if not csv_file.exists():
        raise HTTPException(status_code=404, detail="Equity data not found")
    
    df = pd.read_csv(csv_file)
    
    return {
        "columns": df.columns.tolist(),
        "data": df.to_dict(orient="records")
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)