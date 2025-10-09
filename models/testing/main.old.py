from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Any, Dict
import uvicorn
import time
import sys
import json

app = FastAPI(title="LLM Test Service", version="0.1")

class InferenceRequest(BaseModel):
    request_id: Optional[str] = None
    # допускаем любой payload, но основной кейс — text
    text: Optional[str] = None
    # любой дополнительный словарь
    payload: Optional[Dict[str, Any]] = None

class InferenceResponse(BaseModel):
    request_id: str
    received: bool = True
    note: Optional[str] = None

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/api/v1/infer", response_model=InferenceResponse)
async def infer(req: InferenceRequest, request: Request):
    """
    Простая точка приёма: печатает в stdout весь JSON и возвращает ack.
    """
    rid = req.request_id or f"auto-{int(time.time()*1000)}"
    # Формируем лог-строку — печатаем в stdout (docker/compose покажет это в logs)
    try:
        incoming = await request.json()
    except Exception:
        # Если тело не JSON — возвращаем 400
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Печать в консоль — удобнее следить в docker logs
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{ts}] Received /api/v1/infer request_id={rid} payload={json.dumps(incoming, ensure_ascii=False)}", flush=True, file=sys.stdout)

    # Можно тут сделать любую обработку — сейчас просто ACK
    return InferenceResponse(request_id=rid, received=True, note="printed to stdout")
