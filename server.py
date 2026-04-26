print("SERVER FILE LOADED")

import os
import re
from datetime import datetime, timezone
from typing import Dict, Any

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException

# =========================
# ENV
# =========================
load_dotenv()

RETELL_API_KEY = os.getenv("RETELL_API_KEY")
RETELL_AGENT_ID = os.getenv("RETELL_AGENT_ID")
RETELL_FROM_NUMBER = os.getenv("RETELL_FROM_NUMBER")

DEMO_SHEET_ID = os.getenv("GOOGLE_DEMO_SHEET_ID")
DEMO_WORKSHEET_NAME = os.getenv("GOOGLE_DEMO_WORKSHEET_NAME", "LeeWave Demo Leads")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

app = FastAPI()


# =========================
# HELPERS
# =========================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_phone(phone):
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11:
        return f"+{digits}"
    raise HTTPException(status_code=400, detail="Invalid phone")


def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)


def get_ws():
    gc = gs_client()
    return gc.open_by_key(DEMO_SHEET_ID).worksheet(DEMO_WORKSHEET_NAME)


def header_map(ws):
    headers = ws.row_values(1)
    return {h.strip().lower(): i + 1 for i, h in enumerate(headers)}


def append_row(ws, hm, data):
    headers = ws.row_values(1)
    row = [""] * len(headers)
    for k, v in data.items():
        if k in hm:
            row[hm[k] - 1] = str(v)
    ws.append_row(row, value_input_option="RAW")
    return len(ws.col_values(1))


def update_row(ws, hm, row, data):
    updates = []
    for k, v in data.items():
        if k in hm:
            col = hm[k]
            updates.append(
                {"range": gspread.utils.rowcol_to_a1(row, col), "values": [[str(v)]]}
            )
    if updates:
        ws.batch_update(updates)


# =========================
# RETELL
# =========================
def create_call(phone, first_name, last_name, email):
    url = "https://api.retellai.com/v2/create-phone-call"

    payload = {
        "from_number": RETELL_FROM_NUMBER,
        "to_number": phone,
        "override_agent_id": RETELL_AGENT_ID,
        "metadata": {"flow_type": "demo", "email": email},
        "retell_llm_dynamic_variables": {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
        },
    }

    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    print("=== RETELL REQUEST ===")
    print(payload)

    r = requests.post(url, json=payload, headers=headers)

    print("RETELL STATUS:", r.status_code)
    print("RETELL BODY:", r.text)

    r.raise_for_status()
    return r.json()


# =========================
# ROUTE
# =========================
@app.post("/demo-lead")
async def demo_lead(request: Request):
    data = await request.json()

    first = data.get("first_name")
    last = data.get("last_name")
    phone_raw = data.get("phone")
    email = (data.get("email") or "").lower()

    if not first or not phone_raw:
        raise HTTPException(status_code=400, detail="Missing fields")

    phone = normalize_phone(phone_raw)

    ws = get_ws()
    hm = header_map(ws)

    # =========================
    # 1. CREATE ROW
    # =========================
    row = append_row(
        ws,
        hm,
        {
            "first_name": first,
            "last_name": last,
            "phone": phone,
            "lead_id": phone,
            "email_primary": email,
            "source": "LeeWave Demo",
            "status": "CALL_REQUESTED",
            "next_action": "CALL",
            "last_called_at": utc_now_iso(),
        },
    )

    # =========================
    # 2. CALL
    # =========================
    try:
        resp = create_call(phone, first, last, email)
        call_id = resp.get("call_id")

        if not call_id:
            raise Exception("No call_id returned")

        # SUCCESS
        update_row(
            ws,
            hm,
            row,
            {
                "status": "CALL_STARTED",
                "last_klaviyo_call_id": call_id,
                "next_action": "REVIEW",
            },
        )

    except Exception as e:
        print("CALL FAILED:", str(e))

        # FAILURE
        update_row(ws, hm, row, {"status": "CALL_FAILED", "next_action": "RETRY"})

    return {"ok": True}
