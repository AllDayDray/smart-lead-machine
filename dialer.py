import email
import os
import re
import time
import uuid
import requests
from datetime import datetime, timezone

import pytz
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

import smart_lead_machine


# ------------------------
# ENV + CONFIG
# ------------------------
load_dotenv()

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

DEMO_SHEET_ID = os.getenv("GOOGLE_DEMO_SHEET_ID")
DEMO_WORKSHEET_NAME = os.getenv("GOOGLE_DEMO_WORKSHEET_NAME", "LeeWave Demo Leads")

DIALER_SHEET_ID = os.getenv("GOOGLE_DIALER_SHEET_ID")
DIALER_WORKSHEET_NAME = os.getenv("GOOGLE_DIALER_WORKSHEET_NAME", "LeeWave Dialer")


RETELL_API_KEY = os.getenv("RETELL_API_KEY")
RETELL_AGENT_ID = os.getenv("RETELL_AGENT_ID")
RETELL_FROM_NUMBER = os.getenv("RETELL_FROM_NUMBER")

RETELL_CREATE_CALL_URL = "https://api.retellai.com/v2/create-phone-call"


# ------------------------
# HELPERS
# ------------------------
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def norm_header(s: str) -> str:
    """Normalize sheet headers: 'Do Not Call' -> 'do_not_call'."""
    return str(s).strip().lower().replace(" ", "_")


def normalize_phone(raw) -> str:
    """
    Normalize to E.164.
    Accepts:
        - +13032463246 (already E.164)
        - 303-246-3246
        - (303) 246-3246
        - 13032463246
    """
    if raw is None:
        return ""

    s = str(raw).strip()
    if not s:
        return ""

    # Handle Google Sheets numeric quirks like 13032463246.0
    if s.endswith(".0"):
        s = s[:-2]

    # Already E.164-ish
    if s.startswith("+"):
        if re.fullmatch(r"\+[1-9]\d{7,14}", s):
            return s
        return ""

    digits = re.sub(r"\D", "", s)

    # US fallback
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits

    return ""


def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)


def retell_create_call(to_number: str, lead_id: str, lead_email: str):
    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "from_number": RETELL_FROM_NUMBER,
        "to_number": to_number,
        "agent_id": RETELL_AGENT_ID,
        "retell_llm_dynamic_variables": {
            "lead_id": lead_id,
            "flow_type": "outbound",
            "email": lead_email or "",
        },
        "metadata": {
            "lead_id": lead_id,
            "email": lead_email or "",
            "source": "smart_lead_machine",
            "flow_type": "outbound",
        },
    }

    print("=== OUTBOUND RETELL REQUEST ===")
    print(payload)

    r = requests.post(
        RETELL_CREATE_CALL_URL,
        headers=headers,
        json=payload,
        timeout=30,
    )
    print("RETELL STATUS:", r.status_code)
    print("RETELL BODY:", r.text)
    r.raise_for_status()
    return r.json()


def promote_queued_rows(all_values, headers, hm, ws, max_to_ready):
    """
    Promote up to max_to_ready rows from QUEUE -> READY
    and set next_action to CALL if blank.
    """
    promoted = 0

    for i in range(1, len(all_values)):
        if promoted >= max_to_ready:
            break

        sheet_row = i + 1
        row_vals = all_values[i]
        row = {
            headers[j]: (row_vals[j] if j < len(row_vals) else "")
            for j in range(len(headers))
        }

        dnc = str(row.get("do_not_call") or "").strip().upper()
        if dnc == "TRUE":
            continue

        status = str(row.get("status") or "").strip().upper()
        next_action = str(row.get("next_action") or "").strip().upper()

        attempts_raw = row.get("call_attempts") or "0"
        try:
            attempts = int(float(str(attempts_raw).strip() or "0"))
        except ValueError:
            attempts = 0

        if attempts >= 3:
            continue

        if status == "QUEUE":
            updates = {}

            if "status" in hm:
                updates["status"] = "READY"

            if "next_action" in hm:
                updates["next_action"] = "CALL"

            batch_write_cells(ws, sheet_row, hm, updates)
            promoted += 1
            print(f"PROMOTED ROW {sheet_row}: QUEUE -> READY")

    return promoted


# ------------------------
# MAIN DIALER LOOP
# ------------------------


def main(limit_per_run=10, sleep_between_calls=0.6):
    if not all(
        [DIALER_SHEET_ID, RETELL_API_KEY, RETELL_AGENT_ID, RETELL_FROM_NUMBER]
    ):
        raise SystemExit("❌ Missing env vars. Check .env")

    gc = get_gspread_client()

    # 🔍 DEBUG: prove which spreadsheet + worksheet is being opened
    print("DIALER_SHEET_ID:", repr(DIALER_SHEET_ID))
    print("WORKSHEET_NAME:", repr(DIALER_WORKSHEET_NAME))
    print("SERVICE_ACCOUNT_JSON:", repr(SERVICE_ACCOUNT_JSON))
    sh = gc.open_by_key(DIALER_SHEET_ID)

    print("SPREADSHEET TITLE:", sh.title)
    print("WORKSHEETS:", [w.title for w in sh.worksheets()])
    print("ENV WORKSHEET_NAME:", DIALER_WORKSHEET_NAME)

    ws = sh.worksheet(DIALER_WORKSHEET_NAME)
    print("OPENED WORKSHEET TITLE:", ws.title)

    # Pull all values so we fully control headers + row parsing
    all_values = ws.get_all_values()
    if not all_values or len(all_values) < 2:
        print("❌ Sheet has no data rows.")
        return

    raw_headers = all_values[0]
    headers = [norm_header(h) for h in raw_headers]

    # Map normalized header -> 1-based column index (for update_cell)
    hm = {headers[i]: i + 1 for i in range(len(headers)) if headers[i]}
    print("HEADERS (normalized):", list(hm.keys()))

    ready_now = 0
    for i in range(1, len(all_values)):
        row_vals = all_values[i]
        row = {
            headers[j]: (row_vals[j] if j < len(row_vals) else "")
            for j in range(len(headers))
        }
        status = str(row.get("status") or "").strip().upper()
        next_action = str(row.get("next_action") or "").strip().upper()
        if status == "READY" and next_action == "CALL":
            ready_now += 1

    limit_per_run = int(limit_per_run)
    slots_to_fill = max(0, limit_per_run - ready_now)

    if slots_to_fill > 0:
        promoted = promote_queued_rows(all_values, headers, hm, ws, slots_to_fill)
        print(f"PROMOTED {promoted} QUEUED ROW(S)")
        all_values = ws.get_all_values()

    started = 0

    # Iterate data rows (sheet row index starts at 2)
    for i in range(1, len(all_values)):
        if started >= limit_per_run:
            break

        sheet_row = i + 1  # because all_values[0] is header; sheet rows are 1-based

        row_vals = all_values[i]
        row = {
            headers[j]: (row_vals[j] if j < len(row_vals) else "")
            for j in range(len(headers))
        }

        # DNC check
        dnc = str(row.get("do_not_call") or "").strip().upper()
        if dnc == "TRUE":
            continue

        status = str(row.get("status") or "").strip().upper()
        next_action = str(row.get("next_action") or "").strip().upper()

        flow_type = str(row.get("flow_type") or "").strip().lower()

        # Never let the outbound dialer touch inbound/demo website leads
        if flow_type and flow_type != "outbound":
            continue

        # 🔒 SAFETY NET: only dial explicitly queued rows
        if next_action != "CALL":
            continue

        # 🔒 STATUS GATE: only READY rows can be dialed
        if status != "READY":
            continue

        # attempts + cooldown
        max_attempts = 3
        cooldown_minutes = 60

        attempts_raw = row.get("call_attempts") or "0"
        try:
            attempts = int(float(str(attempts_raw).strip() or "0"))
        except ValueError:
            attempts = 0

        if attempts >= max_attempts:
            continue

        last_called = str(
            row.get("last_called_at") or row.get("last_call_at") or ""
        ).strip()
        if last_called:
            try:
                dt = datetime.fromisoformat(last_called.replace("Z", "+00:00"))
                if (
                    datetime.now(timezone.utc) - dt
                ).total_seconds() < cooldown_minutes * 60:
                    continue
            except Exception:
                pass

        # -------------------
        # PHONE PICKING RULE:
        # Prefer the E.164-looking value in lead_id (your sheet column B)
        # Otherwise fallback to Phone column (A)
        # -------------------
        raw_phone = row.get("lead_id") or row.get("phone") or ""
        phone = normalize_phone(raw_phone)

        if not phone:
            if "status" in hm:
                ws.update_cell(sheet_row, hm["status"], "BAD_PHONE")
            notes_col = hm.get("notes")
            if notes_col:
                ws.update_cell(sheet_row, notes_col, f"Invalid phone: {raw_phone}")
            else:
                if "status" in hm:
                    ws.update_cell(sheet_row, hm["status"], f"BAD_PHONE ({raw_phone})")
            continue

        # -------------------
        # LEAD ID FOR RETELL:
        # Do NOT overwrite your 'lead_id' column if it contains phone.
        # Use lead_uuid if present; else deterministic fallback.
        # -------------------
        lead_uuid = str(row.get("lead_uuid") or "").strip()
        if not lead_uuid:
            lead_uuid = "lead_" + uuid.uuid4().hex[:12]
            if "lead_uuid" in hm:
                ws.update_cell(sheet_row, hm["lead_uuid"], lead_uuid)

        retell_lead_id = lead_uuid if lead_uuid else f"row_{sheet_row}"

        lead_email = (
            str(row.get("email_primary") or row.get("email") or "").strip().lower()
        )

        # Business hours gating (optional)
        tz = str(row.get("timezone") or "").strip() or "America/Los_Angeles"
        hours = str(row.get("business_hours") or "").strip() or "9-17"
        try:
            start_h, end_h = map(int, hours.split("-"))
        except Exception:
            start_h, end_h = 9, 17

        try:
            now = datetime.now(pytz.timezone(tz))
            if not (start_h <= now.hour < end_h):
                continue
        except Exception as e:
            print("BUSINESS HOURS CHECK ERROR:", e)

        try:
            print(
                f"CALLING → raw={raw_phone} normalized={phone} lead_id={retell_lead_id}"
            )

            resp = retell_create_call(phone, retell_lead_id, lead_email)
            call_id = resp.get("call_id") or resp.get("call", {}).get("call_id")

            if "status" in hm:
                ws.update_cell(sheet_row, hm["status"], "CALL_STARTED")
            if "next_action" in hm:
                ws.update_cell(sheet_row, hm["next_action"], "WAITING_FOR_ANALYSIS")

            if "call_attempts" in hm:
                ws.update_cell(sheet_row, hm["call_attempts"], attempts + 1)
            if call_id and "last_klaviyo_call_id" in hm:
                ws.update_cell(sheet_row, hm["last_klaviyo_call_id"], call_id)
            if "last_called_at" in hm:
                ws.update_cell(sheet_row, hm["last_called_at"], utc_now_iso())

            started += 1
            time.sleep(sleep_between_calls)

        except Exception as e:
            if "status" in hm:
                ws.update_cell(sheet_row, hm["status"], "CALL_ERROR")
            notes_col = hm.get("notes")
            if notes_col:
                ws.update_cell(sheet_row, notes_col, str(e))

    print(f"✅ Dialer finished. Calls started: {started}")


if __name__ == "__main__":
    limit = int(os.getenv("DIALER_LIMIT_PER_RUN", "20"))
    sleep_between = float(os.getenv("DIALER_SLEEP_BETWEEN_CALLS", "2.0"))

    print("=== STARTING SCHEDULED DIALER ===")
    print("DIALER_LIMIT_PER_RUN:", limit)
    print("DIALER_SLEEP_BETWEEN_CALLS:", sleep_between)

    main(limit_per_run=limit, sleep_between_calls=sleep_between)

    print("=== DIALER RUN COMPLETE ===")
