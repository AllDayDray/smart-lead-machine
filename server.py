# server.py
print("SERVER FILE LOADED - strict email + callback-safe matching v4")

import os
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException

load_dotenv(dotenv_path=".env")

KLAVIYO_PRIVATE_API_KEY = os.getenv("KLAVIYO_PRIVATE_API_KEY", "").strip()
KLAVIYO_LIST_ID = os.getenv("KLAVIYO_LIST_ID", "").strip()

SERVICE_ACCOUNT_JSON = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json"
).strip()

DIALER_SHEET_ID = os.getenv("GOOGLE_DIALER_SHEET_ID", "").strip()
DIALER_WORKSHEET_NAME = os.getenv(
    "GOOGLE_DIALER_WORKSHEET_NAME", "LeeWave Dialer"
).strip()

DEMO_SHEET_ID = os.getenv("GOOGLE_DEMO_SHEET_ID", "").strip()
DEMO_WORKSHEET_NAME = os.getenv(
    "GOOGLE_DEMO_WORKSHEET_NAME", "LeeWave Demo Leads"
).strip()

RETELL_API_KEY = os.getenv("RETELL_API_KEY", "").strip()
RETELL_AGENT_ID = os.getenv("RETELL_AGENT_ID", "").strip()
RETELL_FROM_NUMBER = os.getenv("RETELL_FROM_NUMBER", "").strip()

app = FastAPI()


STATUS_PRIORITY = {
    "": 0,
    "CALL_ENDED": 0,
    "WAITING_FOR_ANALYSIS": 0,
    "CALL_REQUESTED": 0,
    "CALL_STARTED": 0,
    "CALL_FAILED": 0,
    "REVIEW": 1,
    "NO_ANSWER": 2,
    "FOLLOW_UP": 3,
    "CALLBACK": 4,
    "NOT_INTERESTED": 5,
    "NOT INTERESTED": 5,
    "BOOKED": 6,
}

FINAL_STATUSES = {
    "BOOKED",
    "CALLBACK",
    "FOLLOW_UP",
    "NO_ANSWER",
    "NOT_INTERESTED",
    "NOT INTERESTED",
    "WRONG_NUMBER",
    "GATEKEEPER",
}

# =========================
# EMAIL HELPERS
# =========================
TEST_EMAIL_RE = re.compile(r"(^ping@|^test@|@example\.com$)", re.I)
EMAIL_RE = re.compile(
    r"(?<![A-Z0-9._%+-])([A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\.(?:com|net|org|ai|io|co|us|biz|info|dev|me|app|tech|site|online|store|agency|marketing|consulting|design|digital|media|solutions))(?![A-Z0-9._%+-])",
    re.I,
)
BAD_EMAIL_WORDS = [
    "theagent",
    "called",
    "scheduled",
    "appointment",
    "meeting",
    "wednesday",
    "thursday",
    "friday",
    "monday",
    "tuesday",
    "saturday",
    "sunday",
    "pacifictime",
    "thecall",
    "successfully",
    "discussion",
    "collaboration",
    "followup",
    "follow-up",
    "services",
    "outreach",
    "bookingprocesses",
]


def is_test_email(email: Optional[str]) -> bool:
    if not email:
        return False
    return bool(TEST_EMAIL_RE.search(email.strip()))


def is_valid_real_email(email: str) -> bool:
    email = (email or "").strip().lower().strip(".,;:()[]{}<>\"'")
    if not email or "@" not in email or is_test_email(email):
        return False
    if not EMAIL_RE.fullmatch(email):
        return False
    local, _, domain = email.rpartition("@")
    if not local or not domain or "." not in domain:
        return False
    compact = email.replace(".", "").replace("_", "").replace("-", "")
    if any(word.replace("-", "") in compact for word in BAD_EMAIL_WORDS):
        return False
    if len(local) > 40:
        return False
    if any(len(part) > 63 or not part for part in domain.split(".")):
        return False
    return True


def normalize_spoken_email(text: str) -> str:
    if not text:
        return ""

    t = str(text).lower()

    t = (
        t.replace("g-mail", "gmail")
        .replace("g mail", "gmail")
        .replace("gee mail", "gmail")
    )

    t = re.sub(r"\s+\bat\b\s+", "@", t)
    t = re.sub(r"\s+\bdot\b\s+", ".", t)

    # critical fix: remove spaces around email symbols
    t = re.sub(r"\s*@\s*", "@", t)
    t = re.sub(r"\s*\.\s*", ".", t)

    # 🔥 ADD THIS BLOCK ONLY
    # joins spelled-out letters before @
    # "t h e d r a y d e v@gmail.com" → "thedraydev@gmail.com"
    t = re.sub(
        r"((?:\b[a-z]\b\s*){2,})(?=@)",
        lambda m: re.sub(r"\s+", "", m.group(1)),
        t,
    )

    t = re.sub(r"[,;!?()\[\]{}<>\"']", " ", t)

    return t


def extract_all_emails(text: str) -> List[str]:
    if not text:
        return []
    found: List[str] = []
    for match in EMAIL_RE.findall(text):
        email = match.lower().strip(".,;:()[]{}<>\"'")
        if is_valid_real_email(email) and email not in found:
            found.append(email)
    return found


def pick_best_email(text: str) -> str:
    emails = extract_all_emails(text)
    if not emails:
        return ""
    t = (text or "").lower()
    confirm_words = [
        "yes",
        "correct",
        "confirmed",
        "confirm",
        "right",
        "email is",
        "email address is",
    ]
    best = ("", -1)
    for email in emails:
        idx = t.rfind(email)
        window = t[max(0, idx - 140) : idx + 140]
        score = sum(1 for w in confirm_words if w in window) * 10 + idx
        if score > best[1]:
            best = (email, score)
    return best[0]


def clean_found_list(value: str) -> str:
    items = [x.strip().lower() for x in (value or "").split(",") if x.strip()]
    out = []
    seen = set()
    for item in items:
        item = item.strip(".,;:()[]{}<>\"'")
        if not is_valid_real_email(item):
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return ", ".join(out)


def append_unique_email(existing: str, new_email: str) -> str:
    existing_clean = clean_found_list(existing)
    items = [e.strip().lower() for e in existing_clean.split(",") if e.strip()]
    if new_email and is_valid_real_email(new_email) and new_email.lower() not in items:
        items.append(new_email.lower())
    return ", ".join(items)


# =========================
# GENERAL HELPERS
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_phone_e164(phone_raw: str) -> str:
    phone_raw = str(phone_raw or "").strip()
    digits = re.sub(r"\D", "", phone_raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if phone_raw.startswith("+") and digits:
        return f"+{digits}"
    raise HTTPException(status_code=400, detail=f"Invalid phone: {phone_raw}")


def safe_normalize_phone_e164(phone_raw: str) -> str:
    try:
        return normalize_phone_e164(phone_raw)
    except Exception:
        return ""


def norm_header(s: str) -> str:
    return str(s).strip().lower().replace(" ", "_")


def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)


def get_ws(sheet_id: str, worksheet_name: str):
    if not sheet_id:
        raise HTTPException(status_code=500, detail="Missing Google Sheet ID in .env")
    return gs_client().open_by_key(sheet_id).worksheet(worksheet_name)


def header_map_norm(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {norm_header(h): i + 1 for i, h in enumerate(headers) if h}


def ensure_required_columns(
    hm: Dict[str, int], required: List[str], label: str
) -> None:
    missing = [c for c in required if c not in hm]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"{label} sheet missing columns: {', '.join(missing)}",
        )


def batch_write_cells(
    ws, row_num: int, hm: Dict[str, int], values: Dict[str, Any]
) -> None:
    updates = []
    for col_name, val in values.items():
        if col_name not in hm:
            continue
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(row_num, hm[col_name]),
                "values": [[str(val) if val is not None else ""]],
            }
        )
    if updates:
        ws.batch_update(updates, value_input_option="RAW")


def append_row_by_headers(ws, hm: Dict[str, int], values: Dict[str, Any]) -> int:
    headers = ws.row_values(1)
    row = [""] * len(headers)
    for col_name, val in values.items():
        if col_name in hm:
            row[hm[col_name] - 1] = "" if val is None else str(val)
    ws.append_row(row, value_input_option="RAW")
    return len(ws.col_values(1))


def safe_find(ws, value: str, col_index: int):
    if not value or not col_index:
        return None
    try:
        return ws.find(value, in_column=col_index)
    except Exception:
        return None


def find_row_by_call_id_only(ws, hm: Dict[str, int], call_id: str) -> Optional[int]:
    if call_id and "last_klaviyo_call_id" in hm:
        cell = safe_find(ws, call_id, hm["last_klaviyo_call_id"])
        if cell:
            return cell.row
    return None


def find_matching_row_outbound(
    ws, hm: Dict[str, int], call_id: str, lead_id: str, to_number: str
) -> Optional[int]:
    row = find_row_by_call_id_only(ws, hm, call_id)
    if row:
        return row
    for col in ("lead_uuid", "lead_id", "phone"):
        value = lead_id if col != "phone" else to_number
        if value and col in hm:
            cell = safe_find(ws, value, hm[col])
            if cell:
                return cell.row
    return None


def find_matching_row_by_phone_candidates(
    ws, hm: Dict[str, int], phone_candidates: List[str]
) -> Optional[int]:
    for phone in phone_candidates:
        normalized = safe_normalize_phone_e164(phone)
        if not normalized:
            continue

        for col in ("lead_id", "phone"):
            if col in hm:
                cell = safe_find(ws, normalized, hm[col])
                if cell:
                    return cell.row

    return None


def get_sheet_for_flow(flow_type: str):
    if flow_type == "demo":
        return get_ws(DEMO_SHEET_ID, DEMO_WORKSHEET_NAME), "Demo"
    return get_ws(DIALER_SHEET_ID, DIALER_WORKSHEET_NAME), "Dialer"


def first_found(obj, keys):
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key]:
                return obj[key]
        for value in obj.values():
            hit = first_found(value, keys)
            if hit:
                return hit
    elif isinstance(obj, list):
        for value in obj:
            hit = first_found(value, keys)
            if hit:
                return hit
    return ""


def pick_final_status(
    current_status: str,
    new_status: str,
    current_next_action: str,
    new_next_action: str,
):
    current_status = (current_status or "").strip().upper()
    new_status = (new_status or "").strip().upper()

    current_priority = STATUS_PRIORITY.get(current_status, 0)
    new_priority = STATUS_PRIORITY.get(new_status, 0)

    if new_priority >= current_priority:
        return new_status, new_next_action

    return current_status, current_next_action


# =========================
# KLAVIYO
# =========================
def klaviyo_upsert_profile(email: str) -> None:
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")
    if not is_valid_real_email(email):
        raise RuntimeError(f"Invalid email for Klaviyo: {email}")
    url = "https://a.klaviyo.com/api/profile-import"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": "2025-01-15",
    }
    payload = {"data": {"type": "profile", "attributes": {"email": email}}}
    requests.post(url, headers=headers, json=payload, timeout=20).raise_for_status()


def klaviyo_add_to_list(email: str, list_id: str) -> None:
    if not list_id:
        return
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")
    if not is_valid_real_email(email):
        raise RuntimeError(f"Invalid email for Klaviyo list: {email}")
    url = f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": "2025-01-15",
    }
    payload = {"data": [{"type": "profile", "attributes": {"email": email}}]}
    requests.post(url, headers=headers, json=payload, timeout=20).raise_for_status()


def klaviyo_track_call_outcome(email: str, call_outcome: str, props: dict) -> None:
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")
    if not is_valid_real_email(email):
        raise RuntimeError(f"Invalid email for Klaviyo event: {email}")
    url = "https://a.klaviyo.com/api/events"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": "2025-01-15",
    }
    payload = {
        "data": {
            "type": "event",
            "attributes": {
                "metric": {
                    "data": {"type": "metric", "attributes": {"name": "Call Outcome"}}
                },
                "profile": {
                    "data": {"type": "profile", "attributes": {"email": email}}
                },
                "properties": {"call_outcome": call_outcome, **(props or {})},
                "time": utc_now_iso(),
            },
        }
    }
    requests.post(url, headers=headers, json=payload, timeout=20).raise_for_status()


# =========================
# RETELL
# =========================
def create_retell_call_for_demo(
    *,
    first_name: str,
    last_name: str,
    email: str,
    source: str,
    lead_id: str,
    to_number: str,
) -> dict:
    if not RETELL_API_KEY:
        raise HTTPException(status_code=500, detail="Missing RETELL_API_KEY in .env")
    if not RETELL_AGENT_ID:
        raise HTTPException(status_code=500, detail="Missing RETELL_AGENT_ID in .env")
    if not RETELL_FROM_NUMBER:
        raise HTTPException(
            status_code=500, detail="Missing RETELL_FROM_NUMBER in .env"
        )

    payload = {
        "from_number": RETELL_FROM_NUMBER,
        "to_number": to_number,
        "override_agent_id": RETELL_AGENT_ID,
        "metadata": {
            "flow_type": "demo",
            "lead_id": lead_id,
            "source": source,
            "email": email if is_valid_real_email(email) else "",
        },
        "retell_llm_dynamic_variables": {
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}".strip(),
            "email": email if is_valid_real_email(email) else "",
            "source": source,
            "lead_id": lead_id,
            "flow_type": "demo",
        },
    }
    headers = {
        "Authorization": f"Bearer {RETELL_API_KEY}",
        "Content-Type": "application/json",
    }
    print("=== RETELL REQUEST ===")
    print(payload)
    r = requests.post(
        "https://api.retellai.com/v2/create-phone-call",
        headers=headers,
        json=payload,
        timeout=30,
    )
    print("RETELL STATUS:", r.status_code)
    print("RETELL BODY:", r.text)
    r.raise_for_status()
    return r.json()


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "server.py",
        "version": "strict-email-callback-safe-v4",
    }


@app.post("/demo-lead")
async def demo_lead(request: Request):
    data = await request.json()

    first_name = str(data.get("first_name") or "").strip()
    last_name = str(data.get("last_name") or "").strip()
    phone_raw = str(data.get("phone") or "").strip()
    email = str(data.get("email") or "").strip().lower()
    source = str(data.get("source") or "LeeWave Demo").strip()

    if not first_name:
        raise HTTPException(status_code=400, detail="Missing first_name")
    if not phone_raw:
        raise HTTPException(status_code=400, detail="Missing phone")

    phone_e164 = normalize_phone_e164(phone_raw)
    safe_email = email if is_valid_real_email(email) else ""

    ws = get_ws(DEMO_SHEET_ID, DEMO_WORKSHEET_NAME)
    hm = header_map_norm(ws)

    ensure_required_columns(
        hm,
        ["lead_id", "status", "next_action", "last_called_at", "last_klaviyo_call_id"],
        "Demo",
    )

    # =========================
    # LIGHT DUPLICATE BLOCK (optional safety)
    # Prevents accidental rapid double-submit (like button spam)
    # =========================
    phone_digits = re.sub(r"\D", "", phone_e164)

    try:
        rows = ws.get_all_records()

        for idx in range(len(rows) - 1, -1, -1):
            row = rows[idx]

            existing_phone = re.sub(r"\D", "", str(row.get("phone", "")))
            if existing_phone != phone_digits:
                continue

            existing_status = str(row.get("status") or "").strip().upper()
            existing_next_action = str(row.get("next_action") or "").strip().upper()

            if existing_status in {"CALL_REQUESTED", "CALL_STARTED", "CALL_ENDED"}:
                print(
                    "🚫 ACTIVE DEMO CALL ALREADY EXISTS",
                    {
                        "phone": phone_e164,
                        "status": existing_status,
                        "next_action": existing_next_action,
                    },
                )

                return {
                    "ok": True,
                    "skipped": "active_demo_call_exists",
                    "phone": phone_e164,
                    "existing_status": existing_status,
                }

            last_called_raw = str(row.get("last_called_at") or "").strip()
            if not last_called_raw:
                continue

            try:
                last_dt = datetime.fromisoformat(last_called_raw.replace("Z", "+00:00"))

                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)

                seconds_since = (datetime.now(timezone.utc) - last_dt).total_seconds()

                if 0 <= seconds_since < 60:
                    print(
                        "🚫 DUPLICATE DEMO SUBMIT BLOCKED",
                        {
                            "phone": phone_e164,
                            "seconds_since": round(seconds_since, 2),
                        },
                    )

                    return {
                        "ok": True,
                        "skipped": "duplicate_recent_submit",
                        "phone": phone_e164,
                    }

            except Exception as e:
                print("Duplicate check parse skipped:", str(e))

    except Exception as e:
        print("Duplicate check failed, continuing:", str(e))

    # =========================
    # CREATE ONE NEW ROW
    # lead_id stays the same as phone. Do not generate demo IDs.
    # =========================
    row_num = append_row_by_headers(
        ws,
        hm,
        {
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone_e164,
            "lead_id": phone_e164,
            "email_primary": safe_email,
            "emails_found": safe_email,
            "source": source,
            "status": "CALL_REQUESTED",
            "next_action": "WAITING_FOR_RETELL",
            "last_called_at": utc_now_iso(),
            "last_klaviyo_call_id": "",
        },
    )

    print("NEW DEMO ROW:", row_num, phone_e164)

    # =========================
    # CREATE RETELL CALL
    # =========================
    try:
        resp = create_retell_call_for_demo(
            first_name=first_name,
            last_name=last_name,
            email=safe_email,
            source=source,
            lead_id=phone_e164,
            to_number=phone_e164,
        )

        call_id = str(resp.get("call_id") or "").strip()

        if not call_id:
            raise RuntimeError(f"Retell returned no call_id. Response: {resp}")

        batch_write_cells(
            ws,
            row_num,
            hm,
            {
                "status": "CALL_STARTED",
                "next_action": "WAITING_FOR_ANALYSIS",
                "last_called_at": utc_now_iso(),
                "last_klaviyo_call_id": call_id,
                "flow_type": "demo",
                "call_attempts": 1,
            },
        )

        print("RETELL CALL CREATED:", call_id)

    except Exception as e:
        print("RETELL CALL FAILED:", str(e))

        batch_write_cells(
            ws,
            row_num,
            hm,
            {
                "status": "CALL_FAILED",
                "next_action": "REVIEW",
                "last_called_at": utc_now_iso(),
            },
        )

        return {
            "ok": True,
            "row": row_num,
            "lead_id": phone_e164,
            "call_started": False,
        }

    # =========================
    # KLAVIYO PROFILE SYNC
    # =========================
    if safe_email:
        try:
            klaviyo_upsert_profile(safe_email)
            if KLAVIYO_LIST_ID:
                klaviyo_add_to_list(safe_email, KLAVIYO_LIST_ID)
        except Exception as e:
            print("KLAVIYO PROFILE SYNC ERROR:", str(e))

    return {
        "ok": True,
        "row": row_num,
        "lead_id": phone_e164,
        "call_id": call_id,
    }


@app.post("/retell/post-call")
async def retell_post_call(request: Request):
    payload = await request.json()
    event = str(payload.get("event") or payload.get("type") or "").strip()

    if event not in ("call_ended", "call_analyzed"):
        return {"ok": True, "ignored_event": event}

    call = payload.get("call") or {}
    meta = call.get("metadata") or {}
    dyn = call.get("retell_llm_dynamic_variables") or {}
    analysis = call.get("call_analysis") or {}

    print("ANALYSIS KEYS:", analysis.keys())
    print("CUSTOM ANALYSIS:", analysis.get("custom_analysis_data"))
    print("RAW ANALYSIS:", analysis)

    flow_type = (
        str(meta.get("flow_type") or dyn.get("flow_type") or "").strip().lower()
        or "outbound"
    )

    ws, sheet_label = get_sheet_for_flow(flow_type)
    hm = header_map_norm(ws)
    ensure_required_columns(
        hm,
        ["status", "next_action", "last_called_at", "last_klaviyo_call_id", "lead_id"],
        sheet_label,
    )

    call_id = str(call.get("call_id") or payload.get("call_id") or "").strip()
    lead_id = str(
        dyn.get("lead_id") or meta.get("lead_id") or payload.get("lead_id") or ""
    ).strip()
    to_number = str(call.get("to_number") or payload.get("to_number") or "").strip()
    from_number = str(
        call.get("from_number") or payload.get("from_number") or ""
    ).strip()

    summary = str(
        first_found(analysis, ["summary", "analysis_summary"])
        or first_found(call, ["summary", "call_summary", "notes"])
        or first_found(payload, ["summary", "call_summary", "notes"])
        or ""
    ).strip()

    transcript_text = str(
        first_found(
            call,
            [
                "transcript",
                "transcript_text",
                "call_transcript",
                "transcript_with_tool_calls",
                "messages",
                "dialogue",
                "conversation",
            ],
        )
        or first_found(
            payload,
            [
                "transcript",
                "transcript_text",
                "call_transcript",
                "messages",
                "dialogue",
                "conversation",
            ],
        )
        or ""
    ).strip()

    outcome = str(
        first_found(analysis, ["outcome", "disposition", "result", "status"])
        or first_found(
            call, ["call_outcome", "outcome", "disposition", "result", "status"]
        )
        or first_found(
            payload, ["call_outcome", "outcome", "disposition", "result", "status"]
        )
        or ""
    ).strip()

    custom_analysis = analysis.get("custom_analysis_data") or {}
    structured_email = (
        str(
            custom_analysis.get("captured_email")
            or custom_analysis.get("email")
            or custom_analysis.get("email_address")
            or first_found(
                analysis,
                [
                    "captured_email",
                    "email",
                    "email_address",
                    "customer_email",
                    "provided_email",
                ],
            )
            or ""
        )
        .strip()
        .lower()
    )
    meta_email = str(meta.get("email") or "").strip().lower()
    dyn_email = str(dyn.get("email") or "").strip().lower()

    captured_email = ""
    for candidate in (meta_email, dyn_email, structured_email):
        if is_valid_real_email(candidate):
            captured_email = candidate
            break

    if not captured_email:
        normalized_transcript = normalize_spoken_email(transcript_text)
        normalized_payload = normalize_spoken_email(str(payload))
        normalized_call = normalize_spoken_email(str(call))

        captured_email = (
            pick_best_email(normalized_transcript)
            or pick_best_email(transcript_text)
            or pick_best_email(normalized_call)
            or pick_best_email(normalized_payload)
        )

    print("STRUCTURED EMAIL:", structured_email)
    print("META EMAIL:", meta_email)
    print("DYN EMAIL:", dyn_email)
    print("CAPTURED EMAIL FINAL:", captured_email)
    print("TRANSCRIPT (first 500):", transcript_text[:500])

    text_blob = " ".join([outcome, summary, transcript_text]).lower()

    def has_any(words):
        return any(w in text_blob for w in words)

    if event == "call_analyzed":
        if has_any(
            [
                "appointment booked",
                "booked appointment",
                "appointment scheduled",
                "scheduled appointment",
                "meeting booked",
                "meeting scheduled",
                "consultation booked",
                "calendar invite sent",
                "booking confirmed",
                "confirmed appointment",
                "confirmed meeting",
                "you're all set",
                "you are all set",
                "calendar invite",
                "confirmation email",
            ]
        ):
            status_val = "BOOKED"
            next_action_val = "SEND_CONFIRMATION_EMAIL"
        elif has_any(
            [
                "not interested",
                "do not call",
                "stop calling",
                "remove me",
                "unsubscribe",
            ]
        ):
            status_val = "NOT_INTERESTED"
            next_action_val = "NONE"
        elif has_any(
            [
                "no answer",
                "no_answer",
                "voicemail",
                "left voicemail",
                "busy signal",
                "declined call",
                "unreachable",
            ]
        ):
            status_val = "NO_ANSWER"
            next_action_val = "NONE"
        elif has_any(
            [
                "call back",
                "callback",
                "call me back",
                "reach back out",
                "try me later",
                "call later",
                "try again later",
                "give me a call later",
            ]
        ):
            status_val = "CALLBACK"
            next_action_val = "CALLBACK_LATER"
        elif has_any(
            [
                "send me info",
                "send me information",
                "send me details",
                "email me",
                "send an email",
                "follow up by email",
                "shoot me an email",
                "send that over",
                "send it over",
                "requested more information",
                "asked to be emailed",
            ]
        ):
            status_val = "FOLLOW_UP"
            next_action_val = "EMAIL_FOLLOWUP"
        else:
            status_val = "REVIEW"
            next_action_val = "REVIEW"
    else:
        status_val = ""
        next_action_val = ""

    print("=== RETELL WEBHOOK ===")
    print("EVENT:", event, "FLOW:", flow_type, "CALL_ID:", call_id, "LEAD_ID:", lead_id)
    print("STATUS:", status_val, "NEXT:", next_action_val, "EMAIL:", captured_email)

    row_num = None

    if flow_type == "demo":
        row_num = find_row_by_call_id_only(ws, hm, call_id)

        if row_num is None:
            row_num = find_matching_row_by_phone_candidates(
                ws,
                hm,
                [from_number, lead_id, to_number],
            )

        else:
            row_num = find_matching_row_outbound(ws, hm, call_id, lead_id, to_number)

        # Fallback: callback came in as outbound, but belongs to demo sheet
        if row_num is None:
            try:
                demo_ws = get_ws(DEMO_SHEET_ID, DEMO_WORKSHEET_NAME)
                demo_hm = header_map_norm(demo_ws)

                demo_row = find_matching_row_by_phone_candidates(
                    demo_ws,
                    demo_hm,
                    [from_number, lead_id, to_number],
                )

                if demo_row:
                    print("CALLBACK MATCHED DEMO SHEET BY PHONE")
                    ws = demo_ws
                    hm = demo_hm
                    row_num = demo_row
                    flow_type = "demo"

            except Exception as e:
                print("Demo fallback match failed:", str(e))

        if row_num is None:
            print(
                "WEBHOOK IGNORED — NO MATCHING ROW:",
                call_id,
                from_number,
                to_number,
                lead_id,
                flow_type,
            )
            return {
                "ok": True,
                "ignored": "no_matching_row",
                "call_id": call_id,
                "from_number": from_number,
                "to_number": to_number,
                "lead_id": lead_id,
                "flow_type": flow_type,
            }


        cleanup: Dict[str, Any] = {}

        if "email_primary" in hm:
            existing_primary = (
                (ws.cell(row_num, hm["email_primary"]).value or "").strip().lower()
            )
            if existing_primary and not is_valid_real_email(existing_primary):
                cleanup["email_primary"] = ""

        if "emails_found" in hm:
            existing_found = ws.cell(row_num, hm["emails_found"]).value or ""
            cleaned_found = clean_found_list(existing_found)
            if cleaned_found != existing_found:
                cleanup["emails_found"] = cleaned_found

        if cleanup:
            batch_write_cells(ws, row_num, hm, cleanup)

        if event == "call_ended":
            current_status = (ws.cell(row_num, hm["status"]).value or "").strip().upper()
            updates = {"last_called_at": utc_now_iso()}

            if call_id:
                updates["last_klaviyo_call_id"] = call_id

            if current_status not in FINAL_STATUSES:
                updates["status"] = "CALL_ENDED"
                updates["next_action"] = "WAITING_FOR_ANALYSIS"

            batch_write_cells(ws, row_num, hm, updates)

            return {
                "ok": True,
                "event": event,
                "row": row_num,
                "call_id": call_id,
            }

        email_updates: Dict[str, Any] = {}

        if captured_email:
            if "email_primary" in hm:
                current_primary = (
                    (ws.cell(row_num, hm["email_primary"]).value or "").strip().lower()
                )
                if not is_valid_real_email(current_primary):
                    email_updates["email_primary"] = captured_email

            if "emails_found" in hm:
                current_found = ws.cell(row_num, hm["emails_found"]).value or ""
                email_updates["emails_found"] = append_unique_email(
                    current_found, captured_email
                )

        current_status = (
            (ws.cell(row_num, hm["status"]).value or "").strip().upper()
            if "status" in hm
            else ""
        )
        current_next_action = (
            (ws.cell(row_num, hm["next_action"]).value or "").strip()
            if "next_action" in hm
            else ""
        )

        final_status, final_next_action = pick_final_status(
            current_status,
            status_val,
            current_next_action,
            next_action_val,
        )

        final_updates = {
            "status": final_status,
            "next_action": final_next_action,
            "last_called_at": utc_now_iso(),
            "last_klaviyo_call_id": call_id,
            **email_updates,
        }
        batch_write_cells(ws, row_num, hm, final_updates)

        email_for_klaviyo = ""

        if "email_primary" in hm:
            row_email = (ws.cell(row_num, hm["email_primary"]).value or "").strip().lower()
            if is_valid_real_email(row_email):
                email_for_klaviyo = row_email

        if not email_for_klaviyo and captured_email:
            email_for_klaviyo = captured_email

        if email_for_klaviyo and is_valid_real_email(email_for_klaviyo):
            raw = final_status.upper()

            if raw == "BOOKED":
                klaviyo_outcome = "BOOKED"
            elif raw == "CALLBACK":
                klaviyo_outcome = "CALLBACK"
            elif raw in ("NOT_INTERESTED", "NOT INTERESTED"):
                klaviyo_outcome = "NOT INTERESTED"
            else:
                klaviyo_outcome = "FOLLOW_UP"

            try:
                klaviyo_upsert_profile(email_for_klaviyo)

                if klaviyo_outcome in ("BOOKED", "CALLBACK", "FOLLOW_UP"):
                    klaviyo_track_call_outcome(
                        email_for_klaviyo,
                        klaviyo_outcome,
                        {
                            "call_id": call_id,
                            "lead_id": lead_id,
                            "to_number": to_number,
                            "from_number": from_number,
                            "sheet_status": final_status,
                            "next_action": final_next_action,
                            "flow_type": flow_type,
                        },
                    )
                    print("Klaviyo Call Outcome sent:", klaviyo_outcome, email_for_klaviyo)

            except Exception as e:
                print("Klaviyo error:", str(e))
        else:
            print("No valid email for Klaviyo; skipped.")

        return {
            "ok": True,
            "event": event,
            "row": row_num,
            "status": final_status,
            "next_action": final_next_action,
            "call_id": call_id,
            "captured_email": captured_email,
            "flow_type": flow_type,
        }
