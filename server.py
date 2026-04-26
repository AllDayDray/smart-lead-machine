# server.py
print("SERVER FILE LOADED")

from email.mime import text
import os
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException

# =========================
# ENV
# =========================
load_dotenv(dotenv_path=".env")

KLAVIYO_PRIVATE_API_KEY = os.getenv("KLAVIYO_PRIVATE_API_KEY")
KLAVIYO_LIST_ID = os.getenv("KLAVIYO_LIST_ID", "")

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

# =========================
# EMAIL EXTRACTION
# =========================
TEST_EMAIL_RE = re.compile(r"(^ping@|^test@|@example\.com$)", re.I)
EMAIL_RE_ALL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)


def extract_all_emails(text: str) -> List[str]:
    if not text:
        return []
    return [e.lower() for e in EMAIL_RE_ALL.findall(text)]


def pick_best_email(text: str) -> str:
    emails = extract_all_emails(text)
    if not emails:
        return ""

    t = (text or "").lower()
    confirm_words = [
        "yes",
        "correct",
        "that's correct",
        "that is correct",
        "confirmed",
        "confirm",
        "exactly",
        "right",
        "you got it",
    ]

    best = ("", -1)
    for e in emails:
        idx = t.rfind(e)
        window = t[max(0, idx - 120) : idx + 120]
        score = sum(1 for w in confirm_words if w in window)
        score = score * 10 + idx
        if score > best[1]:
            best = (e, score)

    return best[0] or emails[-1]


def normalize_spoken_email(text: str) -> str:
    if not text:
        return ""

    t = text.lower()

    # Convert common spoken email patterns into email symbols.
    t = re.sub(r"\s+at\s+", "@", t)
    t = re.sub(r"\s+dot\s+", ".", t)

    # Common domain speech cleanup.
    t = t.replace("g-mail", "gmail")
    t = t.replace("g mail", "gmail")
    t = t.replace("gee mail", "gmail")

    # Remove filler punctuation/spacing that speech transcripts add.
    t = t.replace(" [at] ", "@")
    t = t.replace(" [dot] ", ".")
    t = re.sub(r"[^a-z0-9@._%+\-]+", "", t)

    return t


def is_test_email(email: Optional[str]) -> bool:
    if not email:
        return False
    return bool(TEST_EMAIL_RE.search(email.strip()))


def append_unique_email(existing: str, new_email: str) -> str:
    items = [e.strip().lower() for e in (existing or "").split(",") if e.strip()]
    if new_email and new_email.lower() not in items:
        items.append(new_email.lower())
    return ", ".join(items)


def clean_found_list(s: str) -> str:
    items = [e.strip().lower() for e in (s or "").split(",") if e.strip()]
    seen = set()
    out = []
    for e in items:
        if is_test_email(e):
            continue
        if e in seen:
            continue
        seen.add(e)
        out.append(e)
    return ", ".join(out)


# =========================
# HELPERS
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


def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)


def get_ws(sheet_id: str, worksheet_name: str):
    if not sheet_id:
        raise HTTPException(status_code=500, detail="Missing Google Sheet ID in .env")
    gc = gs_client()
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(worksheet_name)


def norm_header(s: str) -> str:
    return str(s).strip().lower().replace(" ", "_")


def header_map_norm(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {norm_header(h): i + 1 for i, h in enumerate(headers) if h}


def row_dict(ws, row_num: int) -> Dict[str, str]:
    headers = ws.row_values(1)
    values = ws.row_values(row_num)
    out: Dict[str, str] = {}
    for i, h in enumerate(headers):
        key = norm_header(h)
        out[key] = (values[i] if i < len(values) else "") or ""
    return out


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
        # RAW keeps phone numbers like +13032463246 from being converted by Google Sheets.
        ws.batch_update(updates, value_input_option="RAW")


def append_row_by_headers(ws, hm: Dict[str, int], values: Dict[str, Any]) -> int:
    headers = ws.row_values(1)
    row = [""] * len(headers)
    for col_name, val in values.items():
        if col_name in hm:
            row[hm[col_name] - 1] = "" if val is None else str(val)
    # RAW keeps + phone numbers as text instead of Google Sheets stripping the plus sign.
    ws.append_row(row, value_input_option="RAW")
    return len(ws.col_values(1))


def safe_find(ws, value: str, col_index: int):
    if not value or not col_index:
        return None
    try:
        return ws.find(value, in_column=col_index)
    except Exception:
        return None


def digits_only(value: str) -> str:
    return re.sub(r"\D", "", str(value or ""))


def parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def find_existing_row_by_phone(
    ws, hm: Dict[str, int], phone_e164: str
) -> Optional[int]:
    """Find a row even if Google Sheets stripped + from the phone/lead_id."""
    target_digits = digits_only(phone_e164)
    if not target_digits:
        return None

    for col_name in ("lead_id", "phone"):
        if col_name not in hm:
            continue
        values = ws.col_values(hm[col_name])
        for idx, value in enumerate(values[1:], start=2):
            if digits_only(value) == target_digits:
                return idx

    return None


def is_recent_duplicate(row_data: Dict[str, str], seconds: int = 90) -> bool:
    last_called_raw = row_data.get("last_called_at", "")
    last_called = parse_iso_datetime(last_called_raw)
    if not last_called:
        return False
    age = (datetime.now(timezone.utc) - last_called).total_seconds()
    return age >= 0 and age <= seconds


def upsert_row_by_lead_id(
    ws, hm: Dict[str, int], lead_id: str, values: Dict[str, Any]
) -> int:
    existing_row = None

    if "lead_id" in hm and lead_id:
        cell = safe_find(ws, lead_id, hm["lead_id"])
        if cell:
            existing_row = cell.row

    # Fallback for existing rows where Google Sheets stripped the leading +.
    if not existing_row:
        existing_row = find_existing_row_by_phone(ws, hm, lead_id)

    if existing_row:
        safe_values = dict(values)
        if not safe_values.get("email_primary"):
            safe_values.pop("email_primary", None)

        batch_write_cells(ws, existing_row, hm, safe_values)
        return existing_row

    return append_row_by_headers(ws, hm, values)


def find_key_paths(obj, target_key, path="$"):
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}"
            if k == target_key:
                hits.append((new_path, v))
            hits.extend(find_key_paths(v, target_key, new_path))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            hits.extend(find_key_paths(item, target_key, f"{path}[{i}]"))
    return hits


def first_found(payload, keys):
    for key in keys:
        hits = find_key_paths(payload, key)
        for _, v in hits:
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            return v
    return ""


def ensure_required_columns(
    hm: Dict[str, int], required: List[str], label: str
) -> None:
    missing = [c for c in required if c not in hm]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"{label} sheet missing columns: {', '.join(missing)}",
        )


def get_sheet_for_flow(flow_type: str):
    if flow_type == "demo":
        ws = get_ws(DEMO_SHEET_ID, DEMO_WORKSHEET_NAME)
        label = "Demo"
    else:
        ws = get_ws(DIALER_SHEET_ID, DIALER_WORKSHEET_NAME)
        label = "Dialer"
    hm = header_map_norm(ws)
    return ws, hm, label


def find_matching_row(
    ws, hm: Dict[str, int], call_id: str, lead_id: str, to_number: str
) -> Optional[int]:
    if call_id and "last_klaviyo_call_id" in hm:
        cell = safe_find(ws, call_id, hm["last_klaviyo_call_id"])
        if cell:
            return cell.row

    # outbound-specific match: Retell lead_id may actually be stored in lead_uuid
    if lead_id and "lead_uuid" in hm:
        cell = safe_find(ws, lead_id, hm["lead_uuid"])
        if cell:
            return cell.row

    if lead_id and "lead_id" in hm:
        cell = safe_find(ws, lead_id, hm["lead_id"])
        if cell:
            return cell.row

    if to_number and "lead_id" in hm:
        cell = safe_find(ws, to_number, hm["lead_id"])
        if cell:
            return cell.row

    if to_number and "phone" in hm:
        cell = safe_find(ws, to_number, hm["phone"])
        if cell:
            return cell.row

    return None


# =========================
# KLAVIYO
# =========================
def klaviyo_upsert_profile(email: str) -> None:
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")

    url = "https://a.klaviyo.com/api/profile-import"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": "2025-01-15",
    }
    payload = {"data": {"type": "profile", "attributes": {"email": email}}}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()


def klaviyo_add_to_list(email: str, list_id: str) -> None:
    if not list_id:
        return
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")

    url = f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles"
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": "2025-01-15",
    }
    payload = {"data": [{"type": "profile", "attributes": {"email": email}}]}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()


def klaviyo_track_call_outcome(email: str, call_outcome: str, props: dict) -> None:
    if not KLAVIYO_PRIVATE_API_KEY:
        raise RuntimeError("Missing KLAVIYO_PRIVATE_API_KEY in .env")

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

    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()


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

    url = "https://api.retellai.com/v2/create-phone-call"

    payload = {
        "from_number": RETELL_FROM_NUMBER,
        "to_number": to_number,
        "override_agent_id": RETELL_AGENT_ID,
        "metadata": {
            "flow_type": "demo",
            "lead_id": lead_id,
            "source": source,
            "email": email or "",
        },
        "retell_llm_dynamic_variables": {
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}".strip(),
            "email": email or "",
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

    r = requests.post(url, headers=headers, json=payload, timeout=30)

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
        "demo_worksheet": DEMO_WORKSHEET_NAME,
        "dialer_worksheet": DIALER_WORKSHEET_NAME,
    }


@app.post("/demo-lead")
async def demo_lead(request: Request):
    data = await request.json()
    print("STEP 1: request parsed")

    first_name = str(data.get("first_name") or "").strip()
    last_name = str(data.get("last_name") or "").strip()
    phone_raw = str(data.get("phone") or "").strip()
    email = str(data.get("email") or "").strip().lower()
    source = str(data.get("source") or "LeeWave Demo").strip()

    print("=== NEW DEMO LEAD ===")
    print("FIRST:", first_name)
    print("LAST:", last_name)
    print("PHONE RAW:", phone_raw)
    print("EMAIL:", email)
    print("SOURCE:", source)

    if not first_name:
        raise HTTPException(status_code=400, detail="Missing first_name")
    if not phone_raw:
        raise HTTPException(status_code=400, detail="Missing phone")

    phone_e164 = normalize_phone_e164(phone_raw)
    print("STEP 2: phone normalized", phone_e164)

    print("PHONE E164:", phone_e164)
    print("DEBUG DEMO_SHEET_ID:", repr(DEMO_SHEET_ID))
    print("DEBUG DEMO_WORKSHEET_NAME:", repr(DEMO_WORKSHEET_NAME))

    print("STEP 3: opening demo sheet")
    demo_ws = get_ws(DEMO_SHEET_ID, DEMO_WORKSHEET_NAME)
    demo_hm = header_map_norm(demo_ws)

    ensure_required_columns(
        demo_hm,
        ["lead_id", "status", "next_action", "last_called_at", "last_klaviyo_call_id"],
        "Demo",
    )

    # Block duplicate Elfsight/Make submits for the same phone within a short window.
    # This prevents double calls while still allowing fresh tests later.
    existing_row = find_existing_row_by_phone(demo_ws, demo_hm, phone_e164)
    if existing_row:
        existing_data = row_dict(demo_ws, existing_row)
        if is_recent_duplicate(existing_data, seconds=90):
            print(
                "🚫 DUPLICATE DEMO SUBMIT BLOCKED",
                {
                    "row": existing_row,
                    "phone": phone_e164,
                    "status": existing_data.get("status", ""),
                    "last_called_at": existing_data.get("last_called_at", ""),
                    "last_klaviyo_call_id": existing_data.get(
                        "last_klaviyo_call_id", ""
                    ),
                },
            )
            return {
                "ok": True,
                "skipped": "duplicate_recent_submit",
                "row": existing_row,
                "phone": phone_e164,
                "status": existing_data.get("status", ""),
            }

    demo_values = {
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone_e164,
        "lead_id": phone_e164,
        "email_primary": email,
        "source": source,
        "status": "CALL_STARTED",
        "next_action": "REVIEW",
        "last_called_at": utc_now_iso(),
    }

    row_num = upsert_row_by_lead_id(
        demo_ws,
        demo_hm,
        phone_e164,
        demo_values,
    )
    print("STEP 4: row written", row_num)

    print("ROW WRITTEN TO DEMO SHEET:", row_num)

    print("STEP 5: creating Retell call")
    resp = create_retell_call_for_demo(
        first_name=first_name,
        last_name=last_name,
        email=email,
        source=source,
        lead_id=phone_e164,
        to_number=phone_e164,
    )

    print("STEP 6: retell created")

    retell_call_id = str(resp.get("call_id") or "").strip()
    if retell_call_id:
        batch_write_cells(
            demo_ws,
            row_num,
            demo_hm,
            {"last_klaviyo_call_id": retell_call_id},
        )

    print("RETELL CALL CREATED:", resp)

    if email:
        try:
            klaviyo_upsert_profile(email)
            if KLAVIYO_LIST_ID:
                klaviyo_add_to_list(email, KLAVIYO_LIST_ID)
            print("KLAVIYO PROFILE SYNCED")
        except Exception as e:
            print("KLAVIYO ERROR:", str(e))

    return {
        "ok": True,
        "first_name": first_name,
        "phone": phone_e164,
        "row": row_num,
        "retell": resp,
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

    flow_type = str(meta.get("flow_type") or dyn.get("flow_type") or "").strip().lower()

    print("FLOW_TYPE:", flow_type)

    ws, hm, sheet_label = get_sheet_for_flow(flow_type or "outbound")
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

    outcome = str(
        first_found(
            call,
            ["call_outcome", "outcome", "disposition", "result", "status"],
        )
        or first_found(
            call.get("call_analysis") or {},
            ["outcome", "disposition", "result", "status"],
        )
        or first_found(
            payload,
            ["call_outcome", "outcome", "disposition", "result", "status"],
        )
        or ""
    ).strip()

    summary = str(
        first_found(
            call,
            ["summary", "call_summary", "notes"],
        )
        or first_found(
            call.get("call_analysis") or {},
            ["summary", "analysis_summary"],
        )
        or first_found(
            payload,
            ["summary", "call_summary", "notes"],
        )
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

    analysis_summary = str(
        first_found(
            call,
            ["analysis_summary", "agent_summary", "post_call_analysis"],
        )
        or first_found(
            call.get("call_analysis") or {},
            ["analysis_summary", "summary", "outcome", "disposition"],
        )
        or first_found(
            payload,
            ["analysis_summary", "agent_summary", "post_call_analysis"],
        )
        or ""
    ).strip()

    meta_email = str(meta.get("email") or "").strip().lower()

    normalized_summary = normalize_spoken_email(summary)
    normalized_transcript = normalize_spoken_email(transcript_text)

    captured_email = (
        pick_best_email(normalized_summary)
        or pick_best_email(normalized_transcript)
        or pick_best_email(normalize_spoken_email(str(call)))
        or pick_best_email(normalize_spoken_email(str(payload)))
        or pick_best_email(summary)
        or pick_best_email(transcript_text)
        or meta_email
        or pick_best_email(str(call))
        or pick_best_email(str(payload))
    )

    text_blob = " ".join([outcome, summary, transcript_text, analysis_summary]).lower()

    def has_any(words):
        return any(w in text_blob for w in words)

    if event == "call_analyzed":
        booked_phrases = [
            "appointment booked",
            "booked appointment",
            "appointment scheduled",
            "scheduled appointment",
            "meeting booked",
            "booked a meeting",
            "meeting scheduled",
            "consultation booked",
            "consultation scheduled",
            "calendar invite sent",
            "booking confirmed",
            "confirmed appointment",
            "confirmed meeting",
            "set for",
            "you're all set",
            "you are all set",
            "we're all set",
            "we are all set",
            "calendar invite",
            "confirmation email",
            "i would love to sit down and book an appointment",
            "book an appointment with him",
        ]

        not_interested_phrases = [
            "not interested",
            "do not call",
            "stop calling",
            "remove me",
            "unsubscribe",
        ]

        no_answer_phrases = [
            "no answer",
            "no_answer",
            "voicemail",
            "left voicemail",
            "busy signal",
            "declined call",
            "unreachable",
        ]

        callback_phrases = [
            "call back",
            "callback",
            "call me back",
            "reach back out",
            "try me later",
            "call later",
            "reach back out",
            "try again later",
            "give me a call later",
            "can you call me back",
            "circle back",
        ]

        followup_phrases = [
            "send me info",
            "send me information",
            "send me details",
            "send more information",
            "send me more info",
            "email me",
            "send an email",
            "follow up by email",
            "follow up later",
            "shoot me an email",
            "send that over",
            "send it over",
            "send it to me",
            "you can email me",
            "can you email me",
            "feel free to email me",
            "forward me the details",
            "send me your website",
            "send me your info",
            "send me something",
            "have him email me",
            "i'll take a look if you email it",
            "email over the details",
            "just email me",
            "just send me the info",
            "requested more information",
            "requested information",
            "requested details",
            "requested materials",
            "asked for more information",
            "asked for details",
            "asked to be emailed",
            "asked for an email",
            "interested in receiving info",
            "wants more information",
            "wants details by email",
        ]

        booking_pattern = (
            (
                "what time zone are you in" in text_blob
                or "before i mention times" in text_blob
            )
            and (
                "would tomorrow at" in text_blob
                or "would thursday at" in text_blob
                or "would friday at" in text_blob
                or "would monday at" in text_blob
                or "would tuesday at" in text_blob
                or "would wednesday at" in text_blob
            )
            and (
                "would work a lot better" in text_blob
                or "would work better" in text_blob
                or "thursday would work" in text_blob
                or "tomorrow would work" in text_blob
                or "yes. yes." in text_blob
                or "yes." in text_blob
            )
            and (
                "you're all set" in text_blob
                or "you’re all set" in text_blob
                or "calendar invite" in text_blob
                or "confirmation email" in text_blob
                or "just to confirm, you're good for" in text_blob
                or "just to confirm, you’re good for" in text_blob
            )
        )

        explicit_datetime_pattern = bool(
            re.search(
                r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b.*\b\d{1,2}(:\d{2})?\s?(am|pm)\b",
                text_blob,
            )
        ) and (
            "calendar invite" in text_blob
            or "confirmation email" in text_blob
            or "you're all set" in text_blob
            or "you’re all set" in text_blob
        )

        if has_any(booked_phrases) or booking_pattern or explicit_datetime_pattern:
            status_val = "BOOKED"
            next_action_val = "SEND_CONFIRMATION_EMAIL"
        elif has_any(not_interested_phrases):
            status_val = "NOT_INTERESTED"
            next_action_val = "NONE"
        elif has_any(no_answer_phrases):
            status_val = "NO_ANSWER"
            next_action_val = "CALLBACK_LATER"
        elif has_any(callback_phrases):
            status_val = "CALLBACK"
            next_action_val = "CALLBACK_LATER"
        elif has_any(followup_phrases):
            status_val = "FOLLOW_UP"
            next_action_val = "EMAIL_FOLLOWUP"
        else:
            status_val = "REVIEW"
            next_action_val = "REVIEW"
    else:
        status_val = ""
        next_action_val = ""

    print("\n=== RETELL WEBHOOK ===")
    print("EVENT:", event)
    print("FLOW_TYPE:", flow_type)
    print("CALL_ID:", call_id)
    print("LEAD_ID:", lead_id)
    print("TO:", to_number, "FROM:", from_number)
    print("OUTCOME:", outcome)
    print("SUMMARY:", summary)
    print("TEXT_BLOB:", text_blob)
    print("TRANSCRIPT_TEXT:", transcript_text)
    print("NORMALIZED_TRANSCRIPT:", normalized_transcript)
    print("ANALYSIS_SUMMARY:", analysis_summary)
    print("STATUS:", status_val, "NEXT_ACTION:", next_action_val)
    print("CAPTURED_EMAIL:", captured_email)
    print()

    row_num = find_matching_row(ws, hm, call_id, lead_id, to_number)
    if row_num is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not locate row (call_id={call_id}, lead_id={lead_id}, to_number={to_number})",
        )

    print("MATCHED ROW:", row_num)

    if event == "call_ended":
        current_status = (ws.cell(row_num, hm["status"]).value or "").strip().upper()
        current_call_id = (
            ws.cell(row_num, hm["last_klaviyo_call_id"]).value or ""
        ).strip()

        duration_ms = call.get("duration_ms") or payload.get("duration_ms") or 0
        try:
            duration_ms = int(duration_ms)
        except Exception:
            duration_ms = 0

        protected_statuses = {
            "BOOKED",
            "CALLBACK",
            "FOLLOW_UP",
            "NO_ANSWER",
            "NOT_INTERESTED",
            "WRONG_NUMBER",
            "GATEKEEPER",
        }

        if current_status in protected_statuses:
            print(
                "call_ended received — no fallback:",
                {
                    "current_status": current_status,
                    "current_call_id": current_call_id,
                    "call_id": call_id,
                    "duration_ms": duration_ms,
                },
            )
            return {
                "ok": True,
                "event": event,
                "row": row_num,
                "status": "(unchanged)",
                "next_action": "(unchanged)",
                "call_id": call_id,
                "lead_id": lead_id,
                "to_number": to_number,
                "captured_email": captured_email,
            }

        updates: Dict[str, Any] = {"last_called_at": utc_now_iso()}
        if call_id:
            updates["last_klaviyo_call_id"] = call_id

        if (
            current_status == "CALL_STARTED"
            and current_call_id
            and call_id == current_call_id
            and duration_ms >= 6000
        ):
            updates["status"] = "FOLLOW_UP"
            updates["next_action"] = "EMAIL_FOLLOWUP"
            print("SAFE FALLBACK APPLIED (call_ended): FOLLOW_UP / EMAIL_FOLLOWUP")
        else:
            print(
                "call_ended received — no fallback:",
                {
                    "current_status": current_status,
                    "current_call_id": current_call_id,
                    "call_id": call_id,
                    "duration_ms": duration_ms,
                },
            )

        batch_write_cells(ws, row_num, hm, updates)

        return {
            "ok": True,
            "event": event,
            "row": row_num,
            "status": updates.get("status", "(unchanged)"),
            "next_action": updates.get("next_action", "(unchanged)"),
            "call_id": call_id,
            "lead_id": lead_id,
            "to_number": to_number,
            "captured_email": captured_email,
        }

    if captured_email:
        updates = {}
        if "email_primary" in hm:
            current_primary = (
                (ws.cell(row_num, hm["email_primary"]).value or "").strip().lower()
            )
            if not current_primary:
                updates["email_primary"] = captured_email.strip().lower()

        if "emails_found" in hm:
            current_found = ws.cell(row_num, hm["emails_found"]).value or ""
            merged = append_unique_email(current_found, captured_email.strip().lower())
            updates["emails_found"] = clean_found_list(merged)

        if updates:
            batch_write_cells(ws, row_num, hm, updates)

        try:
            klaviyo_upsert_profile(captured_email.strip().lower())
            if KLAVIYO_LIST_ID:
                klaviyo_add_to_list(captured_email.strip().lower(), KLAVIYO_LIST_ID)
            print("Klaviyo updated:", captured_email.strip().lower())
        except Exception as e:
            print("Klaviyo update error:", str(e))

    batch_write_cells(
        ws,
        row_num,
        hm,
        {
            "status": status_val,
            "next_action": next_action_val,
            "last_called_at": utc_now_iso(),
            "last_klaviyo_call_id": call_id,
        },
    )

    email_permission = False
    if (
        dyn.get("email_permission") is not None
        and str(dyn.get("email_permission")).strip()
    ):
        email_permission = str(dyn.get("email_permission")).strip().lower() == "true"
    elif (
        meta.get("email_permission") is not None
        and str(meta.get("email_permission")).strip()
    ):
        email_permission = str(meta.get("email_permission")).strip().lower() == "true"

    email_for_klaviyo = ""
    if "email_primary" in hm:
        email_for_klaviyo = (
            (ws.cell(row_num, hm["email_primary"]).value or "").strip().lower()
        )
    if not email_for_klaviyo and captured_email:
        email_for_klaviyo = captured_email.strip().lower()

    if email_for_klaviyo:
        raw = (status_val or "").strip().upper()
        if raw == "BOOKED":
            klaviyo_outcome = "BOOKED"
        elif raw == "CALLBACK":
            klaviyo_outcome = "CALLBACK"
        elif raw in ("NOT_INTERESTED", "NOT INTERESTED"):
            klaviyo_outcome = "NOT INTERESTED"
        else:
            klaviyo_outcome = "FOLLOW_UP"

        should_send = klaviyo_outcome in ("BOOKED", "CALLBACK", "FOLLOW_UP")

        try:
            klaviyo_upsert_profile(email_for_klaviyo)
            if should_send:
                klaviyo_track_call_outcome(
                    email=email_for_klaviyo,
                    call_outcome=klaviyo_outcome,
                    props={
                        "call_id": call_id,
                        "lead_id": lead_id,
                        "to_number": to_number,
                        "from_number": from_number,
                        "sheet_status": status_val,
                        "next_action": next_action_val,
                        "email_permission": email_permission,
                        "flow_type": flow_type or "outbound",
                    },
                )
                print(
                    "Klaviyo Call Outcome event sent:",
                    klaviyo_outcome,
                    "->",
                    email_for_klaviyo,
                )
            else:
                print(
                    "Skipping Klaviyo Call Outcome event.",
                    "Outcome:",
                    klaviyo_outcome,
                    "| email_permission:",
                    email_permission,
                )
        except Exception as e:
            print("Klaviyo Call Outcome event error:", str(e))
    else:
        print("No email available for Klaviyo Call Outcome trigger (skipping).")

    return {
        "ok": True,
        "event": event,
        "row": row_num,
        "status": status_val,
        "next_action": next_action_val,
        "call_id": call_id,
        "lead_id": lead_id,
        "to_number": to_number,
        "captured_email": captured_email,
        "email_permission": email_permission,
        "flow_type": flow_type or "outbound",
    }
