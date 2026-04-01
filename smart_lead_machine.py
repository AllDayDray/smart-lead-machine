#!/usr/bin/env python3
"""
Smart Lead Machine v1 (CSV -> Decision + Message)

Assumes Google Sheet columns A–P:
A Prospect Type
B Occupation
C Business Name
D City
E State/Province
F Contact Name
G Email
H Phone
I Website (URL or 'N-A')
J Facebook
K Instagram
L Funnel Stage (Cold Lead, Discovery, Warm Lead, Hot Lead, Client)
M Needs & Pain Points (should include Primary Gap; can be plain text)
N Notes & Next Step
O First Touch Date
P Next Follow Up Date
"""

import csv
import sys
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any


PRIMARY_GAPS = {"No Website", "Weak Website", "No Email", "No Funnel", "Needs Optimization"}
STAGES = {"Cold Lead", "Discovery", "Warm Lead", "Hot Lead", "Client"}

PITCH_MESSAGES = {
    "No Website": (
        "Hey {name} — I came across {business}. Quick question: are you open to a simple, clean website "
        "that helps you get more bookings without living in DMs? I can build a one-page site + contact/booking "
        "flow fast. Want me to send a quick concept idea?"
    ),
    "Weak Website": (
        "Hey {name} — I checked out your site for {business}. It looks like it’s doing the job, but it could "
        "convert way better with a cleaner layout + clearer “book now” flow. I can do a quick refresh that makes "
        "it easier for visitors to take action. Want a quick breakdown of what I’d change?"
    ),
    "No Email": (
        "Hey {name} — quick idea for {business}: most people visit once and disappear. I can set up a simple email "
        "capture + automated follow-up so you can turn visitors into repeat customers without extra work. Want me "
        "to show you what that looks like?"
    ),
    "No Funnel": (
        "Hey {name} — you’ve got the pieces, but it looks like there’s no clear path from visitor → customer. "
        "I can set up a simple funnel (landing page + offer + follow-up) so people know exactly what to do next. "
        "Want a quick outline tailored to {business}?"
    ),
    "Needs Optimization": (
        "Hey {name} — I took a look at your online setup. You’re already ahead of most businesses — I think a few "
        "small tweaks could bring you more leads without rebuilding everything. I can do a quick audit and tell you "
        "the top 3 changes I’d make. Want that?"
    ),
}

NUDGE_MESSAGE = (
    "Hey {name} — quick one: what’s the main thing you want more of right now — more calls/bookings, "
    "more walk-ins, or more repeat customers? If you tell me, I’ll point you to the simplest upgrade for {business}."
)

UPSELL_MESSAGE = (
    "Hey {name} — quick idea: if you want consistent leads instead of random spikes, I can run a simple monthly "
    "growth setup (site + email + tracking) so things keep improving each month. Want me to lay out a clean plan?"
)


@dataclass
class Lead:
    prospect_type: str
    occupation: str
    business: str
    city: str
    state_or_province: str
    contact_name: str
    email: str
    phone: str
    website: str
    facebook: str
    instagram: str
    stage: str
    needs_pain_points: str
    notes_next_step: str
    first_touch_date: Optional[date]
    next_follow_up_date: Optional[date]


def parse_date(value: str) -> Optional[date]:
    """
    Tries common Google Sheets export formats.
    Accepts: YYYY-MM-DD, MM/DD/YYYY, MM/DD/YY, M/D/YYYY, etc.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None

    fmts = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d/%m/%Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def has_website(lead: Lead) -> bool:
    s = (lead.website or "").strip()
    return bool(s) and s.upper() != "N-A"


def extract_primary_gap(needs_text: str, website_present: bool) -> str:
    """
    Supports either:
      - 'Primary Gap: No Website'
      - or raw text containing one of the standard gap phrases
      - or fallback based on website presence
    """
    t = (needs_text or "").strip()

    # 1) Look for "Primary Gap: X"
    m = re.search(r"primary\s*gap\s*:\s*(.+)", t, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        # handle extra text after the gap
        for gap in PRIMARY_GAPS:
            if gap.lower() in candidate.lower():
                return gap

    # 2) Look for any gap phrase anywhere in the text
    lower = t.lower()
    for gap in PRIMARY_GAPS:
        if gap.lower() in lower:
            return gap

    # 3) Fallback rules
    if not website_present:
        return "No Website"
    return "Needs Optimization"


def days_overdue(d: Optional[date], today: date) -> Optional[int]:
    if d is None:
        return None
    return (today - d).days


def route_lead(lead: Lead, today: Optional[date] = None, drop_after_days: int = 14) -> Dict[str, Any]:
    """
    Returns:
      {
        'path': 'DROP'|'NURTURE'|'PITCH'|'UPSELL',
        'reason': str,
        'primary_gap': str,
        'message': str|None
      }
    """
    today = today or date.today()

    # Normalize stage
    stage = (lead.stage or "").strip()
    if stage not in STAGES:
        stage = "Discovery"  # safe default

    website_present = has_website(lead)
    primary_gap = extract_primary_gap(lead.needs_pain_points, website_present)

    # 1) Upsell
    if stage == "Client":
        return {
            "path": "UPSELL",
            "reason": "Stage is Client",
            "primary_gap": primary_gap,
            "message": UPSELL_MESSAGE.format(name=lead.contact_name or "there"),
        }

    # 2) Drop (cold + stale)
    if stage == "Cold Lead" and lead.next_follow_up_date:
        overdue = days_overdue(lead.next_follow_up_date, today)
        if overdue is not None and overdue > drop_after_days:
            return {"path": "DROP", "reason": f"Cold Lead and stale by {overdue} days", "primary_gap": primary_gap, "message": None}

    # 3) Nurture (early)
    if stage in {"Cold Lead", "Discovery"}:
        return {
            "path": "NURTURE",
            "reason": f"Stage is {stage} (early)",
            "primary_gap": primary_gap,
            "message": NUDGE_MESSAGE.format(name=lead.contact_name or "there", business=lead.business or "your business"),
        }

    # 4) Pitch (warm/hot)
    if stage in {"Warm Lead", "Hot Lead"}:
        msg = PITCH_MESSAGES.get(primary_gap, PITCH_MESSAGES["Needs Optimization"]).format(
            name=lead.contact_name or "there",
            business=lead.business or "your business",
        )
        return {"path": "PITCH", "reason": f"{stage} + Primary Gap={primary_gap}", "primary_gap": primary_gap, "message": msg}

    return {"path": "NURTURE", "reason": "Fallback", "primary_gap": primary_gap, "message": None}


def read_leads_from_csv(csv_path: str) -> list[Lead]:
    leads: list[Lead] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return leads

    # If the first row looks like headers, skip it.
    # (We assume your export includes headers — but this handles both.)
    first = [c.strip().lower() for c in rows[0]]
    looks_like_header = any("prospect" in c or "funnel" in c or "stage" in c or "website" in c for c in first)

    start_idx = 1 if looks_like_header else 0

    for r in rows[start_idx:]:
        # ensure at least 16 columns (A–P)
        r = (r + [""] * 16)[:16]

        lead = Lead(
            prospect_type=r[0].strip(),
            occupation=r[1].strip(),
            business=r[2].strip(),
            city=r[3].strip(),
            state_or_province=r[4].strip(),
            contact_name=r[5].strip(),
            email=r[6].strip(),
            phone=r[7].strip(),
            website=r[8].strip(),
            facebook=r[9].strip(),
            instagram=r[10].strip(),
            stage=r[11].strip(),
            needs_pain_points=r[12].strip(),
            notes_next_step=r[13].strip(),
            first_touch_date=parse_date(r[14]),
            next_follow_up_date=parse_date(r[15]),
        )
        # Ignore totally empty rows
        if any(getattr(lead, field) for field in lead.__dataclass_fields__ if field not in {"first_touch_date", "next_follow_up_date"}):
            leads.append(lead)

    return leads


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 smart_lead_machine.py leads.csv")
        sys.exit(1)

    csv_path = sys.argv[1]
    today = date.today()

    leads = read_leads_from_csv(csv_path)
    print(f"\nSmart Lead Machine v1 — {len(leads)} leads loaded ({today.isoformat()})\n")

    for i, lead in enumerate(leads, start=1):
        decision = route_lead(lead, today=today)

        print("=" * 72)
        print(f"{i}. {lead.business or '(No Business Name)'} — {lead.city}, {lead.state_or_province}")
        print(f"   Prospect Type: {lead.prospect_type} | Stage: {lead.stage} | Website: {lead.website or 'N-A'}")
        print(f"   Primary Gap: {decision['primary_gap']}")
        print(f"   PATH: {decision['path']}  | Reason: {decision['reason']}")
        if decision["message"]:
            print("\n   Recommended Message:")
            print(f"   {decision['message']}")
        else:
            print("\n   Recommended Message: (none)")

    print("\nDone.\n")


if __name__ == "__main__":
    main()
