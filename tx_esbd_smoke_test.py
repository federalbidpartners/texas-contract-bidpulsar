import re
import json
import time
import argparse
from datetime import datetime
import requests
from bs4 import BeautifulSoup

BASE_LIST = "https://www.txsmartbuy.com/esbd"
BASE_DETAIL = "https://www.txsmartbuy.gov"

UA = "Mozilla/5.0 (compatible; BidPulsarBot/1.0)"
SOURCE_SYSTEM = "state_tx_esbd"
JURISDICTION_LEVEL = "state"
JURISDICTION_STATE = "TX"

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_mmddyyyy(s: str):
    s = clean(s)
    if not s:
        return None
    try:
        return datetime.strptime(s, "%m/%d/%Y").date().isoformat()
    except Exception:
        return None

def parse_time_hhmm_ampm(s: str):
    s = clean(s).upper()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%I:%M %p").time()
    except Exception:
        return None

def response_deadline_iso(due_date: str, due_time: str):
    if not due_date:
        return None
    if due_time:
        t = parse_time_hhmm_ampm(due_time)
        if t:
            dt = datetime.strptime(due_date, "%Y-%m-%d").replace(
                hour=t.hour, minute=t.minute, second=0, microsecond=0
            )
            return dt.isoformat()
    return due_date

def slugify(text: str, max_len: int = 120) -> str:
    text = clean(text).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    if len(text) > max_len:
        text = text[:max_len].rstrip("-")
    return text

def fetch_html(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.text

def extract_list_items(html: str):
    """
    ESBD list pages are HTML. Each solicitation has a link like /esbd/<ID>.
    We'll harvest the title + surrounding text block for: id, status, agency, dates, due time.
    """
    soup = BeautifulSoup(html, "lxml")
    items = []

    # These are the solicitation links.
    for a in soup.select('a[href^="/esbd/"]'):
        href = a.get("href", "")
        title = clean(a.get_text(" ", strip=True))
        if not href or not title:
            continue

        # Container text holds labels like "Solicitation ID:" etc.
        # Prefer the full result row if available.
        container = a.find_parent(class_="esbd-result-row") or a.parent
        if not container:
            continue
        block = clean(container.get_text("\n", strip=True))

        m = re.search(r"Solicitation ID:\s*([A-Za-z0-9\-_]+)", block)
        if not m:
            continue
        sol_id = m.group(1)

        posted = None
        m = re.search(r"Posting Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", block)
        if m:
            posted = parse_mmddyyyy(m.group(1))

        due_date = None
        m = re.search(r"Due Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", block)
        if m:
            due_date = parse_mmddyyyy(m.group(1))

        due_time = None
        m = re.search(r"Due Time:\s*([0-9]{1,2}:[0-9]{2}\s*[AP]M)", block, re.IGNORECASE)
        if m:
            due_time = clean(m.group(1)).upper()

        agency = None
        m = re.search(r"Agency/Texas SmartBuy Member Number:\s*([A-Za-z0-9]+)", block)
        if m:
            agency = clean(m.group(1))

        status = None
        m = re.search(r"Status:\s*([A-Za-z ]+)", block)
        if m:
            status = clean(m.group(1))

        items.append({
            "solicitation_id": sol_id,
            "title": title,
            "agency_code": agency,
            "status": status,
            "posted_date": posted,
            "due_date": due_date,
            "due_time": due_time,
            "detail_url": f"{BASE_DETAIL}{href}",
        })

    # De-dupe by solicitation_id
    seen = set()
    deduped = []
    for it in items:
        if it["solicitation_id"] in seen:
            continue
        seen.add(it["solicitation_id"])
        deduped.append(it)
    return deduped

def extract_description_from_detail(detail_html: str):
    """
    Detail pages vary, so we do a robust text-based section extraction.
    We look for 'Solicitation Description:' and capture until 'Attachments' or a common next section.
    """
    soup = BeautifulSoup(detail_html, "lxml")
    text_lines = [ln.strip() for ln in soup.get_text("\n").split("\n")]
    text_lines = [ln for ln in text_lines if ln]

    desc = []
    in_desc = False
    for ln in text_lines:
        if ln.strip() == "Solicitation Description:":
            in_desc = True
            continue
        if in_desc:
            if ln.strip() in ("Attachments", "Contact Information", "Questions", "Vendor Information"):
                break
            desc.append(ln.strip())

    out = clean("\n".join(desc)) if desc else None
    # guard against accidentally capturing a ton of page chrome
    if out and len(out) < 20:
        return None
    return out

def extract_agency_from_detail(detail_html: str):
    soup = BeautifulSoup(detail_html, "lxml")
    text = soup.get_text("\n")
    for label in [
        "Agency/Texas SmartBuy Member Name:",
        "Agency Name:",
        "Agency:",
        "Issuing Agency:",
        "Issuing Organization:",
    ]:
        m = re.search(rf"{re.escape(label)}\s*(.+)", text)
        if m:
            return clean(m.group(1))
    return None

def extract_attachments(detail_html: str):
    soup = BeautifulSoup(detail_html, "lxml")
    attachments = []

    # Try to find a section labeled "Attachments"
    anchors = []
    for tag in soup.find_all(text=re.compile(r"\bAttachments\b", re.IGNORECASE)):
        parent = tag.parent
        if not parent:
            continue
        container = parent.find_next_sibling() or parent.parent
        if not container:
            continue
        anchors = container.find_all("a", href=True)
        if anchors:
            break

    if not anchors:
        # Fallback: only collect likely file links
        anchors = [
            a for a in soup.find_all("a", href=True)
            if re.search(r"\.(pdf|docx?|xlsx?|csv|zip)$", a["href"], re.IGNORECASE)
        ]

    for a in anchors:
        name = clean(a.get_text(" ", strip=True))
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = f"{BASE_DETAIL}{href}"
        attachments.append({"name": name or None, "url": href})

    return attachments or None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=2, help="How many ESBD list pages to scan (newest first).")
    ap.add_argument("--max-details", type=int, default=10, help="How many detail pages to fetch for description.")
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--source-system", default=SOURCE_SYSTEM)
    ap.add_argument("--jurisdiction-level", default=JURISDICTION_LEVEL)
    ap.add_argument("--jurisdiction-state", default=JURISDICTION_STATE)
    ap.add_argument("--supabase-url", default=None)
    ap.add_argument("--supabase-key", default=None)
    ap.add_argument("--supabase-table", default=None)
    ap.add_argument("--on-conflict", default="external_id,source_system")
    args = ap.parse_args()

    session = requests.Session()

    all_items = []
    for p in range(1, args.pages + 1):
        url = f"{BASE_LIST}?page={p}"
        html = fetch_html(session, url)
        items = extract_list_items(html)
        print(f"[list] page {p}: {len(items)} items")
        all_items.extend(items)
        time.sleep(args.sleep)

    # De-dupe across pages
    seen = set()
    uniq = []
    for it in all_items:
        if it["solicitation_id"] in seen:
            continue
        seen.add(it["solicitation_id"])
        uniq.append(it)

    # Pull details for the first N and attach description
    enriched = []
    for i, it in enumerate(uniq[: args.max_details], start=1):
        try:
            dhtml = fetch_html(session, it["detail_url"])
            desc = extract_description_from_detail(dhtml)
            it2 = dict(it)
            it2["description"] = desc
            it2["agency_name"] = extract_agency_from_detail(dhtml)
            it2["attachments"] = extract_attachments(dhtml)
            enriched.append(it2)
            print(f"[detail] {i}/{min(len(uniq), args.max_details)} ok {it['solicitation_id']}")
        except Exception as e:
            it2 = dict(it)
            it2["description"] = None
            it2["agency_name"] = None
            it2["attachments"] = None
            it2["detail_error"] = str(e)
            enriched.append(it2)
            print(f"[detail] {i}/{min(len(uniq), args.max_details)} FAIL {it['solicitation_id']}: {e}")
        time.sleep(args.sleep)

    # Map to Supabase fields
    mapped = []
    for it in enriched:
        external_id = it.get("solicitation_id")
        title = it.get("title")
        agency = it.get("agency_name") or it.get("agency_code")
        posted_date = it.get("posted_date")
        response_deadline = response_deadline_iso(it.get("due_date"), it.get("due_time"))
        url = it.get("detail_url")
        description = it.get("description")
        attachments = it.get("attachments")
        slug_base = f"{args.jurisdiction_state} {external_id} {title or ''}"

        mapped.append({
            "external_id": external_id,
            "source_system": args.source_system,
            "jurisdiction_level": args.jurisdiction_level,
            "jurisdiction_state": args.jurisdiction_state,
            "title": title,
            "agency": agency,
            "posted_date": posted_date,
            "response_deadline": response_deadline,
            "url": url,
            "description": description,
            "attachments": attachments,
            "slug": slugify(slug_base),
        })

    # Output a preview JSON to stdout (easy to eyeball)
    print("\n=== PREVIEW (first 5 enriched records) ===")
    print(json.dumps(mapped[:5], indent=2, ensure_ascii=False))

    # Also write a file so you can inspect it
    out_path = "tx_esbd_preview.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(mapped, f, indent=2, ensure_ascii=False)
    print(f"\n[wrote] {out_path} ({len(mapped)} records)")

    # Optional: push to Supabase via REST
    if args.supabase_url and args.supabase_key and args.supabase_table:
        endpoint = f"{args.supabase_url}/rest/v1/{args.supabase_table}?on_conflict={args.on_conflict}"
        headers = {
            "apikey": args.supabase_key,
            "Authorization": f"Bearer {args.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        r = session.post(endpoint, headers=headers, data=json.dumps(mapped))
        try:
            r.raise_for_status()
            print(f"[supabase] upsert ok: {r.status_code}")
        except Exception:
            print(f"[supabase] upsert failed: {r.status_code} {r.text}")

if __name__ == "__main__":
    main()
