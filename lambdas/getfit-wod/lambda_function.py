"""getfit-wod: Concept2 WOD (displayed with permission, attributed).
Daily fetch + cache, /wod/today, subscribe/unsubscribe, daily fan-out to subscribers."""
import json, re, datetime, uuid, urllib.request
import html as _htmlmod
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
TABLE = boto3.resource("dynamodb").Table("GetFit")
UA = {"User-Agent": "GregsEnduranceTraining-WOD/1.0 (coach@gregsendurancetraining.com)"}
PLAN_ID = "c2-wod"
PLAN_TITLE = "Concept2 Workout of the Day"

def today_et():
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5)))
    return now.date().isoformat()

WORDNUM = {"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,
           "eight":8,"nine":9,"ten":10,"fifteen":15,"twenty":20,"thirty":30}

def _num(tok):
    tok = tok.strip().lower()
    if tok.isdigit(): return int(tok)
    return WORDNUM.get(tok)

def _rest_seconds(text):
    t = text.lower()
    m = re.search(r"(\d+|[a-z]+)\s*min(?:ute)?s?\s*(?:of\s*)?(?:rest|easy|at light pressure|light pressure|paddle)", t)
    if m:
        n = _num(m.group(1))
        if n: return n * 60
    m = re.search(r"(?:rest|easy)\s*(?:of\s*)?(\d+|[a-z]+)\s*min(?:ute)?s?", t)
    if m:
        n = _num(m.group(1))
        if n: return n * 60
    m = re.search(r"/\s*(\d+)\s*min", t)
    if m: return int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*sec(?:ond)?s?\s*(?:of\s*)?rest", t)
    if m: return int(m.group(1))
    m = re.search(r"(?:/|w/|with)?\s*(\d+):(\d{2})\s*(?:rest|easy|paddle)", t)
    if m: return int(m.group(1)) * 60 + int(m.group(2))
    return None

def _ladder(text):
    m = re.search(r"((?:\d+\s*[/\-]\s*){2,}\d+)\s*min(?:ute)?s?", text, re.I)
    if m:
        return [int(x) for x in re.split(r"[/\-]", m.group(1)) if x.strip().isdigit()]
    seq = re.findall(r"(\d+)\s*min(?:ute)?s?(?=\s*[,;]|\s+and\b)", text, re.I)
    if len(seq) >= 3:
        allm = re.findall(r"(\d+)\s*min(?:ute)?s?", text, re.I)
        return [int(x) for x in allm]
    return None

def parse_erg(title, desc=""):
    text = str(title) + ". " + str(desc)
    rest = _rest_seconds(text)
    m = re.search(r"(\d+)\s*[x\u00d7]\s*(\d+(?:\.\d+)?)\s*(m\b|meter|metre|k\b|km)", text, re.I)
    if m:
        sets = int(m.group(1)); d = float(m.group(2))
        if m.group(3).lower().startswith("k"): d *= 1000
        p = {"order": 0, "movement": "Row", "rawText": str(title),
             "sets": Decimal(sets), "distanceMeters": Decimal(str(d))}
        if rest: p["restSeconds"] = Decimal(rest)
        return [p]
    m = re.search(r"(\d+)\s*[x\u00d7]\s*(\d+)\s*min(?:ute)?s?", text, re.I)
    if m:
        p = {"order": 0, "movement": "Row", "rawText": str(title),
             "sets": Decimal(int(m.group(1))), "durationSeconds": Decimal(int(m.group(2)) * 60)}
        if rest: p["restSeconds"] = Decimal(rest)
        return [p]
    lad = _ladder(text)
    if lad and len(lad) >= 3:
        out = []
        for i, mins in enumerate(lad):
            p = {"order": i, "movement": "Row", "rawText": str(mins) + " min",
                 "sets": Decimal(1), "durationSeconds": Decimal(mins * 60)}
            if rest: p["restSeconds"] = Decimal(rest)
            out.append(p)
        return out
    m = re.search(r"(\d+)\s*[x\u00d7]\s*(\d+)\s*cal", text, re.I)
    if m:
        p = {"order": 0, "movement": "Row", "rawText": str(title),
             "sets": Decimal(int(m.group(1))), "calories": Decimal(int(m.group(2)))}
        if rest: p["restSeconds"] = Decimal(rest)
        return [p]
    m = re.search(r"^\s*(\d+(?:,\d{3})*)\s*(m\b|meter|metre)", str(title), re.I)
    if m:
        return [{"order": 0, "movement": "Row", "rawText": str(title),
                 "sets": Decimal(1), "distanceMeters": Decimal(m.group(1).replace(",", ""))}]
    m = re.search(r"^\s*(\d+)\s*min(?:ute)?s?", str(title), re.I)
    if m:
        return [{"order": 0, "movement": "Row", "rawText": str(title),
                 "sets": Decimal(1), "durationSeconds": Decimal(int(m.group(1)) * 60)}]
    return [{"order": 0, "movement": "Row", "rawText": str(title)}]

def fetch_wod(date_s):
    url = f"https://log.concept2.com/wod/{date_s}/rowerg"
    html = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=15).read().decode(errors="replace")
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&laquo;", "\u00ab", text).replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text)
    m = re.search(r"Workout of the Day\s*\u00ab?\s*([A-Z][a-z]+ \d{1,2}, \d{4})\s*(.+?)\s*(?:RowErg SkiErg BikeErg|Pos\. Name)", text)
    if not m:
        raise ValueError("WOD block not found")
    blob = m.group(2).strip()
    blob = _htmlmod.unescape(blob).replace("\u00ab", "").replace("\u00bb", "").strip()
    tm = re.match(r"^(.{3,90}?)\s+([A-Z][^.]{8,}\..*)$", blob)
    title, desc = (tm.group(1).strip(), tm.group(2).strip()) if tm else (blob[:80], blob)
    return {"PK": "WOD#concept2", "SK": f"DATE#{date_s}", "itemType": "WOD",
            "date": date_s, "machine": "rowerg", "title": title, "description": desc,
            "attribution": "Workout of the Day courtesy of Concept2",
            "attributionUrl": f"https://log.concept2.com/wod/{date_s}/rowerg",
            "logbookNote": "Row it with ErgData and it lands on your Concept2 Logbook and the Honorboard.",
            "prescriptions": parse_erg(title, desc),
            "fetchedAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}

def get_or_fetch(date_s):
    r = TABLE.get_item(Key={"PK": "WOD#concept2", "SK": f"DATE#{date_s}"})
    if "Item" in r:
        return r["Item"], False
    item = fetch_wod(date_s)
    TABLE.put_item(Item=item)
    return item, True

def wod_enrollment(sub):
    r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                    Key("SK").begins_with("ENROLLMENT#"))
    for e in r.get("Items", []):
        if e.get("planId") == PLAN_ID and e.get("status") == "active":
            return e
    return None

def deliver(sub, eid, wod):
    """Idempotent: deterministic SK per date+enrollment."""
    sid = uuid.uuid5(uuid.NAMESPACE_URL, f"c2wod:{eid}:{wod['date']}")
    TABLE.put_item(Item={"PK": f"USER#{sub}", "SK": f"SCHED#{wod['date']}#{sid}",
        "itemType": "SCHED", "enrollmentId": eid, "planId": PLAN_ID,
        "planTitle": PLAN_TITLE, "scheduledDate": wod["date"],
        "title": wod["title"], "coachTitle": f"C2 WOD \u00b7 {wod['date']}",
        "sessionType": "conditioning",
        "rawText": f"{wod['description']}\n\n{wod['attribution']}\n{wod['attributionUrl']}",
        "prescriptions": wod.get("prescriptions") or [{"order": 0, "movement": "Row", "rawText": wod["description"]}],
        "coachManaged": False, "logStatus": "pending"})

def all_subscribers():
    out, lek = [], None
    while True:
        kw = {"FilterExpression": "begins_with(SK, :e) AND planId = :p AND #st = :a",
              "ExpressionAttributeNames": {"#st": "status"},
              "ExpressionAttributeValues": {":e": "ENROLLMENT#", ":p": PLAN_ID, ":a": "active"}}
        if lek: kw["ExclusiveStartKey"] = lek
        r = TABLE.scan(**kw)
        out += [(i["PK"].replace("USER#", ""), i["enrollmentId"]) for i in r.get("Items", [])]
        lek = r.get("LastEvaluatedKey")
        if not lek: break
    return out

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, default=str)}

def _claims_sub(event):
    try:
        return event["requestContext"]["authorizer"]["jwt"]["claims"]["sub"]
    except Exception:
        return None

def lambda_handler(event, context):
    if event.get("source") == "aws.events" or event.get("wodDaily"):
        wod, fetched = get_or_fetch(today_et())
        subs = all_subscribers()
        for sub, eid in subs:
            deliver(sub, eid, wod)
        print(f"daily: {wod['date']} '{wod['title']}' fetched={fetched} delivered={len(subs)}")
        return {"ok": True, "date": wod["date"], "title": wod["title"], "delivered": len(subs)}

    rk = event.get("routeKey", "")
    if rk == "GET /wod/today":
        try:
            item, _ = get_or_fetch(today_et())
            out = {k: v for k, v in item.items() if k not in ("PK", "SK", "itemType")}
            return {"statusCode": 200, "headers": {"Content-Type": "application/json",
                    "Cache-Control": "max-age=900"}, "body": json.dumps(out, default=str)}
        except Exception as e:
            print(f"wod error: {e}")
            return _resp(503, {"error": "WOD temporarily unavailable"})
    if rk == "POST /wod/subscribe":
        sub = _claims_sub(event)
        if not sub: return _resp(401, {"error": "auth required"})
        e = wod_enrollment(sub)
        if e:
            return _resp(200, {"ok": True, "enrollmentId": e["enrollmentId"], "already": True})
        eid = str(uuid.uuid4())
        TABLE.put_item(Item={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{eid}",
            "itemType": "ENROLLMENT", "enrollmentId": eid, "planId": PLAN_ID,
            "planTitle": PLAN_TITLE, "status": "active", "coachManaged": False,
            "startDate": today_et(),
            "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        try:
            wod, _ = get_or_fetch(today_et())
            deliver(sub, eid, wod)
        except Exception as ex:
            print(f"immediate delivery failed: {ex}")
        return _resp(200, {"ok": True, "enrollmentId": eid, "already": False})
    if rk == "POST /wod/unsubscribe":
        sub = _claims_sub(event)
        if not sub: return _resp(401, {"error": "auth required"})
        e = wod_enrollment(sub)
        if not e: return _resp(200, {"ok": True, "already": True})
        TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": e["SK"]},
            UpdateExpression="SET #st = :s",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "ended"})
        return _resp(200, {"ok": True})
    return _resp(404, {"error": "unknown route"})
