"""GET FIT API v60.3 = profile photos (presign kind=profile, photoKey on PROFILE, photoUrl in roster) + GET /me/profile + POST /coach/athletes/photo.
GET FIT API v59.4 = v59.3 + athlete groups (tags) + archived flag in the roster. Route: POST /coach/athletes/tags.
GET FIT API v59.3 = v59.2 + self-logged workouts (POST /me/sessions) start the comment thread like /logs does.
GET FIT API v59.2 = v59.1 + feed rows carry SK (dashboard opens threads from Recent Activity).
GET FIT API v59.1 = v59 + DEFAULT_COACH_SUB fallback (unlinked athletes' comments still reach the coach).
GET FIT API v59 = v58 + workout comments (athlete <-> coach threads on a session; see _comment_add).
  Rows: USER#<athlete> / CMT#<sessionUUID>#<ts>#<id>  {author, authorSub, authorName, text, createdAt, readByCoachAt, readByAthleteAt}
  SCHED row gets commentCount, lastCommentAt, lastCommentBy, athleteUnread (coach msgs the athlete hasn't opened).
  Coach inbox = GSI1 query CMTINBOX#<coachSub> (athlete-authored rows until the coach opens the thread).
  Routes: GET/POST /me/comments  ·  GET /coach/comments/inbox  ·  GET/POST /coach/comments
  POST /logs with a comment also starts the thread (message #0). Pushes carry kind=comment + sessionSK.
GET FIT API v58 = v57 + last-seen/build stamping on PROFILE (see _stamp_last_seen).
GET FIT API v5 = v4 + rolling subscriptions.
New: POST /subscriptions {planId,startDate,chosenWeekdays?}   test-mode subscribe, delivers weeks 1-4
     GET  /me/subscriptions
     POST /subscriptions/cancel {planId}                       autoRenew off; delivered weeks stay
     POST /coach/subscriptions/grant {athleteSub,planId,startDate,chosenWeekdays?}
EventBridge daily: {"task":"deliverWeeks"} -> renew active subs (+28d, +4 weeks), expire lapsed.
Looping: delivered week D serves content week ((D-1) mod totalWeeks)+1.
"""
import json, os, re, csv, io, uuid, random, datetime
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

TABLE = boto3.resource("dynamodb").Table("GetFit")
S3 = boto3.client("s3")
IDP = boto3.client("cognito-idp")
SNS = boto3.client("sns")
PLATFORM_ARN = os.environ.get("APNS_PLATFORM_ARN", "")
PLATFORM_ARN_PROD = "arn:aws:sns:us-east-1:446100759389:app/APNS/getfit-apns-prod"
BUCKET = os.environ.get("PHOTO_BUCKET", "")
POOL_ID = os.environ.get("POOL_ID", "")

# v58-last-seen: stamp who is on what build. Additive, non-fatal.
# Reads X-App-Version / X-App-Build / X-OS-Version / X-Device-Model headers
# (case-insensitive; sent by iOS build 20+). Attributes are only written when
# the header is present, so older clients still refresh lastSeenAt without
# blanking a version they never reported. firstSeenAt is set once.
def _stamp_last_seen(event, user_sub):
    try:
        hdrs = {str(k).lower(): v for k, v in (event.get("headers") or {}).items() if k}
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        expr = ["lastSeenAt = :t", "firstSeenAt = if_not_exists(firstSeenAt, :t)"]
        vals = {":t": now}
        for hdr, attr, ph in (("x-app-version", "lastAppVersion", ":v"),
                              ("x-app-build", "lastAppBuild", ":b"),
                              ("x-os-version", "lastOSVersion", ":o"),
                              ("x-device-model", "lastDeviceModel", ":d")):
            val = str(hdrs.get(hdr) or "").strip()[:48]
            if val:
                expr.append(f"{attr} = {ph}"); vals[ph] = val
        TABLE.update_item(Key={"PK": f"USER#{user_sub}", "SK": "PROFILE"},
            UpdateExpression="SET " + ", ".join(expr),
            ExpressionAttributeValues=vals)
    except Exception as e:
        print(f"[v58 last-seen] non-fatal: {e}")

def _clean(o):
    if isinstance(o, list): return [_clean(x) for x in o]
    if isinstance(o, dict): return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, Decimal): return int(o) if o % 1 == 0 else float(o)
    return o
def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(_clean(body))}
# v56-benchmark-delete
# v57-row-brick-types
def _claims(e): return e["requestContext"]["authorizer"]["jwt"]["claims"]
def _is_coach(e): return "coaches" in str(_claims(e).get("cognito:groups", ""))
def _body(e): return json.loads(e.get("body") or "{}", parse_float=Decimal)
def _py_weekday(wd): return (int(wd) - 2) % 7
def _first_on_or_after(start, wd):
    d = start
    while d.weekday() != _py_weekday(wd): d += datetime.timedelta(days=1)
    return d
def _today(): return datetime.date.today()

# ---------- CSV parser (unchanged grammar) ----------
WEEKDAYS = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
BENCH_MAP = {"5k tt":"5K Time Trial","5k time trial":"5K Time Trial","10k tt":"10K",
             "1 mile tt":"1 Mile","mile tt":"1 Mile","1/2 marathon tt":"Half Marathon",
             "half marathon tt":"Half Marathon","1/2 marathon":"Half Marathon",
             "10m tt":"10 Mile","10 mile tt":"10 Mile"}
def norm_ref(r): return BENCH_MAP.get(r.strip().lower(), r.strip())
def wd_cell(c):
    cl = c.strip().lower()
    for i, w in enumerate(WEEKDAYS):
        if w in cl: return i + 1
    return None
def dayn_cell(c):
    m = re.fullmatch(r"day\s*(\d+)", c.strip(), re.I)
    return int(m.group(1)) if m else None
def is_header(cells):
    return sum(1 for c in cells if wd_cell(c)) >= 3 or sum(1 for c in cells if dayn_cell(c)) >= 2
def colmap(cells):
    wd, dn = [], []
    for i, c in enumerate(cells):
        cc = c.strip()
        if not cc or "result" in cc.lower(): continue
        w = wd_cell(cc); n = dayn_cell(cc)
        if w: wd.append((i, w, cc))
        elif n: dn.append((i, n, cc))
    if wd: return [(i, w, lbl, w - 1) for (i, w, lbl) in wd], False
    K = len(dn)
    return [(i, None, f"Day {n}", (n - 1) * 7 // K) for (i, n, _) in dn], True
SWIM_SECTIONS = (("warm", "warmup"), ("post", "postwork"), ("cool", "postwork"),
                 ("workout", "workout"), ("main", "workout"))

def _swim_section(line):
    low = line.lower().rstrip(":").strip()
    if len(low) > 24: return None
    for key, sec in SWIM_SECTIONS:
        if low.startswith(key): return sec
    return None

def parse_cell_multi(text, category=None):
    """Swim plans: one prescription per line, tagged with its section.
    warmup/postwork = done-or-not; workout = per-piece logging."""
    if (category or "").lower() != "swim":
        return [parse_cell(text)]
    out, section = [], "workout"
    for raw in str(text).splitlines():
        line = raw.strip()
        if not line: continue
        sec = _swim_section(line)
        if sec is not None:
            section = sec; continue
        low = line.lower()
        m = re.fullmatch(r"rest\s*:?\s*(\d+)\s*(min(?:ute)?s?|sec(?:ond)?s?|s)?\.?", low)
        if m:
            secs = int(m.group(1)) * (60 if (m.group(2) or "").startswith("min") else 1)
            if out: out[-1]["restSeconds"] = secs
            continue
        p = {"order": len(out), "movement": "Swim", "section": section, "rawText": line}
        if re.fullmatch(r"\d+\s*(rounds?|sets?)\b.*", low) or line.startswith("*"):
            out.append(p); continue  # structural / annotation: no numbers
        body = re.sub(r"^\d+\s*[-\u2013]\s*\d+\s*\.\s*", "", line)
        m = re.search(r"(\d+)\s*[x\u00d7]\s*(\d+)", body)
        if m:
            p["sets"] = int(m.group(1)); p["distanceMeters"] = Decimal(m.group(2))
        else:
            m2 = re.match(r"^(\d+(?:,\d{3})*)\b(?!\s*\.)", body)
            if m2: p["sets"] = 1; p["distanceMeters"] = Decimal(m2.group(1).replace(",", ""))
        m = re.search(r"@\s*(\d+):(\d{2})", body)
        if m: p["intervalSeconds"] = int(m.group(1)) * 60 + int(m.group(2))
        else:
            m = re.search(r"@\s*(\d+)\s*min\b", body, re.I)
            if m: p["intervalSeconds"] = int(m.group(1)) * 60
        m = re.search(r"rest\s*:?\s*(\d+)\s*min", low)
        if m: p["restSeconds"] = int(m.group(1)) * 60
        else:
            m = re.search(r"rest\s*:(\d+)\b", low)
            if m: p["restSeconds"] = int(m.group(1))
        if "for time" in low: p["forTime"] = True
        out.append(p)
    return out or [parse_cell(text)]

def parse_cell(text):
    p = {"order": 0, "movement": "Run", "rawText": text}
    t = text.lower()
    m = re.search(r"(\d+)\s*[-\u2013]?\s*\d*\s*[\*x]\s*(\d+(?:\.\d+)?)\s*(m|mile|miles|k|km)\b", t)
    if m:
        p["sets"] = int(m.group(1)); d = float(m.group(2)); u = m.group(3)
        p["distanceMeters"] = Decimal(str(d * (1609.344 if "mile" in u else (1000 if u in ("k","km") else 1))))
    m = re.search(r"(?:w/|with|rest)\s*(\d+)\s*min(?:ute)?s?\s*rest|rest\s*(\d+)\s*min", t)
    if m: p["restSeconds"] = int(m.group(1) or m.group(2)) * 60
    m = re.search(r"(\d+)\s*min(?:ute)?s?\s*(?:t\b|tempo)", t)
    if m: p["durationSeconds"] = int(m.group(1)) * 60
    m = re.search(r"@\s*(\d+)\s*%\s*(?:of\s*)?([^,\n(]+)", text, re.I)
    if m:
        p["intensity"] = {"kind": "percentBenchmark", "value": Decimal(m.group(1)),
                          "benchmarkRef": norm_ref(m.group(2))}
    else:
        m = re.search(r"@\s*([^,\n(]+?)\s+pace", text, re.I)
        if m: p["intensity"] = {"kind": "percentBenchmark", "value": Decimal(100),
                                "benchmarkRef": norm_ref(m.group(1))}
    return p
# Cell TYPE keywords: first line of a cell may start with e.g. "SWIM — Easy aerobic".
# Keyword (any case) → sessionType. Checked before the text heuristics below.
_TYPE_KEYWORDS = [
    ("swim",         "swim"),
    ("bike",         "bike"),
    ("ride",         "bike"),
    ("row",          "row"),
    ("erg",          "row"),
    ("conditioning", "conditioning"),
    ("wod",          "conditioning"),
    ("strength",     "strength"),
    ("lift",         "strength"),
    ("rest",         "rest"),
    ("note",         "rest"),          # the app has no "note" type; a NOTE cell is a rest day with text
    ("race",         "run"),
    ("run",          "run"),
]
_TYPE_RE = re.compile(r"^\s*([a-z]+)\s*(?:—|–|-|:)", re.I)

def stype_bench(text):
    t = text.strip().lower()
    m = re.fullmatch(r"([\w/ .]+?)\s*(?:tt|time trial)", t)
    if m: return "timeTrial", norm_ref(t)
    # v60: explicit TYPE keyword on the first line wins
    first = t.split("\n", 1)[0]
    km = _TYPE_RE.match(first)
    if km:
        kw = km.group(1).lower()
        for k, st in _TYPE_KEYWORDS:
            if kw == k:
                if st == "run":
                    break          # fall through to tempo/strength heuristics
                return st, None
    if "tempo" in t or re.search(r"\bmin\s+t\b|\bt\s*@", t): return "tempo", None
    if re.search(r"squat|press|deadlift|bench", t): return "strength", None
    return "run", None
def athlete_title(category, label, session_type, raw_text=""):
    """Athlete-facing name: no week numbers. Coach dashboard shows weekNumber separately."""
    cat = (category or "").lower()
    lab = (label or "").strip()
    low = (raw_text or "").lower()
    if session_type == "strength" or cat == "strength":
        for lift in ("Back Squat", "Strict Press", "Bench Press", "Deadlift"):
            if lift.lower() in low:
                return f"{lift.split()[-2] if lift != 'Deadlift' else 'Deadlift'} Day" if False else f"{lift} Day"
        return "Strength Day"
    if cat == "swim":
        import re as _re
        m = _re.match(r"day\s*(\d+)", lab, _re.I)
        return f"Swim {m.group(1)}" if m else (f"Swim \u00b7 {lab}" if lab else "Swim")
    if cat in ("run", ""):
        # v60.1: name by the resolved type so a BIKE/SWIM/ROW cell in a run plan isn't called a run
        word = {"swim": "Swim", "bike": "Bike", "row": "Row", "rest": "Rest", "conditioning": "WOD"}.get(session_type, "Run")
        return f"Today's {word} \u00b7 {lab}" if lab and not lab.lower().startswith("day") else f"Today's {word}"
    if cat == "conditioning":
        import re as _re
        m = _re.match(r"day\s*(\d+)", lab, _re.I)
        return f"WOD {m.group(1)}" if m else "Today's WOD"
    return lab or "Training"

def parse_plan_csv(csv_text, category=None):
    rows = list(csv.reader(io.StringIO(csv_text.replace("\r\n","\n").replace("\r","\n"))))
    cmap, floating, sessions, week = None, False, [], 0
    for row in rows:
        if not any(c.strip() for c in row): continue
        if is_header(row):
            cmap, floating = colmap(row)
            cmap = [(ci, wd, label, k) for k, (ci, wd, label, off) in enumerate(sorted(cmap, key=lambda x: x[0]))]
            continue
        if cmap is None: continue
        week += 1
        for (ci, wd, label, off) in cmap:
            cell = row[ci].strip() if ci < len(row) else ""
            if not cell or cell.lower() == "rest": continue
            parts = [p.strip() for p in re.split(r"\n\s*\+\+\+\s*\n", cell) if p.strip()]
            for pi, part in enumerate(parts):
                st, bench = stype_bench(part)
                lab = label if len(parts) == 1 else f"{label} \u00b7 {pi + 1}"
                s = {"weekNumber": week, "dayOffset": (week - 1) * 7 + off,
                     "title": athlete_title(None, lab, st, part), "coachTitle": f"Week {week} \u00b7 {lab}", "sessionType": st,
                     "rawText": part, "prescriptions": parse_cell_multi(part, category)}
                if not floating: s["weekday"] = wd
                if bench: s["establishesBenchmark"] = bench
                sessions.append(s)
    return sessions, week, floating
def write_plan(title, category, author, publish, csv_text):
    if "swim" in title.lower(): category = "swim"
    sessions, weeks, floating = parse_plan_csv(csv_text, category)
    if not sessions: return None, "no sessions parsed - check header row"
    plan_id = str(uuid.uuid4())
    meta = {"PK": f"PLAN#{plan_id}", "SK": "META", "itemType": "PLAN", "planId": plan_id,
            "title": title, "author": author, "category": category or "run",
            "durationWeeks": weeks, "isFloatingSchedule": floating,
            "isPublished": bool(publish), "version": 1,
            "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    if publish:
        meta["GSI1PK"] = "MARKETPLACE"; meta["GSI1SK"] = f"PLAN#{title}"
    with TABLE.batch_writer() as bw:
        bw.put_item(Item=meta)
        for s in sessions:
            if s.get("sessionType") in (None, "run") and category in ("swim", "conditioning", "strength"):
                s["sessionType"] = category if category != "conditioning" else "conditioning"
            _mv = {"swim": "Swim", "strength": "Strength", "conditioning": "Conditioning"}.get(category)
            if _mv:
                for _p in s.get("prescriptions", []):
                    if _p.get("movement") in (None, "Run"): _p["movement"] = _mv
            s["title"] = athlete_title(category, s.get("coachTitle","").split("\u00b7")[-1].strip() if s.get("coachTitle") else "", s.get("sessionType"), s.get("rawText",""))
            bw.put_item(Item={"PK": f"PLAN#{plan_id}",
                              "SK": f"SESSION#{s['dayOffset']:04d}#{uuid.uuid4()}",
                              "itemType": "SESSION", **s})
    return {"planId": plan_id, "weeks": weeks, "sessions": len(sessions),
            "floating": floating}, None

# ---------- plan/session helpers ----------
def load_plan(plan_id):
    r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"PLAN#{plan_id}"))
    items = r.get("Items", [])
    meta = next((i for i in items if i["SK"] == "META"), None)
    sessions = sorted((i for i in items if i["SK"].startswith("SESSION#")), key=lambda i: i["SK"])
    return meta, sessions

def _sched_q(user_sub, frm, to):
    cond = Key("PK").eq(f"USER#{user_sub}")
    cond = cond & (Key("SK").between(f"SCHED#{frm}", f"SCHED#{to}#zzzz") if frm and to
                   else Key("SK").begins_with("SCHED#"))
    r = TABLE.query(KeyConditionExpression=cond)
    return r.get("Items", []), r["Count"]

def _move(user_sub, sk, new_date):
    old = TABLE.get_item(Key={"PK": f"USER#{user_sub}", "SK": sk}).get("Item")
    if not old: return None
    new_sk = f"SCHED#{new_date}#{old['SK'].split('#')[-1]}"
    old["SK"] = new_sk; old["scheduledDate"] = new_date; old["userMoved"] = True
    TABLE.put_item(Item=old)
    TABLE.delete_item(Key={"PK": f"USER#{user_sub}", "SK": sk})
    return new_sk

def materialize(user_sub, plan_id, start_str, chosen, coach_managed, coach_sub=None, txn_id=None, product_id=None):
    meta, sessions = load_plan(plan_id)
    if not meta or not sessions: return None, "plan not found"
    start = datetime.date.fromisoformat(start_str)
    floating = bool(meta.get("isFloatingSchedule"))
    enroll_id = str(uuid.uuid4())
    dated = []
    if floating:
        weeks = {}
        for s in sessions: weeks.setdefault(int(s["weekNumber"]), []).append(s)
        K = max(len(v) for v in weeks.values())
        slots = [int(w) for w in (chosen or [])]   # ORDERED
        if len(slots) != K: slots = [2 + (i * 7) // K for i in range(K)]
        anchor = _first_on_or_after(start, slots[0])
        for w, ss in weeks.items():
            for idx, s in enumerate(sorted(ss, key=lambda x: x["SK"])):
                slot = slots[min(idx, K - 1)]
                dated.append((anchor + datetime.timedelta(days=(w - 1) * 7 + ((slot - slots[0]) % 7)), s))
    else:
        for s in sessions:
            dated.append((start + datetime.timedelta(days=int(s["dayOffset"])), s))
    enroll = {"PK": f"USER#{user_sub}", "SK": f"ENROLLMENT#{enroll_id}", "itemType": "ENROLLMENT",
              "enrollmentId": enroll_id, "planId": plan_id, "planTitle": meta.get("title",""),
              "startDate": start_str, "status": "active", "coachManaged": coach_managed,
              "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    if txn_id: enroll["transactionId"] = txn_id
    if product_id: enroll["productId"] = product_id
    if coach_sub:
        enroll["GSI1PK"] = f"COACH#{coach_sub}"
        enroll["GSI1SK"] = f"ATHLETE#{user_sub}#ENROLLMENT#{enroll_id}"
    with TABLE.batch_writer() as bw:
        bw.put_item(Item=enroll)
        for d, s in dated:
            bw.put_item(Item={"PK": f"USER#{user_sub}", "SK": f"SCHED#{d.isoformat()}#{uuid.uuid4()}",
                "itemType": "SCHED", "enrollmentId": enroll_id, "planId": plan_id,
                "planTitle": meta.get("title",""), "scheduledDate": d.isoformat(),
                "title": s.get("title",""), "sessionType": s.get("sessionType","other"),
                "rawText": s.get("rawText",""), "weekNumber": s.get("weekNumber",0),
                "prescriptions": s.get("prescriptions",[]),
                "coachManaged": coach_managed, "logStatus": "pending"})
    return {"enrollmentId": enroll_id, "sessionsCreated": len(dated),
            "firstSession": min(d for d,_ in dated).isoformat()}, None

# ---------- SUBSCRIPTIONS ----------
def deliver_sub_weeks(user_sub, sub, n_weeks=4):
    """Deliver the next n delivered-weeks, looping content. Returns count of sessions written."""
    meta, sessions = load_plan(sub["planId"])
    if not meta or not sessions: return 0
    weeks_map = {}
    for s in sessions: weeks_map.setdefault(int(s["weekNumber"]), []).append(s)
    total = max(weeks_map.keys())
    floating = bool(meta.get("isFloatingSchedule"))
    anchor = datetime.date.fromisoformat(sub["anchorDate"])
    slots = [int(w) for w in sub.get("chosenWeekdays", []) or []]   # ORDERED
    if floating and not slots:
        K = max(len(v) for v in weeks_map.values())
        slots = [2 + (i * 7) // K for i in range(K)]
    done = int(sub.get("weeksDelivered", 0))
    written = 0
    with TABLE.batch_writer() as bw:
        for d_week in range(done + 1, done + 1 + n_weeks):
            if sub.get("raceDate") and d_week > total: break
            content_week = ((d_week - 1) % total) + 1
            cw = sorted(weeks_map.get(content_week, []), key=lambda x: x["SK"])
            for idx, s in enumerate(cw):
                if floating:
                    slot = slots[min(idx, len(slots) - 1)]
                    d = anchor + datetime.timedelta(days=(d_week - 1) * 7 + ((slot - slots[0]) % 7))
                else:
                    off_in_week = int(s["dayOffset"]) % 7
                    d = anchor + datetime.timedelta(days=(d_week - 1) * 7 + off_in_week)
                bw.put_item(Item={"PK": f"USER#{user_sub}",
                    "SK": f"SCHED#{d.isoformat()}#{uuid.uuid4()}",
                    "itemType": "SCHED", "enrollmentId": sub["enrollmentId"],
                    "planId": sub["planId"], "planTitle": meta.get("title",""),
                    "scheduledDate": d.isoformat(),
                    "title": s.get("title",""), "sessionType": s.get("sessionType","other"),
                    "rawText": s.get("rawText",""), "weekNumber": content_week,
                    "deliveredWeek": d_week,
                    "prescriptions": s.get("prescriptions",[]),
                    "coachManaged": bool(sub.get("coachManaged", False)),
                    "logStatus": "pending"})
                written += 1
    new_done = min(done + n_weeks, total) if sub.get("raceDate") else done + n_weeks
    if sub.get("raceDate") and new_done >= total:
        TABLE.update_item(Key={"PK": f"USER#{user_sub}", "SK": sub["SK"]},
            UpdateExpression="SET weeksDelivered = :w, autoRenew = :f",
            ExpressionAttributeValues={":w": new_done, ":f": False})
    else:
        TABLE.update_item(Key={"PK": f"USER#{user_sub}", "SK": sub["SK"]},
            UpdateExpression="SET weeksDelivered = :w",
            ExpressionAttributeValues={":w": new_done})
    return written

def create_subscription(user_sub, plan_id, start_str, chosen, coach_managed=False, coach_sub=None, race_date=None, race_name=None, txn_id=None, product_id=None):
    meta, _ = load_plan(plan_id)
    if not meta: return None, "plan not found"
    if meta.get("accessModel") != "subscription":
        return None, "plan is not a subscription plan"
    existing = TABLE.get_item(Key={"PK": f"USER#{user_sub}", "SK": f"SUB#{plan_id}"}).get("Item")
    if existing and existing.get("status") == "active":
        return None, "already subscribed"
    start = datetime.date.fromisoformat(start_str)
    floating = bool(meta.get("isFloatingSchedule"))
    slots = [int(w) for w in (chosen or [])]   # ORDERED: slot i = plan Day i+1
    anchor = _first_on_or_after(start, slots[0]) if (floating and slots) else start
    enroll_id = str(uuid.uuid4())
    period_end = (_today() + datetime.timedelta(days=28)).isoformat()
    sub = {"PK": f"USER#{user_sub}", "SK": f"SUB#{plan_id}", "itemType": "SUB",
           "planId": plan_id, "planTitle": meta.get("title",""),
           "status": "active", "autoRenew": True, "billingMode": "test",
           "startDate": start_str, "anchorDate": anchor.isoformat(),
           "chosenWeekdays": slots, "weeksDelivered": 0,
           "currentPeriodEnd": period_end, "enrollmentId": enroll_id,
           "coachManaged": coach_managed,
           "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    enroll = {"PK": f"USER#{user_sub}", "SK": f"ENROLLMENT#{enroll_id}", "itemType": "ENROLLMENT",
              "enrollmentId": enroll_id, "planId": plan_id, "planTitle": meta.get("title",""),
              "startDate": start_str, "status": "active", "coachManaged": coach_managed,
              "isSubscription": True,
              "createdAt": sub["createdAt"]}
    if txn_id:
        enroll["transactionId"] = txn_id
        sub["transactionId"] = txn_id
    if product_id:
        enroll["productId"] = product_id
        sub["productId"] = product_id
    if coach_sub:
        enroll["GSI1PK"] = f"COACH#{coach_sub}"
        enroll["GSI1SK"] = f"ATHLETE#{user_sub}#ENROLLMENT#{enroll_id}"
    if race_date:
        sub["raceDate"] = race_date; enroll["raceDate"] = race_date
        if race_name:
            sub["raceName"] = race_name; enroll["raceName"] = race_name
    TABLE.put_item(Item=sub)
    TABLE.put_item(Item=enroll)
    n = deliver_due_weeks(user_sub, sub)
    return {"planId": plan_id, "status": "active", "sessionsDelivered": n,
            "currentPeriodEnd": period_end,
            "note": "test billing mode - StoreKit replaces this in Phase C"}, None


def deliver_due_weeks(user_sub, sub):
    """Deliver plan-weeks whose week has arrived (through this Saturday). Returns sessions written."""
    meta, sessions = load_plan(sub["planId"])
    if not meta or not sessions: return 0
    total = max(int(s["weekNumber"]) for s in sessions)
    today = _today()
    saturday = today + datetime.timedelta(days=(5 - today.weekday()) % 7)
    anchor = datetime.date.fromisoformat(sub["anchorDate"])
    if saturday < anchor: return 0
    target = ((saturday - anchor).days // 7) + 1
    if sub.get("raceDate"): target = min(target, total)
    done = int(sub.get("weeksDelivered", 0))
    n = target - done
    if n <= 0: return 0
    w = deliver_sub_weeks(user_sub, sub, n)
    sub["weeksDelivered"] = done + n
    return w

def sweep_deliveries():
    """Daily task: renew active auto-renew subs near period end (+28d, +4 weeks); expire lapsed."""
    r = TABLE.scan(FilterExpression="itemType = :t",
                   ExpressionAttributeValues={":t": "SUB"})
    today = _today()
    renewed, expired, delivered = 0, 0, 0
    for sub in r.get("Items", []):
        user_sub = sub["PK"].split("#", 1)[1]
        pe = datetime.date.fromisoformat(sub["currentPeriodEnd"])
        if sub.get("status") == "active" and sub.get("autoRenew") and (pe - today).days <= 3:
            new_pe = (pe + datetime.timedelta(days=28)).isoformat()
            TABLE.update_item(Key={"PK": sub["PK"], "SK": sub["SK"]},
                UpdateExpression="SET currentPeriodEnd = :p",
                ExpressionAttributeValues={":p": new_pe})
            sub["currentPeriodEnd"] = new_pe
            renewed += 1
        elif sub.get("status") == "active" and not sub.get("autoRenew") and pe < today:
            TABLE.update_item(Key={"PK": sub["PK"], "SK": sub["SK"]},
                UpdateExpression="SET #s = :e",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":e": "expired"})
            expired += 1
            sub["status"] = "expired"
        if sub.get("status") == "active":
            delivered += deliver_due_weeks(user_sub, sub)
    print(f"sweep: renewed={renewed} expired={expired} weeklySessionsDelivered={delivered}")
    return {"renewed": renewed, "expired": expired, "delivered": delivered}

# ---------- handler ----------

def notification_sweep():
    """Every 15 min: for each profile with a device + enabled, if local time is within
    the 15-min window of morningTime -> push today's sessions; eveningTime -> tomorrow's."""
    from zoneinfo import ZoneInfo
    r = TABLE.scan(FilterExpression="itemType = :t AND attribute_exists(endpointArn)",
                   ExpressionAttributeValues={":t": "PROFILE"})
    sent = 0
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    for prof in r.get("Items", []):
        if prof.get("notificationsEnabled") is False:
            continue
        try:
            tz = ZoneInfo(prof.get("tz", "America/New_York"))
        except Exception:
            tz = ZoneInfo("America/New_York")
        local = now_utc.astimezone(tz)
        user_sub = prof["PK"].split("#", 1)[1]
        def in_window(hhmm):
            if not hhmm: return False
            try: h, m = map(int, hhmm.split(":"))
            except Exception: return False
            slot = h * 60 + m
            cur = local.hour * 60 + local.minute
            return 0 <= cur - slot < 15
        def sessions_on(day):
            iso = day.isoformat()
            q = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{user_sub}") &
                            Key("SK").between(f"SCHED#{iso}", f"SCHED#{iso}#zzzz"))
            return q.get("Items", [])
        msgs = []
        if in_window(prof.get("morningTime", "06:00")):
            todays = sessions_on(local.date())
            if todays:
                titles = ", ".join(s.get("title", "workout") for s in todays[:3])
                msgs.append(("Today's training", f"{titles} — let's go!"))
        if in_window(prof.get("eveningTime")):
            tmrw = sessions_on(local.date() + datetime.timedelta(days=1))
            if tmrw:
                titles = ", ".join(s.get("title", "workout") for s in tmrw[:3])
                msgs.append(("Tomorrow's training", f"{titles} — plan ahead."))
        for title, body in msgs:
            try:
                SNS.publish(TargetArn=prof["endpointArn"],
                    MessageStructure="json",
                    Message=json.dumps({"APNS": json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}), "APNS_SANDBOX": json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}), "default": body}))
                sent += 1
            except SNS.exceptions.EndpointDisabledException:
                pass
    print(f"notification sweep: sent={sent}")
    return {"sent": sent}


def _notify(user_sub, title, body):
    """Push to one athlete's device if registered + enabled. Never raises."""
    try:
        prof = TABLE.get_item(Key={"PK": f"USER#{user_sub}", "SK": "PROFILE"}).get("Item") or {}
        ep = prof.get("endpointArn")
        if not ep or prof.get("notificationsEnabled") is False:
            return False
        SNS.publish(TargetArn=ep, MessageStructure="json",
            Message=json.dumps({"APNS": json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}), "APNS_SANDBOX": json.dumps({"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}), "default": body}))
        return True
    except Exception as e:
        print(f"notify skipped: {e}")
        return False


# ---------- v59 workout comments ----------
COACH_DISPLAY = os.environ.get("COACH_DISPLAY_NAME", "Coach Greg")
DEFAULT_COACH_SUB = os.environ.get("DEFAULT_COACH_SUB", "")   # v59.1: inbox for athletes with no COACH# link

def _push_data(user_sub, title, body, data):
    """Like _notify but with custom keys (kind, sessionSK, ...) so a tap can open the session."""
    try:
        prof = TABLE.get_item(Key={"PK": f"USER#{user_sub}", "SK": "PROFILE"}).get("Item") or {}
        ep = prof.get("endpointArn")
        if not ep or prof.get("notificationsEnabled") is False: return False
        aps = {"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}; aps.update(data or {})
        SNS.publish(TargetArn=ep, MessageStructure="json",
            Message=json.dumps({"APNS": json.dumps(aps), "APNS_SANDBOX": json.dumps(aps), "default": body}))
        return True
    except Exception as e:
        print(f"push_data skipped: {e}"); return False

def _photo_url(key):
    """v60.3: 1-hour presigned GET for a profile photo key, or None."""
    if not key: return None
    try: return S3.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=3600)
    except Exception as e: print(f"photo url failed: {e}"); return None

def _coach_of(athlete_sub):
    prof = TABLE.get_item(Key={"PK": f"USER#{athlete_sub}", "SK": "PROFILE"}).get("Item") or {}
    g = prof.get("GSI1PK") or ""
    coach = g.replace("COACH#", "") if g.startswith("COACH#") else None
    return (coach or DEFAULT_COACH_SUB or None), prof

def _session_uuid(session_sk):
    parts = session_sk.split("#"); return parts[-1] if len(parts) >= 3 else session_sk

def _comment_add(athlete_sub, session_sk, author, author_sub, text, session=None):
    """Append one message to a session thread. author in ('athlete','coach'). Returns the row."""
    text = (text or "").strip()[:2000]
    if not text: return None
    if session is None:
        session = TABLE.get_item(Key={"PK": f"USER#{athlete_sub}", "SK": session_sk}).get("Item")
    if not session: return None
    coach_sub, prof = _coach_of(athlete_sub)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    cid = uuid.uuid4().hex[:8]
    row = {"PK": f"USER#{athlete_sub}", "SK": f"CMT#{_session_uuid(session_sk)}#{now}#{cid}", "itemType": "CMT",
           "sessionSK": session_sk, "sessionTitle": session.get("coachTitle") or session.get("title", ""),
           "scheduledDate": session.get("scheduledDate", ""), "planTitle": session.get("planTitle", ""),
           "author": author, "authorSub": author_sub,
           "authorName": COACH_DISPLAY if author == "coach" else (prof.get("name") or prof.get("username") or "Athlete"),
           "text": text, "createdAt": now}
    if author == "athlete" and coach_sub:
        row["GSI1PK"] = f"CMTINBOX#{coach_sub}"; row["GSI1SK"] = now   # sits in the coach inbox until opened
    TABLE.put_item(Item=row)
    upd = "SET commentCount = if_not_exists(commentCount, :z) + :one, lastCommentAt = :t, lastCommentBy = :a"
    vals = {":z": 0, ":one": 1, ":t": now, ":a": author}
    if author == "coach":
        upd += ", athleteUnread = if_not_exists(athleteUnread, :z) + :one"
    TABLE.update_item(Key={"PK": f"USER#{athlete_sub}", "SK": session_sk}, UpdateExpression=upd, ExpressionAttributeValues=vals)
    # notify the other side
    label = f"{row['sessionTitle']} · {row['scheduledDate']}"
    data = {"kind": "comment", "sessionSK": session_sk, "athleteSub": athlete_sub}
    if author == "athlete" and coach_sub:
        _push_data(coach_sub, f"{row['authorName']} commented", f"{label}: {text[:120]}", data)
    elif author == "coach":
        _push_data(athlete_sub, f"{COACH_DISPLAY} replied", f"{label}: {text[:120]}", data)
    return row

def _comment_thread(athlete_sub, session_sk):
    r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{athlete_sub}") &
                    Key("SK").begins_with(f"CMT#{_session_uuid(session_sk)}#"))
    out = []
    for c in r.get("Items", []):
        out.append({k: c.get(k) for k in ("SK", "author", "authorName", "text", "createdAt", "readByCoachAt", "readByAthleteAt")})
    return out, r.get("Items", [])


def _race_start(plan_id, race_date, chosen=None):
    meta, sessions = load_plan(plan_id)
    if not meta or not sessions: return None, "plan not found"
    race = datetime.date.fromisoformat(race_date)
    if not meta.get("isFloatingSchedule"):
        max_off = max(int(s["dayOffset"]) for s in sessions)
        return (race - datetime.timedelta(days=max_off)).isoformat(), None
    weeks = {}
    for s in sessions: weeks.setdefault(int(s["weekNumber"]), []).append(s)
    W = max(weeks.keys()); K = max(len(v) for v in weeks.values())
    slots = [int(w) for w in (chosen or [])]
    if len(slots) != K: slots = [2 + (i * 7) // K for i in range(K)]
    rd = ((race.weekday() + 1) % 7) + 1
    final = race - datetime.timedelta(days=(rd - slots[-1]) % 7)
    start = final - datetime.timedelta(days=(W - 1) * 7 + ((slots[-1] - slots[0]) % 7))
    return start.isoformat(), None

def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("task") == "deliverWeeks":
        return _resp(200, sweep_deliveries())
    if isinstance(event, dict) and event.get("task") == "sendNotifications":
        return _resp(200, notification_sweep())
    rk = event.get("routeKey", "")
    try:
        if rk == "POST /devices":
            b = _body(event)
            _arn = PLATFORM_ARN_PROD if b.get("apnsEnv") == "production" else PLATFORM_ARN
            ep = SNS.create_platform_endpoint(
                PlatformApplicationArn=_arn,
                Token=b["token"])["EndpointArn"]
            TABLE.update_item(Key={"PK": f"USER#{_claims(event)['sub']}", "SK": "PROFILE"},
                UpdateExpression="SET endpointArn = :e, deviceToken = :t, tz = :z",
                ExpressionAttributeValues={":e": ep, ":t": b["token"],
                    ":z": b.get("timezone", "America/New_York")})
            return _resp(200, {"ok": True})
        if rk == "POST /me/notification-settings":
            b = _body(event)
            upd, vals = [], {}
            for f, k in [("morningTime", ":m"), ("eveningTime", ":ev"), ("notificationsEnabled", ":en")]:
                if f in b:
                    upd.append(f"{f} = {k}"); vals[k] = b[f]
            if upd:
                TABLE.update_item(Key={"PK": f"USER#{_claims(event)['sub']}", "SK": "PROFILE"},
                    UpdateExpression="SET " + ", ".join(upd),
                    ExpressionAttributeValues=vals)
            return _resp(200, {"ok": True})
        if rk == "POST /signup":
            # PUBLIC self-signup (beta: open registration; add verification pre-launch)
            b = _body(event)
            username = b["username"].strip().lower()
            password = b["password"]
            name = b.get("name", username)
            if len(username) < 3: return _resp(400, {"error": "username too short"})
            # v43-signup-email: collect email so password reset is possible
            _su_email = (b.get("email") or "").strip()
            _su_attrs = [{"Name": "preferred_username", "Value": name}]
            if _su_email:
                _su_attrs += [{"Name": "email", "Value": _su_email},
                              {"Name": "email_verified", "Value": "true"}]
            try:
                u = IDP.admin_create_user(UserPoolId=POOL_ID, Username=username,
                    MessageAction="SUPPRESS",
                    UserAttributes=_su_attrs)  # v43-signup-email
                asub = next(a["Value"] for a in u["User"]["Attributes"] if a["Name"] == "sub")
            except IDP.exceptions.UsernameExistsException:
                return _resp(409, {"error": "username taken"})
            try:  # v45-signup-guard
                IDP.admin_set_user_password(UserPoolId=POOL_ID, Username=username,
                                            Password=password, Permanent=True)
            except IDP.exceptions.InvalidPasswordException:
                IDP.admin_delete_user(UserPoolId=POOL_ID, Username=username)
                return _resp(400, {"error": "Password needs at least 8 characters "
                    "including an uppercase letter, a lowercase letter, and a number."})
            except Exception as e:
                print(f"v45 signup set-pw failed: {e}")
                try: IDP.admin_delete_user(UserPoolId=POOL_ID, Username=username)
                except Exception as e2: print(f"v45 orphan cleanup failed: {e2}")
                return _resp(500, {"error": "Could not finish creating the account. Please try again."})
            TABLE.put_item(Item={"PK": f"USER#{asub}", "SK": "PROFILE", "itemType": "PROFILE",
                "name": name, "username": username, "selfSignup": True,
                **({"email": _su_email} if _su_email else {}),
                "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"username": username, "ok": True})

        if rk == "POST /invites/redeem":
            b = _body(event)
            code = b["code"].strip().upper()
            inv = TABLE.get_item(Key={"PK": f"INVITE#{code}", "SK": "META"}).get("Item")
            if not inv or inv.get("used"): return _resp(404, {"error": "invalid or used code"})
            IDP.admin_set_user_password(UserPoolId=POOL_ID, Username=inv["username"],
                                        Password=b["newPassword"], Permanent=True)
            first = (b.get("firstName") or "").strip()
            last = (b.get("lastName") or "").strip()
            nick = (b.get("nickname") or "").strip()
            email = (b.get("email") or "").strip()
            display = (f"{first} {last}").strip() or nick or inv["username"]
            attrs = []
            if email:
                attrs += [{"Name": "email", "Value": email},
                          {"Name": "email_verified", "Value": "true"}]
            if nick or first:
                attrs.append({"Name": "preferred_username", "Value": nick or display})
            if attrs:
                try:
                    IDP.admin_update_user_attributes(UserPoolId=POOL_ID,
                        Username=inv["username"], UserAttributes=attrs)
                except Exception as e:
                    print(f"attr update skipped: {e}")
            asub = inv.get("athleteSub")
            if asub and (first or last or nick or email):
                sets = ["#n = :n"]; vals = {":n": display}
                for k, v in (("firstName", first), ("lastName", last),
                             ("nickname", nick), ("email", email)):
                    if v:
                        sets.append(f"{k} = :{k}"); vals[f":{k}"] = v
                TABLE.update_item(Key={"PK": f"USER#{asub}", "SK": "PROFILE"},
                    UpdateExpression="SET " + ", ".join(sets),
                    ExpressionAttributeNames={"#n": "name"},
                    ExpressionAttributeValues=vals)
            TABLE.update_item(Key={"PK": f"INVITE#{code}", "SK": "META"},
                UpdateExpression="SET used = :t, redeemedAt = :r",
                ExpressionAttributeValues={":t": True,
                    ":r": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"username": inv["username"], "ok": True})

        claims = _claims(event); sub = claims["sub"]
        _stamp_last_seen(event, sub)  # v58-last-seen

        if rk == "POST /subscriptions":
            b = _body(event)
            start_str = b.get("startDate")
            if b.get("raceDate") and not start_str:
                start_str, rerr = _race_start(b["planId"], b["raceDate"], b.get("chosenWeekdays"))
                if rerr: return _resp(400, {"error": rerr})
            out, err = create_subscription(sub, b["planId"], start_str, b.get("chosenWeekdays"),
                                           race_date=b.get("raceDate"), race_name=b.get("raceName"), txn_id=b.get("transactionId"), product_id=b.get("productId"))
            if not err and b.get("raceDate"): out["computedStart"] = start_str
            return _resp(400, {"error": err}) if err else _resp(200, out)
        if rk == "GET /me/subscriptions":
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                            Key("SK").begins_with("SUB#"))
            return _resp(200, {"subscriptions": r.get("Items", [])})
        if rk == "POST /subscriptions/cancel":
            b = _body(event)
            try:
                TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": f"SUB#{b['planId']}"},
                    UpdateExpression="SET autoRenew = :f",
                    ConditionExpression="attribute_exists(PK)",
                    ExpressionAttributeValues={":f": False})
            except TABLE.meta.client.exceptions.ConditionalCheckFailedException:
                return _resp(404, {"error": "no subscription for that plan"})
            return _resp(200, {"ok": True,
                "note": "access continues until currentPeriodEnd; delivered weeks are yours to keep"})
        if rk == "POST /subscriptions/unsubscribe":
            b = _body(event)
            pid = b["planId"]
            subitem = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": f"SUB#{pid}"}).get("Item")
            if not subitem:
                return _resp(404, {"error": "no subscription for that plan"})
            eid = subitem.get("enrollmentId")
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                            Key("SK").begins_with("SCHED#"))
            removed = 0
            with TABLE.batch_writer() as bw:
                for it in r.get("Items", []):
                    if it.get("enrollmentId") == eid and it.get("logStatus", "pending") == "pending":
                        bw.delete_item(Key={"PK": it["PK"], "SK": it["SK"]})
                        removed += 1
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": f"SUB#{pid}"},
                UpdateExpression="SET #s = :e, autoRenew = :f",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":e": "ended", ":f": False})
            if eid:
                try:
                    TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{eid}"},
                        UpdateExpression="SET #s = :e",
                        ConditionExpression="attribute_exists(PK)",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":e": "ended"})
                except TABLE.meta.client.exceptions.ConditionalCheckFailedException:
                    pass
            return _resp(200, {"ok": True, "sessionsRemoved": removed,
                "note": "logged sessions kept; future sessions removed from your calendar"})

        if rk == "POST /me/link-coach":
            b = _body(event)
            code = b["code"].strip().upper()
            inv = TABLE.get_item(Key={"PK": f"INVITE#{code}", "SK": "META"}).get("Item")
            if not inv or inv.get("used"): return _resp(404, {"error": "invalid or used code"})
            coach = inv.get("coachSub")
            if not coach: return _resp(400, {"error": "code has no coach attached"})
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "PROFILE"},
                UpdateExpression="SET itemType = :it, GSI1PK = :g1, GSI1SK = :g2",
                ExpressionAttributeValues={":it": "PROFILE",
                    ":g1": f"COACH#{coach}", ":g2": f"ATHLETE#{sub}#PROFILE"})
            TABLE.update_item(Key={"PK": f"INVITE#{code}", "SK": "META"},
                UpdateExpression="SET used = :t, redeemedAt = :r, linkedSub = :s",
                ExpressionAttributeValues={":t": True, ":s": sub,
                    ":r": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"ok": True, "linked": True})
        if rk == "POST /enrollments":
            b = _body(event)
            start_str = b.get("startDate")
            if b.get("raceDate") and not start_str:
                start_str, rerr = _race_start(b["planId"], b["raceDate"], b.get("chosenWeekdays"))
                if rerr: return _resp(400, {"error": rerr})
            out, err = materialize(sub, b["planId"], start_str, b.get("chosenWeekdays"), False, txn_id=b.get("transactionId"), product_id=b.get("productId"))
            if err: return _resp(404, {"error": err})
            if b.get("raceDate"):
                out["computedStart"] = start_str
                upd = "SET raceDate = :r"; vals = {":r": b["raceDate"]}
                if b.get("raceName"):
                    upd += ", raceName = :n"; vals[":n"] = b["raceName"]
                TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{out['enrollmentId']}"},
                    UpdateExpression=upd, ExpressionAttributeValues=vals)
            return _resp(200, out)
        if rk == "POST /me/sessions/delete":
            b = _body(event)
            dsk = b.get("sessionSK") or ""
            if not dsk.startswith("SCHED#"):
                return _resp(400, {"error": "sessionSK required"})
            it = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": dsk}).get("Item")
            if not it:
                return _resp(404, {"error": "not found"})
            if it.get("planId") != "my-workouts" or it.get("coachManaged"):
                return _resp(403, {"error": "only self-logged workouts can be deleted"})
            TABLE.delete_item(Key={"PK": f"USER#{sub}", "SK": dsk},
                ConditionExpression="attribute_exists(PK)")
            return _resp(200, {"ok": True, "deleted": dsk})
        if rk == "POST /me/sessions":
            b = _body(event)
            date = b.get("date"); raw = (b.get("rawText") or "").strip()
            if not date or not raw: return _resp(400, {"error": "date and rawText required"})
            st, bench = stype_bench(raw)
            if b.get("sessionType") in ("run", "swim", "bike", "row", "brick", "strength", "rest", "conditioning"):
                st = b["sessionType"]
            title = (b.get("title") or "").strip()[:60] or athlete_title(None, "", st, raw)
            eid = None
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                            Key("SK").begins_with("ENROLLMENT#"))
            for e in r.get("Items", []):
                if e.get("planId") == "my-workouts" and e.get("status") == "active":
                    eid = e["enrollmentId"]; break
            if not eid:
                eid = str(uuid.uuid4())
                TABLE.put_item(Item={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{eid}",
                    "itemType": "ENROLLMENT", "enrollmentId": eid, "planId": "my-workouts",
                    "planTitle": "My Workouts", "startDate": date, "status": "active",
                    "coachManaged": False,
                    "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            sk = f"SCHED#{date}#{uuid.uuid4()}"
            item = {"PK": f"USER#{sub}", "SK": sk,
                "itemType": "SCHED", "enrollmentId": eid, "planId": "my-workouts",
                "planTitle": "My Workouts", "scheduledDate": date, "title": title,
                "coachTitle": f"Athlete add · {date}", "sessionType": st, "rawText": raw,
                "prescriptions": [parse_cell(raw)], "coachManaged": False,
                "logStatus": "pending"}
            _cp = b.get("prescriptions")
            if isinstance(_cp, list) and _cp:
                for _i, _p in enumerate(_cp):
                    if isinstance(_p, dict) and "order" not in _p:
                        _p["order"] = _i
                item["prescriptions"] = _cp
            _pr = b.get("pieceResults")
            if isinstance(_pr, list) and _pr:
                item["logPieceResults"] = _pr
            if isinstance(b.get("warmupDone"), bool):
                item["logWarmupDone"] = b["warmupDone"]
            if isinstance(b.get("postworkDone"), bool):
                item["logPostworkDone"] = b["postworkDone"]
            ls = b.get("logStatus")
            if ls in ("complete", "missed"):
                item["logStatus"] = ls
                item["loggedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                if b.get("logResultText"): item["logResultText"] = str(b["logResultText"])[:2000]
                if b.get("logComment"): item["logComment"] = str(b["logComment"])[:2000]
                if b.get("logTimeSeconds"):
                    try: item["logTimeSeconds"] = int(b["logTimeSeconds"])
                    except Exception: pass
                if b.get("logRPE"):
                    try: item["logRPE"] = int(b["logRPE"])
                    except Exception: pass
            TABLE.put_item(Item=item)
            if item.get("logComment"):   # v59.3: self-logged note starts the comment thread too
                try: _comment_add(sub, sk, "athlete", sub, item["logComment"], session=item)
                except Exception as e: print(f"[v59.3] self-log->comment failed: {e}")
            return _resp(200, {"ok": True, "enrollmentId": eid, "title": title,
                               "sessionType": st, "sk": sk, "date": date,
                               "logged": ls in ("complete", "missed")})
        if rk == "POST /me/profile":
            b = _body(event)
            allowed = {k: str(b[k])[:80] for k in ("nickname", "firstName", "lastName", "email") if k in b}  # v44-profile-email
            if not allowed: return _resp(400, {"error": "nothing to update"})
            upd = ", ".join(f"#{k} = :{k}" for k in allowed)
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "PROFILE"},
                UpdateExpression=f"SET {upd}",
                ExpressionAttributeNames={f"#{k}": k for k in allowed},
                ExpressionAttributeValues={f":{k}": v for k, v in allowed.items()})
            _pe = (allowed.get("email") or "").strip()
            if _pe:
                try:
                    IDP.admin_update_user_attributes(UserPoolId=POOL_ID,
                        Username=claims.get("cognito:username") or claims.get("username") or sub,
                        UserAttributes=[{"Name": "email", "Value": _pe},
                                        {"Name": "email_verified", "Value": "true"}])
                except Exception as e:
                    print(f"v44 email attr update failed: {e}")
            return _resp(200, {"ok": True, "updated": list(allowed)})
        if rk == "POST /me/enrollments/leave":  # v47-leave-plan
            b = _body(event)
            eid = b.get("enrollmentId")
            if not eid: return _resp(400, {"error": "enrollmentId required"})
            er2 = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{eid}"}).get("Item")
            if not er2: return _resp(404, {"error": "enrollment not found"})
            if er2.get("coachManaged"): return _resp(403, {"error": "coach-managed plans can't be left here"})
            today_iso = _today().isoformat()
            r2 = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                             Key("SK").begins_with("SCHED#"))
            doomed = [s for s in r2.get("Items", [])
                      if s.get("enrollmentId") == eid
                      and (s.get("scheduledDate") or "") >= today_iso
                      and s.get("logStatus") not in ("complete", "missed")
                      and not s.get("logPieceResults") and not s.get("photoKeys")]
            if len(doomed) > 400: return _resp(400, {"error": "safety cap exceeded"})
            for s in doomed:
                TABLE.delete_item(Key={"PK": s["PK"], "SK": s["SK"]},
                    ConditionExpression="attribute_not_exists(logPieceResults) AND "
                        "(attribute_not_exists(logStatus) OR logStatus = :p)",
                    ExpressionAttributeValues={":p": "pending"})
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": f"ENROLLMENT#{eid}"},
                UpdateExpression="SET #st = :s, leftAt = :t",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={":s": "left",
                    ":t": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"ok": True, "removed": len(doomed)})
        if rk == "POST /me/account/delete":  # v48-account-delete
            b = _body(event)
            if b.get("confirm") != "DELETE":
                return _resp(400, {"error": "confirmation required"})
            # 1. every DynamoDB item under this user
            _dd = 0; _lek = None
            while True:
                _kw = {"KeyConditionExpression": Key("PK").eq(f"USER#{sub}")}
                if _lek: _kw["ExclusiveStartKey"] = _lek
                _r = TABLE.query(**_kw)
                for _it in _r.get("Items", []):
                    TABLE.delete_item(Key={"PK": _it["PK"], "SK": _it["SK"]}); _dd += 1
                _lek = _r.get("LastEvaluatedKey")
                if not _lek: break
            # 2. photos prefix in S3
            _s3 = boto3.client("s3"); _pd = 0
            try:
                _pg = _s3.get_paginator("list_objects_v2")
                for _pgr in _pg.paginate(Bucket="getfit-photos-446100759389", Prefix=f"photos/{sub}/"):
                    _ks = [{"Key": o["Key"]} for o in _pgr.get("Contents", [])]
                    if _ks:
                        _s3.delete_objects(Bucket="getfit-photos-446100759389", Delete={"Objects": _ks})
                        _pd += len(_ks)
            except Exception as e:
                print(f"v48 photo cleanup failed: {e}")
            # 3. the Cognito user
            _un = claims.get("cognito:username") or claims.get("username") or sub
            try:
                IDP.admin_delete_user(UserPoolId=POOL_ID, Username=_un)
            except Exception as e:
                print(f"v48 cognito delete failed: {e}")
                return _resp(500, {"error": "account data removed but sign-in deletion failed; contact support"})
            return _resp(200, {"ok": True, "itemsDeleted": _dd, "photosDeleted": _pd})
        if rk == "GET /me/enrollments":
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                            Key("SK").begins_with("ENROLLMENT#"))
            return _resp(200, {"enrollments": r.get("Items", [])})
        if rk == "GET /me/schedule":
            q = event.get("queryStringParameters") or {}
            items, count = _sched_q(sub, q.get("from"), q.get("to"))
            er = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                             Key("SK").begins_with("ENROLLMENT#"))
            managed = {e.get("enrollmentId") for e in er.get("Items", [])
                       if e.get("coachManaged") and not e.get("isSubscription")}
            if managed:
                today = _today()
                horizon = (today + datetime.timedelta(days=(6 - today.weekday()) % 7)).isoformat()   # v60.2: through Sunday
                items = [s for s in items
                         if s.get("enrollmentId") not in managed
                         or (s.get("scheduledDate") or "") <= horizon]
                count = len(items)
            # v46-week-mask: marketplace/subscription content locks beyond this week;
            # the plan's first week always unlocks. Dots+titles survive; content strips.
            _mk = {}
            for e in er.get("Items", []):
                pid = e.get("planId")
                if e.get("coachManaged") or pid in ("my-workouts", "coach-extras"): continue
                _mk[e.get("enrollmentId")] = str(e.get("startDate") or e.get("firstSession")
                    or e.get("firstSessionDate") or "")[:10]
            if _mk:
                _t2 = _today()
                _hz = (_t2 + datetime.timedelta(days=(6 - _t2.weekday()) % 7)).isoformat()   # v60.2: through Sunday
                for s in items:
                    _eid = s.get("enrollmentId")
                    if _eid not in _mk: continue
                    _sd = (s.get("scheduledDate") or "")[:10]
                    if not _sd or _sd <= _hz: continue
                    _f = _mk[_eid]
                    if _f:
                        try:
                            _fd = datetime.date.fromisoformat(_f)
                            _w1 = (_fd + datetime.timedelta(days=(6 - _fd.weekday()) % 7)).isoformat()   # v60.2
                            if _sd <= _w1: continue
                        except ValueError: pass
                    s.pop("rawText", None); s.pop("prescriptions", None)
                    s["locked"] = True
            return _resp(200, {"sessions": items, "count": count})
        if rk == "GET /me/comments":   # v59
            q = event.get("queryStringParameters") or {}
            ssk = q.get("sessionSK") or ""
            if not ssk.startswith("SCHED#"): return _resp(400, {"error": "sessionSK required"})
            thread, raw = _comment_thread(sub, ssk)
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for c in raw:   # mark coach messages read by the athlete
                if c.get("author") == "coach" and not c.get("readByAthleteAt"):
                    TABLE.update_item(Key={"PK": c["PK"], "SK": c["SK"]}, UpdateExpression="SET readByAthleteAt = :t",
                                      ExpressionAttributeValues={":t": now})
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": ssk}, UpdateExpression="SET athleteUnread = :z",
                              ConditionExpression="attribute_exists(SK)", ExpressionAttributeValues={":z": 0})
            return _resp(200, {"sessionSK": ssk, "comments": thread})
        if rk == "POST /me/comments":   # v59
            b = _body(event)
            ssk = b.get("sessionSK") or ""
            if not ssk.startswith("SCHED#"): return _resp(400, {"error": "sessionSK required"})
            row = _comment_add(sub, ssk, "athlete", sub, b.get("text"))
            if not row: return _resp(404, {"error": "session not found or empty text"})
            return _resp(200, {"ok": True, "comment": {k: row.get(k) for k in ("SK", "author", "authorName", "text", "createdAt")}})
        if rk == "POST /logs":
            b = _body(event)
            prev = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": b["sessionSK"]}, ProjectionExpression="logComment").get("Item") or {}  # v59
            upd, vals = ["logStatus = :s", "loggedAt = :t"], {
                ":s": b.get("status", "complete"),
                ":t": datetime.datetime.now(datetime.timezone.utc).isoformat()}
            for f, k in [("comment", ":c"), ("resultText", ":r"), ("timeSeconds", ":ts"), ("photoKeys", ":p")]:
                if f in b:
                    upd.append(f"log{f[0].upper()}{f[1:]} = {k}"); vals[k] = b[f]
            TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": b["sessionSK"]},
                              UpdateExpression="SET " + ", ".join(upd),
                              ExpressionAttributeValues=vals)
            c = (b.get("comment") or "").strip()   # v59: the log note is message #0 of the thread
            if c and c != (prev.get("logComment") or "").strip():
                try: _comment_add(sub, b["sessionSK"], "athlete", sub, c)
                except Exception as e: print(f"[v59] log->comment failed: {e}")
            return _resp(200, {"ok": True})
        if rk == "POST /sessions/move":
            b = _body(event)
            nsk = _move(sub, b["sessionSK"], b["newDate"])
            return _resp(404, {"error": "not found"}) if not nsk else _resp(200, {"ok": True, "newSessionSK": nsk})
        if rk == "GET /me/profile":   # v60.3
            prof = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": "PROFILE"}).get("Item") or {}
            out = {k: prof.get(k, "") for k in ("firstName", "lastName", "nickname", "email", "username", "name")}
            out["coached"] = bool((prof.get("GSI1PK") or "").startswith("COACH#"))
            out["photoUrl"] = _photo_url(prof.get("photoKey"))
            out["tags"] = [str(t) for t in (prof.get("tags") or [])]
            return _resp(200, out)
        if rk == "POST /photos/presign":
            b = _body(event)
            if b.get("kind") == "profile":   # v60.3: one fixed key per athlete; overwrite on re-upload
                key = f"profile/{sub}.jpg"
                up = S3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key,
                    "ContentType": b.get("contentType", "image/jpeg")}, ExpiresIn=900)
                TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "PROFILE"}, UpdateExpression="SET photoKey = :k",
                                  ExpressionAttributeValues={":k": key})
                return _resp(200, {"uploadUrl": up, "key": key})
            key = f"photos/{sub}/{uuid.uuid4()}.jpg"
            up = S3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key,
                "ContentType": b.get("contentType", "image/jpeg")}, ExpiresIn=900)
            return _resp(200, {"uploadUrl": up, "key": key})
        if rk == "POST /photos/delete":
            b = _body(event)
            k = b.get("key") or ""
            if not k.startswith(f"photos/{sub}/"):
                return _resp(403, {"error": "you can only delete your own photos"})
            S3.delete_object(Bucket=BUCKET, Key=k)
            return _resp(200, {"ok": True, "deleted": k})
        if rk == "POST /photos/view":
            b = _body(event)
            if not (b.get("key", "").startswith(f"photos/{sub}/") or b.get("key") == f"profile/{sub}.jpg") and not _is_coach(event):  # v49-photo-owner, v60.3 profile
                return _resp(403, {"error": "you can only view your own photos"})
            return _resp(200, {"viewUrl": S3.generate_presigned_url("get_object",
                Params={"Bucket": BUCKET, "Key": b["key"]}, ExpiresIn=3600)})

        if rk.startswith("GET /coach") or rk.startswith("POST /coach"):
            if not _is_coach(event): return _resp(403, {"error": "coaches only"})
        if rk == "POST /coach/athletes/photo":   # v60.3 — coach uploads a photo for an athlete
            b = _body(event); a = b.get("athleteSub") or ""
            if not a: return _resp(400, {"error": "athleteSub required"})
            key = f"profile/{a}.jpg"
            up = S3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key,
                "ContentType": b.get("contentType", "image/jpeg")}, ExpiresIn=900)
            TABLE.update_item(Key={"PK": f"USER#{a}", "SK": "PROFILE"}, UpdateExpression="SET photoKey = :k",
                              ConditionExpression="attribute_exists(SK)", ExpressionAttributeValues={":k": key})
            return _resp(200, {"uploadUrl": up, "key": key})
        if rk == "GET /coach/comments/inbox":   # v59 — athlete messages the coach hasn't opened, newest first
            r = TABLE.query(IndexName="GSI1", KeyConditionExpression=Key("GSI1PK").eq(f"CMTINBOX#{sub}"),
                            ScanIndexForward=False, Limit=100)
            out = []
            for c in r.get("Items", []):
                out.append({"athleteSub": c["PK"].replace("USER#", ""), "authorName": c.get("authorName", ""),
                            "sessionSK": c.get("sessionSK"), "sessionTitle": c.get("sessionTitle", ""),
                            "scheduledDate": c.get("scheduledDate", ""), "planTitle": c.get("planTitle", ""),
                            "text": c.get("text", ""), "createdAt": c.get("createdAt", "")})
            return _resp(200, {"unread": out, "count": len(out)})
        if rk == "GET /coach/comments":   # v59 — full thread; opening it clears those messages from the inbox
            q = event.get("queryStringParameters") or {}
            a, ssk = q.get("athleteSub") or "", q.get("sessionSK") or ""
            if not a or not ssk.startswith("SCHED#"): return _resp(400, {"error": "athleteSub and sessionSK required"})
            thread, raw = _comment_thread(a, ssk)
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for c in raw:
                if c.get("author") == "athlete" and not c.get("readByCoachAt"):
                    TABLE.update_item(Key={"PK": c["PK"], "SK": c["SK"]},
                        UpdateExpression="SET readByCoachAt = :t REMOVE GSI1PK, GSI1SK", ExpressionAttributeValues={":t": now})
            sess = TABLE.get_item(Key={"PK": f"USER#{a}", "SK": ssk}).get("Item") or {}
            return _resp(200, {"athleteSub": a, "sessionSK": ssk, "comments": thread,
                               "session": {k: sess.get(k) for k in ("title", "coachTitle", "planTitle", "scheduledDate", "logStatus",
                                           "logResultText", "logComment", "logTimeSeconds", "loggedAt", "rawText")}})
        if rk == "POST /coach/comments":   # v59
            b = _body(event)
            a, ssk = b.get("athleteSub") or "", b.get("sessionSK") or ""
            if not a or not ssk.startswith("SCHED#"): return _resp(400, {"error": "athleteSub and sessionSK required"})
            row = _comment_add(a, ssk, "coach", sub, b.get("text"))
            if not row: return _resp(404, {"error": "session not found or empty text"})
            return _resp(200, {"ok": True, "comment": {k: row.get(k) for k in ("SK", "author", "authorName", "text", "createdAt")}})
        if rk == "POST /coach/sessions/update":
            b = _body(event)
            a = b["athleteSub"]; sk = b["sessionSK"]
            item = TABLE.get_item(Key={"PK": f"USER#{a}", "SK": sk}).get("Item")
            if not item: return _resp(404, {"error": "session not found"})
            if b.get("delete"):
                TABLE.delete_item(Key={"PK": f"USER#{a}", "SK": sk})
                if b.get("notify"):
                    _notify(a, "Schedule updated",
                            f"{item.get('title','A workout')} on {item.get('scheduledDate','')} was removed by your coach")
                return _resp(200, {"ok": True, "deleted": True})
            sets = []; vals = {}
            if "rawText" in b:
                new_raw = b["rawText"]
                st, bench = stype_bench(new_raw)
                sets += ["rawText = :r", "prescriptions = :p", "sessionType = :st"]
                vals[":r"] = new_raw; vals[":p"] = [parse_cell(new_raw)]; vals[":st"] = st
                item["rawText"] = new_raw
            # v42-movement-fix: body sessionType wins over stype_bench guess;
            # prescriptions carry the final type's movement
            _bst = b.get("sessionType")
            if _bst in ("run", "swim", "bike", "row", "brick", "strength", "rest", "conditioning"):
                if "rawText" in b:
                    vals[":st"] = _bst
                else:
                    sets.append("sessionType = :st"); vals[":st"] = _bst
            _fin = vals.get(":st") or item.get("sessionType")
            _mv = {"swim": "Swim", "bike": "Bike", "row": "Row", "brick": "Brick", "strength": "Strength",
                   "conditioning": "Conditioning"}.get(_fin)
            if _mv and ":p" in vals:
                for _pp in vals[":p"]:
                    if _pp.get("movement") in (None, "Run"):
                        _pp["movement"] = _mv
            if b.get("title"):
                sets += ["title = :t", "coachTitle = :t"]
                vals[":t"] = b["title"]
                item["title"] = b["title"]
            if sets:
                sets.append("coachEdited = :ce"); vals[":ce"] = True
                TABLE.update_item(Key={"PK": f"USER#{a}", "SK": sk},
                    UpdateExpression="SET " + ", ".join(sets),
                    ExpressionAttributeValues=vals)
            new_sk = None
            if b.get("newDate") and b["newDate"] != item.get("scheduledDate"):
                new_sk = _move(a, sk, b["newDate"])
            if b.get("notify"):
                _notify(a, "Coach updated your workout",
                        f"{item.get('title','Workout')} \u00b7 {b.get('newDate') or item.get('scheduledDate','')}")
            return _resp(200, {"ok": True, "newSessionSK": new_sk})
        if rk == "POST /coach/sessions/create":
            b = _body(event)
            a = b["athleteSub"]; date = b["date"]; raw = b["rawText"]
            st, bench = stype_bench(raw)
            if b.get("sessionType") in ("run", "swim", "bike", "row", "brick", "strength", "rest", "conditioning"):
                st = b["sessionType"]
            title = b.get("title") or athlete_title(None, "", st, raw)
            eid = None
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{a}") &
                            Key("SK").begins_with("ENROLLMENT#"))
            for e in r.get("Items", []):
                if e.get("planId") == "coach-extras" and e.get("status") == "active":
                    eid = e["enrollmentId"]; break
            if not eid:
                eid = str(uuid.uuid4())
                TABLE.put_item(Item={"PK": f"USER#{a}", "SK": f"ENROLLMENT#{eid}",
                    "itemType": "ENROLLMENT", "enrollmentId": eid, "planId": "coach-extras",
                    "planTitle": "Coach Extras", "startDate": date, "status": "active",
                    "coachManaged": True, "GSI1PK": f"COACH#{sub}",
                    "GSI1SK": f"ATHLETE#{a}#ENROLLMENT#{eid}",
                    "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            TABLE.put_item(Item={"PK": f"USER#{a}", "SK": f"SCHED#{date}#{uuid.uuid4()}",
                "itemType": "SCHED", "enrollmentId": eid, "planId": "coach-extras",
                "planTitle": "Coach Extras", "scheduledDate": date, "title": title,
                "coachTitle": f"Coach add \u00b7 {date}", "sessionType": st, "rawText": raw,
                "prescriptions": [parse_cell(raw)], "coachManaged": True, "logStatus": "pending"})
            if b.get("notify"):
                _notify(a, "New workout from Coach Greg", f"{title} \u00b7 {date}")
            return _resp(200, {"ok": True, "enrollmentId": eid, "title": title})
        if rk == "GET /coach/results":
            q = event.get("queryStringParameters") or {}
            a = q.get("athleteSub")
            if not a: return _resp(400, {"error": "athleteSub required"})
            items, _cnt = _sched_q(a, q.get("from"), q.get("to"))
            done = [i for i in items if i.get("logStatus") in ("complete", "missed")]
            done.sort(key=lambda x: str(x.get("loggedAt", x.get("scheduledDate",""))), reverse=True)
            keep = ("SK","scheduledDate","title","planTitle","logStatus","logResultText",
                    "logComment","logTimeSeconds","logPhotoKeys","loggedAt","rawText")
            return _resp(200, {"results": [{k: i.get(k) for k in keep if k in i} for i in done[:100]]})
        if rk == "GET /coach/benchmarks":
            q = event.get("queryStringParameters") or {}
            a = q.get("athleteSub")
            if not a: return _resp(400, {"error": "athleteSub required"})
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{a}") &
                            Key("SK").begins_with("BENCH#"))
            bench = sorted(r.get("Items", []), key=lambda x: str(x.get("dateISO","")), reverse=True)
            return _resp(200, {"benchmarks": bench})
        if rk == "GET /me/benchmarks":
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                            Key("SK").begins_with("BENCH#"))
            bench = sorted(r.get("Items", []), key=lambda x: str(x.get("dateISO","")), reverse=True)
            return _resp(200, {"benchmarks": bench})
        if rk == "POST /me/benchmarks":
            b = _body(event)
            movement = b["movement"]
            date_iso = b.get("dateISO") or datetime.date.today().isoformat()
            TABLE.put_item(Item={"PK": f"USER#{sub}", "SK": f"BENCH#{movement}#{date_iso}",
                "itemType": "BENCH", "movement": movement, "kind": b.get("kind","oneRepMax"),
                "value": b["value"], "unit": b.get("unit",""),
                "enteredValue": b.get("enteredValue"), "enteredReps": b.get("enteredReps"),
                "dateISO": date_iso, "source": b.get("source","app"),
                "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"ok": True})
        if rk == "DELETE /me/benchmarks":  # v56-benchmark-delete
            b = _body(event)
            movement = b.get("movement")
            date_iso = b.get("dateISO")
            if not movement or not date_iso:
                return _resp(400, {"error": "movement and dateISO required"})
            TABLE.delete_item(Key={"PK": f"USER#{sub}", "SK": f"BENCH#{movement}#{date_iso}"})
            return _resp(200, {"ok": True, "deleted": f"BENCH#{movement}#{date_iso}"})
        if rk == "GET /coach/enrollments":
            q = event.get("queryStringParameters") or {}
            a = q.get("athleteSub")
            if not a: return _resp(400, {"error": "athleteSub required"})
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{a}") &
                            Key("SK").begins_with("ENROLLMENT#"))
            return _resp(200, {"enrollments": r.get("Items", [])})
        if rk == "GET /coach/invites":
            r = TABLE.scan(FilterExpression="itemType = :t AND coachSub = :c AND #u = :f",
                ExpressionAttributeNames={"#u": "used"},
                ExpressionAttributeValues={":t": "INVITE", ":c": sub, ":f": False})
            inv = [{"code": i["PK"].replace("INVITE#", ""), "username": i.get("username", ""),
                    "createdAt": i.get("createdAt", "")} for i in r.get("Items", [])]
            inv.sort(key=lambda x: x["createdAt"], reverse=True)
            return _resp(200, {"invites": inv})
        if rk == "POST /coach/enrollments/end":
            b = _body(event)
            a = b["athleteSub"]; eid = b["enrollmentId"]
            enr = TABLE.get_item(Key={"PK": f"USER#{a}", "SK": f"ENROLLMENT#{eid}"}).get("Item")
            if not enr: return _resp(404, {"error": "enrollment not found"})
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{a}") &
                            Key("SK").begins_with("SCHED#"))
            removed = 0
            with TABLE.batch_writer() as bw:
                for it in r.get("Items", []):
                    if it.get("enrollmentId") == eid and it.get("logStatus", "pending") == "pending":
                        bw.delete_item(Key={"PK": it["PK"], "SK": it["SK"]})
                        removed += 1
            TABLE.update_item(Key={"PK": f"USER#{a}", "SK": f"ENROLLMENT#{eid}"},
                UpdateExpression="SET #s = :e", ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":e": "ended"})
            pid = enr.get("planId")
            if pid:
                subitem = TABLE.get_item(Key={"PK": f"USER#{a}", "SK": f"SUB#{pid}"}).get("Item")
                if subitem and subitem.get("enrollmentId") == eid and subitem.get("status") == "active":
                    TABLE.update_item(Key={"PK": f"USER#{a}", "SK": f"SUB#{pid}"},
                        UpdateExpression="SET #s = :e, autoRenew = :f",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":e": "ended", ":f": False})
            if b.get("notify"):
                _notify(a, "Training plan ended",
                        f"{enr.get('planTitle','A plan')} was ended by your coach - logged work stays on your calendar")
            return _resp(200, {"ok": True, "sessionsRemoved": removed})
        if rk == "POST /coach/athletes/tags":   # v59.4 — coach-defined groups on the athlete profile
            b = _body(event)
            a = b.get("athleteSub") or ""
            tags = [str(t).strip()[:40] for t in (b.get("tags") or []) if str(t).strip()]
            tags = list(dict.fromkeys(tags))[:20]
            if not a: return _resp(400, {"error": "athleteSub required"})
            TABLE.update_item(Key={"PK": f"USER#{a}", "SK": "PROFILE"},
                UpdateExpression="SET tags = :t", ConditionExpression="attribute_exists(SK)",
                ExpressionAttributeValues={":t": tags})
            return _resp(200, {"ok": True, "tags": tags})
        if rk == "POST /coach/athletes/archive":
            b = _body(event)
            a = b["athleteSub"]; arch = bool(b.get("archive", True))
            prof = TABLE.get_item(Key={"PK": f"USER#{a}", "SK": "PROFILE"}).get("Item") or {}
            uname = prof.get("username")
            if not uname: return _resp(404, {"error": "athlete profile not found"})
            if arch: IDP.admin_disable_user(UserPoolId=POOL_ID, Username=uname)
            else: IDP.admin_enable_user(UserPoolId=POOL_ID, Username=uname)
            TABLE.update_item(Key={"PK": f"USER#{a}", "SK": "PROFILE"},
                UpdateExpression="SET archived = :v",
                ExpressionAttributeValues={":v": arch})
            return _resp(200, {"ok": True, "archived": arch})
        if rk == "POST /coach/plans/rename":
            b = _body(event)
            pid = b["planId"]; new_title = (b.get("title") or "").strip()
            if not new_title: return _resp(400, {"error": "title required"})
            meta = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": "META"}).get("Item")
            if not meta: return _resp(404, {"error": "plan not found"})
            TABLE.update_item(Key={"PK": f"PLAN#{pid}", "SK": "META"},
                UpdateExpression="SET title = :t, GSI1SK = :g",
                ExpressionAttributeValues={":t": new_title, ":g": f"PLAN#{new_title}"})
            n = 0
            scan_kwargs = {"FilterExpression": "planId = :p AND attribute_exists(planTitle)",
                           "ExpressionAttributeValues": {":p": pid}}
            while True:
                r = TABLE.scan(**scan_kwargs)
                for it in r.get("Items", []):
                    TABLE.update_item(Key={"PK": it["PK"], "SK": it["SK"]},
                        UpdateExpression="SET planTitle = :t",
                        ExpressionAttributeValues={":t": new_title})
                    n += 1
                if "LastEvaluatedKey" not in r: break
                scan_kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
            return _resp(200, {"ok": True, "itemsUpdated": n})
        if rk == "GET /coach/races":
            q = event.get("queryStringParameters") or {}
            a = q.get("athleteSub")
            if not a: return _resp(400, {"error": "athleteSub required"})
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{a}") &
                            Key("SK").begins_with("RACE#"))
            races = [{"raceDate": it.get("raceDate"), "raceName": it.get("raceName"),
                      "source": "coach"} for it in r.get("Items", [])]
            races.sort(key=lambda x: x.get("raceDate") or "9999")
            return _resp(200, {"races": races})
        if rk == "POST /coach/races/add":
            b = _body(event)
            a, d, n = b.get("athleteSub"), b.get("date"), (b.get("name") or "Race").strip()[:60]
            if not a or not d: return _resp(400, {"error": "athleteSub and date required"})
            TABLE.put_item(Item={"PK": f"USER#{a}", "SK": f"RACE#{d}", "itemType": "RACE",
                "raceDate": d, "raceName": n,
                "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            return _resp(200, {"ok": True})
        if rk == "POST /coach/races/remove":
            b = _body(event)
            a, d = b.get("athleteSub"), b.get("date")
            if not a or not d: return _resp(400, {"error": "athleteSub and date required"})
            TABLE.delete_item(Key={"PK": f"USER#{a}", "SK": f"RACE#{d}"})
            return _resp(200, {"ok": True})
        if rk == "POST /coach/athletes/delete":
            b = _body(event)
            asub = b.get("athleteSub", "")
            if not asub: return _resp(400, {"error": "athleteSub required"})
            if asub == sub: return _resp(400, {"error": "cannot delete your own coach account"})
            uname = None
            r = IDP.list_users(UserPoolId=POOL_ID, Filter=f'sub = "{asub}"', Limit=1)
            if r.get("Users"): uname = r["Users"][0]["Username"]
            n_items = 0
            lek = None
            while True:
                kw = {"KeyConditionExpression": Key("PK").eq(f"USER#{asub}")}
                if lek: kw["ExclusiveStartKey"] = lek
                q = TABLE.query(**kw)
                with TABLE.batch_writer() as bw:
                    for it in q.get("Items", []):
                        bw.delete_item(Key={"PK": it["PK"], "SK": it["SK"]}); n_items += 1
                lek = q.get("LastEvaluatedKey")
                if not lek: break
            n_inv = 0
            if uname:
                sc = TABLE.scan(FilterExpression="itemType = :t AND username = :u",
                                ExpressionAttributeValues={":t": "INVITE", ":u": uname})
                with TABLE.batch_writer() as bw:
                    for it in sc.get("Items", []):
                        bw.delete_item(Key={"PK": it["PK"], "SK": it["SK"]}); n_inv += 1
                try: IDP.admin_delete_user(UserPoolId=POOL_ID, Username=uname)
                except Exception as e: print(f"cognito delete: {e}")
            return _resp(200, {"ok": True, "username": uname, "itemsDeleted": n_items, "invitesDeleted": n_inv})
        if rk == "GET /coach/feed":
            cut = (_today() - datetime.timedelta(days=14)).isoformat()
            out, lek = [], None
            while True:
                kw = {"FilterExpression": "begins_with(SK, :s) AND attribute_exists(loggedAt) AND loggedAt >= :c",
                      "ExpressionAttributeValues": {":s": "SCHED#", ":c": cut}}
                if lek: kw["ExclusiveStartKey"] = lek
                r = TABLE.scan(**kw)
                for s in r.get("Items", []):
                    out.append({"sub": s["PK"].replace("USER#", ""), "SK": s["SK"],   # v59.2
                        "title": s.get("coachTitle") or s.get("title", ""),
                        "planTitle": s.get("planTitle", ""),
                        "scheduledDate": s.get("scheduledDate", ""),
                        "loggedAt": s.get("loggedAt", ""),
                        "logStatus": s.get("logStatus", ""),
                        "logTimeSeconds": s.get("logTimeSeconds"),
                        "logResultText": (s.get("logResultText") or "")[:300],
                        "logComment": (s.get("logComment") or "")[:300],
                        "logPhotoKeys": s.get("logPhotoKeys") or [],
                        "commentCount": int(s.get("commentCount") or 0), "lastCommentBy": s.get("lastCommentBy")})  # v59
                lek = r.get("LastEvaluatedKey")
                if not lek: break
            out.sort(key=lambda x: x["loggedAt"], reverse=True)
            out = out[:50]
            for _r in out:
                _ks = _r.get("logPhotoKeys") or []
                _r["photoUrls"] = [S3.generate_presigned_url("get_object",
                    Params={"Bucket": BUCKET, "Key": _k}, ExpiresIn=21600)
                    for _k in _ks[:4]]
            return _resp(200, {"results": out})
        if rk == "GET /coach/plans/usage":
            usage, lek = {}, None
            while True:
                kw = {"FilterExpression": "begins_with(SK, :e) AND #st = :a",
                      "ExpressionAttributeNames": {"#st": "status"},
                      "ExpressionAttributeValues": {":e": "ENROLLMENT#", ":a": "active"}}
                if lek: kw["ExclusiveStartKey"] = lek
                r = TABLE.scan(**kw)
                for e in r.get("Items", []):
                    pid = e.get("planId")
                    if not pid: continue
                    u = usage.setdefault(pid, {"assigned": 0, "athlete": 0})
                    u["assigned" if e.get("coachManaged") else "athlete"] += 1
                lek = r.get("LastEvaluatedKey")
                if not lek: break
            return _resp(200, {"usage": usage})
        if rk == "GET /coach/athletes/all":
            users, pag = [], None
            while True:
                kw = {"UserPoolId": POOL_ID, "Limit": 60}
                if pag: kw["PaginationToken"] = pag
                r = IDP.list_users(**kw)
                users += r.get("Users", [])
                pag = r.get("PaginationToken")
                if not pag: break
            today_s = _today().isoformat()
            out = []
            for u in users:
                attrs = {a["Name"]: a["Value"] for a in u.get("Attributes", [])}
                asub = attrs.get("sub")
                if not asub: continue
                prof = TABLE.get_item(Key={"PK": f"USER#{asub}", "SK": "PROFILE"}).get("Item") or {}
                enr = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{asub}") &
                                  Key("SK").begins_with("ENROLLMENT#")).get("Items", [])
                sched = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{asub}") &
                                    Key("SK").begins_with("SCHED#")).get("Items", [])
                logged = sum(1 for s in sched if s.get("logStatus", "pending") != "pending")
                missed = sum(1 for s in sched if s.get("logStatus", "pending") == "pending"
                             and (s.get("scheduledDate") or "9999") < today_s)
                cd = u.get("UserCreateDate")
                out.append({"username": u["Username"], "sub": asub,
                    "name": prof.get("name") or attrs.get("preferred_username") or u["Username"],
                    "email": prof.get("email") or attrs.get("email", ""),
                    "createdAt": cd.isoformat() if hasattr(cd, "isoformat") else str(cd or ""),
                    "enabled": u.get("Enabled", True),
                    "coached": bool(prof.get("GSI1PK")),
                    "provider": prof.get("provider", ""),
                    "activePlans": [e.get("planTitle", "?") for e in enr if e.get("status") == "active"],
                    "logged": logged, "missed": missed,
                    # v58-last-seen
                    "selfSignup": bool(prof.get("selfSignup")),
                    "firstSeenAt": prof.get("firstSeenAt", ""),
                    "lastSeenAt": prof.get("lastSeenAt", ""),
                    "lastAppVersion": prof.get("lastAppVersion", ""),
                    "lastAppBuild": prof.get("lastAppBuild", ""),
                    "lastOSVersion": prof.get("lastOSVersion", ""),
                    "lastDeviceModel": prof.get("lastDeviceModel", ""),
                    # v59.4 groups + archive
                    "tags": [str(t) for t in (prof.get("tags") or [])],
                    "archived": bool(prof.get("archived")),
                    "photoUrl": _photo_url(prof.get("photoKey"))})   # v60.3
            out.sort(key=lambda x: x["createdAt"], reverse=True)
            return _resp(200, {"athletes": out})
        if rk == "POST /coach/subscriptions/grant":
            b = _body(event)
            out, err = create_subscription(b["athleteSub"], b["planId"], b["startDate"],
                                           b.get("chosenWeekdays"), coach_managed=True, coach_sub=sub)
            return _resp(400, {"error": err}) if err else _resp(200, out)
        if rk == "GET /coach/plans":
            r = TABLE.scan(FilterExpression="itemType = :t AND SK = :m",
                ExpressionAttributeValues={":t": "PLAN", ":m": "META"})
            plans = sorted(r.get("Items", []), key=lambda p: p.get("title",""))
            return _resp(200, {"plans": [{
                "planId": p["planId"], "title": p["title"],
                "category": p.get("category","run"), "durationWeeks": p.get("durationWeeks",0),
                "isFloatingSchedule": p.get("isFloatingSchedule",False),
                "isPublished": p.get("isPublished",False),
                "accessModel": p.get("accessModel","oneTime"),
                "priceMonthly": p.get("priceMonthly"),
                "author": p.get("author","")} for p in plans]})
        if rk == "POST /coach/plans/publish":
            b = _body(event)
            pid = b["planId"]; pub = bool(b.get("publish"))
            meta = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": "META"}).get("Item")
            if not meta: return _resp(404, {"error": "plan not found"})
            if pub:
                TABLE.update_item(Key={"PK": f"PLAN#{pid}", "SK": "META"},
                    UpdateExpression="SET isPublished = :t, GSI1PK = :m, GSI1SK = :s",
                    ExpressionAttributeValues={":t": True, ":m": "MARKETPLACE",
                                               ":s": f"PLAN#{meta.get('title','')}"})
            else:
                TABLE.update_item(Key={"PK": f"PLAN#{pid}", "SK": "META"},
                    UpdateExpression="SET isPublished = :f REMOVE GSI1PK, GSI1SK",
                    ExpressionAttributeValues={":f": False})
            return _resp(200, {"ok": True, "planId": pid, "isPublished": pub})
        if rk == "GET /coach/athletes":
            r = TABLE.query(IndexName="GSI1",
                            KeyConditionExpression=Key("GSI1PK").eq(f"COACH#{sub}"))
            return _resp(200, {"assignments": r.get("Items", [])})
        if rk == "POST /coach/assign":
            b = _body(event)
            start_str = b.get("startDate")
            if b.get("raceDate") and not start_str:
                start_str, rerr = _race_start(b["planId"], b["raceDate"], b.get("chosenWeekdays"))
                if rerr: return _resp(400, {"error": rerr})
            out, err = materialize(b["athleteSub"], b["planId"], start_str,
                                   b.get("chosenWeekdays"), True, coach_sub=sub)
            if not err and b.get("raceDate"):
                out["computedStart"] = start_str
                upd = "SET raceDate = :r"; vals = {":r": b["raceDate"]}
                if b.get("raceName"):
                    upd += ", raceName = :n"; vals[":n"] = b["raceName"]
                TABLE.update_item(Key={"PK": f"USER#{b['athleteSub']}", "SK": f"ENROLLMENT#{out['enrollmentId']}"},
                    UpdateExpression=upd, ExpressionAttributeValues=vals)
            if not err and b.get("notify"):
                if b.get("raceName") and b.get("raceDate"):
                    _notify(b["athleteSub"], "New training plan from Coach Greg",
                            f"{out.get('sessionsCreated',0)} sessions targeting {b['raceName']} on {b['raceDate']}")
                else:
                    _notify(b["athleteSub"], "New training plan from Coach Greg",
                            f"{out.get('sessionsCreated',0)} sessions starting {out.get('firstSession','')}")
            return _resp(404, {"error": err}) if err else _resp(200, out)
        if rk == "GET /coach/schedule":
            q = event.get("queryStringParameters") or {}
            a = q.get("athleteSub")
            if not a: return _resp(400, {"error": "athleteSub required"})
            items, count = _sched_q(a, q.get("from"), q.get("to"))
            return _resp(200, {"sessions": items, "count": count})
        if rk == "POST /coach/move":
            b = _body(event)
            nsk = _move(b["athleteSub"], b["sessionSK"], b["newDate"])
            return _resp(404, {"error": "not found"}) if not nsk else _resp(200, {"ok": True, "newSessionSK": nsk})
        if rk == "POST /coach/plans/addweek":  # v53-add-week
            b = _body(event)
            pid = b.get("planId")
            if not pid: return _resp(400, {"error": "planId required"})
            meta = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": "META"}).get("Item")
            if not meta: return _resp(404, {"error": "plan not found"})
            nw = int(meta.get("durationWeeks", 0)) + 1
            TABLE.update_item(Key={"PK": f"PLAN#{pid}", "SK": "META"},
                UpdateExpression="SET durationWeeks = :w",
                ExpressionAttributeValues={":w": nw})
            return _resp(200, {"ok": True, "durationWeeks": nw})
        if rk == "POST /coach/plans/session/update":  # v51-plan-edit
            b = _body(event)
            pid = b.get("planId"); psk = b.get("sessionSK")
            if not pid or not psk or not psk.startswith("SESSION"):
                return _resp(400, {"error": "planId + sessionSK required"})
            item = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": psk}).get("Item")
            if not item: return _resp(404, {"error": "plan session not found"})
            meta = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": "META"}).get("Item") or {}
            sets = []; vals = {}
            if "rawText" in b:
                new_raw = b["rawText"]
                st, bench = stype_bench(new_raw)
                sets += ["rawText = :r", "prescriptions = :p", "sessionType = :st"]
                vals[":r"] = new_raw
                vals[":p"] = parse_cell_multi(new_raw, meta.get("category"))
                vals[":st"] = st
            _bst = b.get("sessionType")
            if _bst in ("run", "swim", "bike", "row", "brick", "strength", "rest", "conditioning"):
                if "rawText" in b: vals[":st"] = _bst
                else: sets.append("sessionType = :st"); vals[":st"] = _bst
            _fin = vals.get(":st") or item.get("sessionType")
            _mv = {"swim": "Swim", "bike": "Bike", "row": "Row", "brick": "Brick", "strength": "Strength",
                   "conditioning": "Conditioning"}.get(_fin)
            if _mv and ":p" in vals:
                for _pp in (vals[":p"] or []):
                    if isinstance(_pp, dict) and _pp.get("movement") in (None, "Run"):
                        _pp["movement"] = _mv
            if b.get("title"):
                sets.append("title = :t"); vals[":t"] = b["title"]
            _nd = b.get("newDayOffset")  # v52-plan-move
            try: _nd = int(_nd) if _nd is not None else None
            except (TypeError, ValueError): _nd = None
            _cur = int(item.get("dayOffset", 0))
            _moving = _nd is not None and 0 <= _nd and _nd != _cur
            if not sets and not _moving:
                return _resp(400, {"error": "nothing to update"})
            sets.append("templateEdited = :te"); vals[":te"] = True
            if _moving:
                _new = dict(item)
                _new["dayOffset"] = _nd
                if ":r" in vals: _new["rawText"] = vals[":r"]
                if ":p" in vals: _new["prescriptions"] = vals[":p"]
                if ":st" in vals: _new["sessionType"] = vals[":st"]
                if ":t" in vals: _new["title"] = vals[":t"]
                _new["templateEdited"] = True
                _sfx = psk.split("#", 2)[2] if psk.count("#") >= 2 else psk
                _nsk = f"SESSION#{_nd:04d}#{_sfx}"
                _new["SK"] = _nsk
                TABLE.put_item(Item=_new)
                TABLE.delete_item(Key={"PK": f"PLAN#{pid}", "SK": psk})
                return _resp(200, {"ok": True, "newSessionSK": _nsk,
                    "sessionType": _new.get("sessionType")})
            TABLE.update_item(Key={"PK": f"PLAN#{pid}", "SK": psk},
                UpdateExpression="SET " + ", ".join(sets),
                ExpressionAttributeValues=vals)
            return _resp(200, {"ok": True, "sessionType": vals.get(":st") or item.get("sessionType"),
                "prescriptions": len(vals.get(":p") or []) if ":p" in vals else None})
        if rk == "GET /coach/plans/detail":  # v50-plan-detail
            q = event.get("queryStringParameters") or {}
            pid = q.get("planId")
            if not pid: return _resp(400, {"error": "planId required"})
            r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"PLAN#{pid}") &
                            Key("SK").begins_with("SESSION"))
            ss = sorted(r.get("Items", []), key=lambda s: int(s.get("dayOffset", 0)))
            meta = TABLE.get_item(Key={"PK": f"PLAN#{pid}", "SK": "META"}).get("Item") or {}
            return _resp(200, {"plan": meta, "sessions": ss, "count": len(ss)})
        if rk == "POST /coach/plans/upload":
            b = _body(event)
            out, err = write_plan(b["title"], b.get("category", "run"),
                                  b.get("author", "Greg's Endurance Training"),
                                  b.get("publish", False), b["csvText"])
            return _resp(400, {"error": err}) if err else _resp(200, out)
        if rk == "POST /coach/athletes/create":
            b = _body(event)
            username = b["username"].strip().lower()
            name = b.get("name", username)
            # v45-coach-email: coach-created athletes get a recoverable account
            _cc_email = (b.get("email") or "").strip()
            _cc_attrs = [{"Name": "preferred_username", "Value": name}] if name else []
            if _cc_email:
                _cc_attrs += [{"Name": "email", "Value": _cc_email},
                              {"Name": "email_verified", "Value": "true"}]
            try:
                u = IDP.admin_create_user(UserPoolId=POOL_ID, Username=username,
                    MessageAction="SUPPRESS",
                    UserAttributes=_cc_attrs)  # v45-coach-email
                asub = next(a["Value"] for a in u["User"]["Attributes"] if a["Name"] == "sub")
            except IDP.exceptions.UsernameExistsException:
                g = IDP.admin_get_user(UserPoolId=POOL_ID, Username=username)
                asub = next(a["Value"] for a in g["UserAttributes"] if a["Name"] == "sub")
                if _cc_email:  # v45-coach-email exists-path
                    try:
                        IDP.admin_update_user_attributes(UserPoolId=POOL_ID, Username=username,
                            UserAttributes=[{"Name": "email", "Value": _cc_email},
                                            {"Name": "email_verified", "Value": "true"}])
                    except Exception as e:
                        print(f"v45 exists-path email update failed: {e}")
            # void older unredeemed invites for this athlete - one live key per door
            old_inv = TABLE.scan(FilterExpression="itemType = :t AND username = :u AND #us = :f",
                ExpressionAttributeNames={"#us": "used"},
                ExpressionAttributeValues={":t": "INVITE", ":u": username, ":f": False})
            for oi in old_inv.get("Items", []):
                TABLE.delete_item(Key={"PK": oi["PK"], "SK": "META"})
            code = "".join(random.choices("ABCDEFGHJKMNPQRSTUVWXYZ23456789", k=8))
            TABLE.put_item(Item={"PK": f"INVITE#{code}", "SK": "META", "itemType": "INVITE",
                "username": username, "athleteSub": asub, "coachSub": sub, "used": False,
                "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()})
            TABLE.update_item(Key={"PK": f"USER#{asub}", "SK": "PROFILE"},
                UpdateExpression="SET itemType = :it, #n = if_not_exists(#n, :n), username = :u, GSI1PK = :g1, GSI1SK = :g2",
                ExpressionAttributeNames={"#n": "name"},
                ExpressionAttributeValues={":it": "PROFILE", ":n": name, ":u": username,
                    ":g1": f"COACH#{sub}", ":g2": f"ATHLETE#{asub}#PROFILE"})
            return _resp(200, {"athleteSub": asub, "username": username, "inviteCode": code})

        return _resp(404, {"error": f"no route: {rk}"})
    except KeyError as e:
        return _resp(400, {"error": f"missing field: {e}"})
    except Exception as e:
        print(f"ERROR: {e}")
        return _resp(500, {"error": "internal error"})
