import json, os, time, uuid, urllib.request, urllib.parse
import boto3
from boto3.dynamodb.conditions import Key, Attr

TABLE = boto3.resource("dynamodb").Table("GetFit")
CID = os.environ["STRAVA_CLIENT_ID"]
SECRET = os.environ["STRAVA_CLIENT_SECRET"]
VERIFY = os.environ.get("STRAVA_VERIFY_TOKEN", "")
API_BASE = "https://tpvbxdx648.execute-api.us-east-1.amazonaws.com"
SPORT = {"Run": "run", "VirtualRun": "run", "TrailRun": "run",
         "Swim": "swim", "Ride": "bike", "VirtualRide": "bike", "GravelRide": "bike", "MountainBikeRide": "bike",
         "Rowing": "row", "VirtualRow": "row",
         "WeightTraining": "strength", "Workout": "conditioning",
         "Elliptical": "conditioning", "HighIntensityIntervalTraining": "conditioning", "Crossfit": "conditioning",
         "StairStepper": "conditioning"}

def _scan_all(**kw):
    """v5.1: full-table scans must paginate — a single page (1 MB) silently drops
    linked athletes whose rows land past page one (that is how Rusty got zero matches)."""
    items = []; resp = TABLE.scan(**kw); items += resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = TABLE.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw); items += resp.get("Items", [])
    return items

from decimal import Decimal
def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o % 1 == 0 else float(o)
    raise TypeError(f"not serializable: {type(o).__name__}")

def _resp(code, body, headers=None):
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    return {"statusCode": code, "headers": h, "body": json.dumps(body, default=_json_default)}

def _html(code, text):
    return {"statusCode": code, "headers": {"Content-Type": "text/html"},
            "body": f"<html><body style='font-family:-apple-system;text-align:center;padding-top:80px'><h2>{text}</h2><p>You can return to the GET FIT app.</p></body></html>"}

def _post(url, data):
    req = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode(), method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def _get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def _fresh_token(rec):
    if int(rec.get("expiresAt", 0)) > time.time() + 60:
        return rec["accessToken"]
    tok = _post("https://www.strava.com/oauth/token", {
        "client_id": CID, "client_secret": SECRET,
        "grant_type": "refresh_token", "refresh_token": rec["refreshToken"]})
    if "access_token" not in tok:
        print(f"refresh failed: {tok}"); return None
    TABLE.update_item(Key={"PK": rec["PK"], "SK": "STRAVA"},
        UpdateExpression="SET accessToken=:a, refreshToken=:r, expiresAt=:e",
        ExpressionAttributeValues={":a": tok["access_token"],
            ":r": tok["refresh_token"], ":e": int(tok["expires_at"])})
    return tok["access_token"]

# v5.2 (2026-11-07): same-day session wins even if already logged (no sliding to adjacent days).
# v5.1 (2026-11-07): paginated scans (root cause of Rusty's zero matches) + more sport mappings.
# v5 (2026-11-07): hourly reconcile + backfill + webhook logging + deauth (see below).
# v4 (2026-09-19): account selection + smarter matching.
#  - One Strava athlete may be linked to several GET FIT accounts (e.g. a coached
#    account and a stray Apple-ID self-signup). Prefer the account that actually
#    has a schedule around the activity date instead of scan order.
#  - 2+ same-type sessions on the day (athlete on several plans): drop "My Workouts"
#    if a plan session exists, then pick the first by SK. Never drop the activity.
#  - 0 sessions on the day: look at the day before/after (late uploads, timezones);
#    use it only if exactly one candidate turns up there.
#  - lastUnmatched now records a reason; matches record lastMatched.
def _cands(sub, date, stype, include_logged=False):
    q = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                    Key("SK").begins_with(f"SCHED#{date}"))
    out = [s for s in q.get("Items", []) if s.get("sessionType") == stype]
    unlogged = [s for s in out if s.get("logStatus") not in ("complete", "missed")]
    # v5.2: prefer unlogged, but a same-day session that is already logged is still
    # the right home for the activity (never slide to an adjacent day past it).
    return unlogged or (out if include_logged else [])

def _shift(date, days):
    import datetime as _d
    return (_d.date.fromisoformat(date) + _d.timedelta(days=days)).isoformat()

def _has_schedule_near(sub, date):
    for d in (date, _shift(date, -1), _shift(date, 1)):
        q = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                        Key("SK").begins_with(f"SCHED#{d}"), Limit=1)
        if q.get("Items"): return True
    return False

# v5 (2026-11-07): the webhook is no longer the only path.
#  - reconcile(): hourly EventBridge task pulls each connected athlete's recent
#    activities from Strava and runs the same matcher (webhook fan-out proved
#    unreliable per athlete: Rusty received zero events in two months while
#    Greg's arrived daily).
#  - backfill on connect (last 14 days) so a new link shows results immediately.
#  - every webhook hit is logged and stamps lastEventAt/lastEventType on the row.
#  - athlete deauthorize events delete the row so /strava/status stops lying.
#  - seenActivityIds (last 100) on the row prevents double-processing.
BACKFILL_DAYS = 14

def _seen(rec):
    return set(int(x) for x in (rec.get("seenActivityIds") or []))

def _mark_seen(rec, activity_id):
    ids = [int(x) for x in (rec.get("seenActivityIds") or [])]
    if int(activity_id) in ids: return
    ids = (ids + [int(activity_id)])[-100:]
    TABLE.update_item(Key={"PK": rec["PK"], "SK": "STRAVA"},
        UpdateExpression="SET seenActivityIds=:i, lastReconcileAt=:t",
        ExpressionAttributeValues={":i": ids, ":t": int(time.time())})
    rec["seenActivityIds"] = ids

def _handle_activity(owner_id, activity_id):
    recs = _scan_all(FilterExpression=Attr("itemType").eq("STRAVA") &
                     Attr("stravaAthleteId").eq(int(owner_id)))
    if not recs:
        print(f"no connected user for strava athlete {owner_id}"); return
    # need the activity date before we can pick an account; use the first rec's token
    token = _fresh_token(recs[0])
    if not token and len(recs) > 1: token = _fresh_token(recs[1])
    if not token: return
    act = _get(f"https://www.strava.com/api/v3/activities/{activity_id}", token)
    _match_activity(recs, token, act, source="webhook")

def _match_activity(recs, token, act, source="webhook"):
    activity_id = act.get("id")
    stype = SPORT.get(act.get("type") or act.get("sport_type", ""))
    if not stype:
        print(f"unmapped sport {act.get('type') or act.get('sport_type')} ({source}) for {recs[0]['PK']}")
        _mark_seen(recs[0], activity_id); return
    date = (act.get("start_date_local") or "")[:10]
    if not date: return
    # pick the account: the one with a schedule near this date, else scan order
    rec = recs[0]
    if len(recs) > 1:
        subs = [x["PK"].replace("USER#", "") for x in recs]
        with_sched = [x for x in recs if _has_schedule_near(x["PK"].replace("USER#", ""), date)]
        if with_sched: rec = with_sched[0]
        print(f"strava athlete {owner_id} linked to {len(recs)} accounts {subs}; using {rec['PK']}")
    sub = rec["PK"].replace("USER#", "")
    cands = _cands(sub, date, stype, include_logged=True); reason = ""
    if cands and all(c.get("logStatus") in ("complete", "missed") for c in cands):
        reason = "same day, already logged"
    if len(cands) == 0:
        near = _cands(sub, _shift(date, -1), stype) + _cands(sub, _shift(date, 1), stype)
        if len(near) == 1: cands = near; reason = "matched adjacent day"
        else: reason = f"no {stype} scheduled on {date} (adjacent days: {len(near)})"
    elif len(cands) > 1:
        plan = [c for c in cands if (c.get("planTitle") or "") != "My Workouts"]
        pool = plan or cands
        pool.sort(key=lambda c: c["SK"])
        reason = (reason + "; " if reason else "") + f"chose 1 of {len(cands)} same-day {stype} sessions"
        cands = [pool[0]]
    _mark_seen(rec, activity_id)
    if len(cands) != 1:
        print(f"unmatched ({source}): {reason} sub {sub}")
        TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"},
            UpdateExpression="SET lastUnmatched=:u",
            ExpressionAttributeValues={":u": {"activityId": int(activity_id),
                "date": date, "type": stype, "candidates": 0, "reason": reason,
                "name": (act.get("name") or "")[:80], "at": int(time.time())}})
        return
    laps = []
    try:
        laps = _get(f"https://www.strava.com/api/v3/activities/{activity_id}/laps", token)
    except Exception as e:
        print(f"laps fetch failed: {e}")
    sug = {"activityId": int(activity_id),
           "name": (act.get("name") or "")[:120],
           "distanceMeters": int(act.get("distance") or 0),
           "movingTimeSeconds": int(act.get("moving_time") or 0),
           "startLocal": act.get("start_date_local", ""),
           "laps": [{"order": i + 1,
                     "seconds": int(l.get("moving_time") or l.get("elapsed_time") or 0),
                     "distanceMeters": int(l.get("distance") or 0)}
                    for i, l in enumerate(laps[:40])],
           "fetchedAt": int(time.time())}
    TABLE.update_item(Key={"PK": cands[0]["PK"], "SK": cands[0]["SK"]},
        UpdateExpression="SET stravaSuggestion=:s, stravaActivityId=:a",
        ExpressionAttributeValues={":s": sug, ":a": int(activity_id)})
    TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"},
        UpdateExpression="SET lastMatched=:m",
        ExpressionAttributeValues={":m": {"activityId": int(activity_id), "date": date,
            "type": stype, "sk": cands[0]["SK"], "reason": reason or "exact",
            "at": int(time.time())}})
    print(f"suggestion attached ({source}): {cands[0]['SK']} <- activity {activity_id} ({len(sug['laps'])} laps) {reason}")

def _reconcile_rec(rec, days):
    """Pull recent activities for one linked athlete and match any not yet seen."""
    token = _fresh_token(rec)
    if not token:
        print(f"reconcile: no token for {rec['PK']}"); return 0
    after = int(time.time()) - days * 86400
    try:
        acts = _get(f"https://www.strava.com/api/v3/athlete/activities?after={after}&per_page=50", token)
    except Exception as e:
        print(f"reconcile: list failed for {rec['PK']}: {e}"); return 0
    if not isinstance(acts, list):
        print(f"reconcile: unexpected list response for {rec['PK']}: {str(acts)[:120]}"); return 0
    seen = _seen(rec); n = 0
    for a in sorted(acts, key=lambda x: x.get("start_date", "")):
        if int(a.get("id", 0)) in seen: continue
        try:
            _match_activity([rec], token, a, source="reconcile"); n += 1
        except Exception as e:
            print(f"reconcile: match failed {a.get('id')} for {rec['PK']}: {e}")
    TABLE.update_item(Key={"PK": rec["PK"], "SK": "STRAVA"},
        UpdateExpression="SET lastReconcileAt=:t", ExpressionAttributeValues={":t": int(time.time())})
    return n

def reconcile(days=2):
    recs = _scan_all(FilterExpression=Attr("itemType").eq("STRAVA"))
    total = 0
    for rec in recs:
        total += _reconcile_rec(rec, days)
    print(f"reconcile: {len(recs)} athlete(s), {total} new activit(ies) processed")
    return {"athletes": len(recs), "processed": total}

def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("task") == "reconcile":
        return _resp(200, reconcile(days=int(event.get("days", 2))))
    rk = event.get("routeKey", "")
    claims = (event.get("requestContext", {}).get("authorizer", {})
              .get("jwt", {}).get("claims", {}))
    sub = claims.get("sub")

    if rk == "GET /strava/webhook":
        q = event.get("queryStringParameters") or {}
        if q.get("hub.mode") == "subscribe" and q.get("hub.verify_token") == VERIFY:
            return _resp(200, {"hub.challenge": q.get("hub.challenge", "")})
        return _resp(403, {"error": "verify failed"})

    if rk == "POST /strava/webhook":
        try:
            ev = json.loads(event.get("body") or "{}")
        except ValueError:
            return _resp(200, {"ok": True})
        print(f"webhook event: owner={ev.get('owner_id')} {ev.get('object_type')}/{ev.get('aspect_type')} object={ev.get('object_id')} updates={ev.get('updates')}")
        try:
            for rec in _scan_all(FilterExpression=Attr("itemType").eq("STRAVA") &
                                 Attr("stravaAthleteId").eq(int(ev.get("owner_id") or 0))):
                TABLE.update_item(Key={"PK": rec["PK"], "SK": "STRAVA"},
                    UpdateExpression="SET lastEventAt=:t, lastEventType=:k",
                    ExpressionAttributeValues={":t": int(time.time()),
                        ":k": f"{ev.get('object_type')}.{ev.get('aspect_type')}"})
                if ev.get("object_type") == "athlete" and str((ev.get("updates") or {}).get("authorized")) == "false":
                    TABLE.delete_item(Key={"PK": rec["PK"], "SK": "STRAVA"})
                    print(f"webhook: athlete {ev.get('owner_id')} deauthorized -> unlinked {rec['PK']}")
        except Exception as e:
            print(f"webhook stamp error: {e}")
        if ev.get("object_type") == "activity" and ev.get("aspect_type") == "create":
            try:
                _handle_activity(ev.get("owner_id"), ev.get("object_id"))
            except Exception as e:
                print(f"webhook processing error: {e}")
        return _resp(200, {"ok": True})

    if rk == "GET /strava/connect":
        if not sub: return _resp(401, {"error": "auth required"})
        state = uuid.uuid4().hex
        TABLE.put_item(Item={"PK": f"STRAVASTATE#{state}", "SK": "STATE",
                             "itemType": "STRAVASTATE", "sub": sub,
                             "expires": int(time.time()) + 600})
        url = ("https://www.strava.com/oauth/authorize?client_id=" + CID +
               "&redirect_uri=" + urllib.parse.quote(API_BASE + "/strava/callback") +
               "&response_type=code&approval_prompt=auto"
               "&scope=activity:read_all&state=" + state)
        return _resp(200, {"authorizeUrl": url})

    if rk == "GET /strava/callback":
        q = event.get("queryStringParameters") or {}
        state, code = q.get("state"), q.get("code")
        if q.get("error"): return _html(200, "Strava connection was cancelled.")
        if not state or not code: return _html(400, "Missing parameters.")
        st = TABLE.get_item(Key={"PK": f"STRAVASTATE#{state}", "SK": "STATE"}).get("Item")
        if not st or int(st.get("expires", 0)) < time.time():
            return _html(400, "This connection link expired \u2014 try again from the app.")
        TABLE.delete_item(Key={"PK": f"STRAVASTATE#{state}", "SK": "STATE"})
        tok = _post("https://www.strava.com/oauth/token", {
            "client_id": CID, "client_secret": SECRET,
            "code": code, "grant_type": "authorization_code"})
        if "access_token" not in tok:
            print(f"strava token exchange failed: {tok}")
            return _html(502, "Strava connection failed \u2014 try again.")
        ath = tok.get("athlete") or {}
        TABLE.put_item(Item={"PK": f"USER#{st['sub']}", "SK": "STRAVA",
            "itemType": "STRAVA",
            "stravaAthleteId": int(ath.get("id", 0)),
            "accessToken": tok["access_token"],
            "refreshToken": tok["refresh_token"],
            "expiresAt": int(tok["expires_at"]),
            "connectedAt": int(time.time()),
            "athleteName": f"{ath.get('firstname','')} {ath.get('lastname','')}".strip()})
        try:   # v5: backfill so the link shows results right away
            rec = TABLE.get_item(Key={"PK": f"USER#{st['sub']}", "SK": "STRAVA"}).get("Item")
            n = _reconcile_rec(rec, BACKFILL_DAYS) if rec else 0
            print(f"connect backfill for {st['sub']}: {n} activit(ies)")
        except Exception as e:
            print(f"connect backfill failed: {e}")
        return _html(200, "Strava connected \u2713")

    if rk == "GET /strava/status":
        if not sub: return _resp(401, {"error": "auth required"})
        it = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"}).get("Item")
        if not it: return _resp(200, {"connected": False})
        return _resp(200, {"connected": True,
                           "athleteName": it.get("athleteName", ""),
                           "since": it.get("connectedAt"),
                           "lastEventAt": it.get("lastEventAt"), "lastReconcileAt": it.get("lastReconcileAt"),
                           "lastMatched": it.get("lastMatched"), "lastUnmatched": it.get("lastUnmatched")})

    if rk == "POST /strava/disconnect":
        if not sub: return _resp(401, {"error": "auth required"})
        TABLE.delete_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"})
        return _resp(200, {"ok": True})

    return _resp(404, {"error": f"no route {rk}"})
