import json, os, time, uuid, urllib.request, urllib.parse
import boto3
from boto3.dynamodb.conditions import Key, Attr

TABLE = boto3.resource("dynamodb").Table("GetFit")
CID = os.environ["STRAVA_CLIENT_ID"]
SECRET = os.environ["STRAVA_CLIENT_SECRET"]
VERIFY = os.environ.get("STRAVA_VERIFY_TOKEN", "")
API_BASE = "https://tpvbxdx648.execute-api.us-east-1.amazonaws.com"
SPORT = {"Run": "run", "VirtualRun": "run", "TrailRun": "run",
         "Swim": "swim", "Ride": "bike", "VirtualRide": "bike",
         "WeightTraining": "strength", "Workout": "conditioning"}

def _resp(code, body, headers=None):
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers)
    return {"statusCode": code, "headers": h, "body": json.dumps(body)}

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

def _handle_activity(owner_id, activity_id):
    r = TABLE.scan(FilterExpression=Attr("itemType").eq("STRAVA") &
                   Attr("stravaAthleteId").eq(int(owner_id)))
    recs = r.get("Items", [])
    if not recs:
        print(f"no connected user for strava athlete {owner_id}"); return
    rec = recs[0]; sub = rec["PK"].replace("USER#", "")
    token = _fresh_token(rec)
    if not token: return
    act = _get(f"https://www.strava.com/api/v3/activities/{activity_id}", token)
    stype = SPORT.get(act.get("type") or act.get("sport_type", ""))
    if not stype:
        print(f"unmapped sport {act.get('type')}"); return
    date = (act.get("start_date_local") or "")[:10]
    if not date: return
    q = TABLE.query(KeyConditionExpression=Key("PK").eq(f"USER#{sub}") &
                    Key("SK").begins_with(f"SCHED#{date}"))
    cands = [s for s in q.get("Items", [])
             if s.get("sessionType") == stype
             and s.get("logStatus") not in ("complete", "missed")]
    if len(cands) != 1:
        print(f"ambiguous match ({len(cands)}) for {stype} on {date} sub {sub}")
        TABLE.update_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"},
            UpdateExpression="SET lastUnmatched=:u",
            ExpressionAttributeValues={":u": {"activityId": int(activity_id),
                "date": date, "type": stype, "candidates": len(cands),
                "at": int(time.time())}})
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
    print(f"suggestion attached: {cands[0]['SK']} <- activity {activity_id} ({len(sug['laps'])} laps)")

def lambda_handler(event, context):
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
        return _html(200, "Strava connected \u2713")

    if rk == "GET /strava/status":
        if not sub: return _resp(401, {"error": "auth required"})
        it = TABLE.get_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"}).get("Item")
        if not it: return _resp(200, {"connected": False})
        return _resp(200, {"connected": True,
                           "athleteName": it.get("athleteName", ""),
                           "since": it.get("connectedAt")})

    if rk == "POST /strava/disconnect":
        if not sub: return _resp(401, {"error": "auth required"})
        TABLE.delete_item(Key={"PK": f"USER#{sub}", "SK": "STRAVA"})
        return _resp(200, {"ok": True})

    return _resp(404, {"error": f"no route {rk}"})
