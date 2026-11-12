"""GET FIT public catalog v5: week-1 preview only - full text never leaves for weeks 2+."""
import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

TABLE = boto3.resource("dynamodb").Table("GetFit")

def _clean(o):
    if isinstance(o, list): return [_clean(x) for x in o]
    if isinstance(o, dict): return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, Decimal): return int(o) if o % 1 == 0 else float(o)
    return o

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(_clean(body))}

def list_plans():
    r = TABLE.query(IndexName="GSI1",
                    KeyConditionExpression=Key("GSI1PK").eq("MARKETPLACE"))
    plans = [{"planId": it["planId"], "title": it["title"],
              "category": it.get("category", "run"),
              "durationWeeks": it.get("durationWeeks", 0),
              "durationLabel": it.get("durationLabel"),
              "tagline": it.get("tagline"),
              "blurb": it.get("blurb"),
              "isFloatingSchedule": it.get("isFloatingSchedule", False),
              "isPublished": True, "author": it.get("author", ""),
              "accessModel": it.get("accessModel", "oneTime"),
              "priceMonthly": it.get("priceMonthly"),
              "priceOneTime": it.get("priceOneTime"),
              "slotLabels": it.get("slotLabels"),
              "shelf": it.get("shelf")}
             for it in r.get("Items", [])]
    plans.sort(key=lambda p: p["title"])
    return _resp(200, {"plans": plans})

def get_plan(plan_id):
    r = TABLE.query(KeyConditionExpression=Key("PK").eq(f"PLAN#{plan_id}"))
    items = r.get("Items", [])
    meta = next((i for i in items if i["SK"] == "META"), None)
    if not meta or not meta.get("isPublished"):
        return _resp(404, {"error": "plan not found"})
    sessions = sorted((i for i in items if i["SK"].startswith("SESSION#")),
                      key=lambda i: i["SK"])
    out = []
    for s in sessions:
        wk = int(s.get("weekNumber", 0))
        if wk <= 1:
            out.append({k: v for k, v in s.items() if k not in ("PK", "SK")})
        else:
            out.append({"weekNumber": wk,
                        "dayOffset": s.get("dayOffset"),
                        "weekday": s.get("weekday"),
                        "sessionType": s.get("sessionType"),
                        "title": s.get("title"),
                        "coachTitle": s.get("coachTitle"),
                        "locked": True})
    return _resp(200, {"plan": meta, "sessions": out, "previewWeek": 1})

def lambda_handler(event, context):
    pid = ((event or {}).get("pathParameters") or {}).get("planId") or (event or {}).get("planId")
    try:
        return get_plan(pid) if pid else list_plans()
    except Exception as e:
        print(f"ERROR: {e}")
        return _resp(500, {"error": "internal error"})
