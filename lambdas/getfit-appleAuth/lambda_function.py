"""getfit-appleAuth: Sign in with Apple via Cognito custom auth + registration API."""
import json, time, urllib.request, os
import boto3, jwt
from jwt import PyJWKClient

POOL_ID = os.environ["POOL_ID"]
BUNDLE_ID = os.environ["BUNDLE_ID"]
IDP = boto3.client("cognito-idp")
TABLE = boto3.resource("dynamodb").Table("GetFit")
_jwks = PyJWKClient("https://appleid.apple.com/auth/keys")

def verify_apple(identity_token):
    key = _jwks.get_signing_key_from_jwt(identity_token)
    claims = jwt.decode(identity_token, key.key, algorithms=["RS256"],
                        audience=BUNDLE_ID, issuer="https://appleid.apple.com")
    return claims  # raises on any failure

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body)}

def lambda_handler(event, context):
    ts = event.get("triggerSource", "")
    # ---- Cognito custom-auth triggers ----
    if ts == "DefineAuthChallenge_Authentication":
        s = event["request"]["session"]
        if s and s[-1].get("challengeName") == "CUSTOM_CHALLENGE" and s[-1].get("challengeResult"):
            event["response"].update(issueTokens=True, failAuthentication=False)
        elif len(s) >= 2:
            event["response"].update(issueTokens=False, failAuthentication=True)
        else:
            event["response"].update(issueTokens=False, failAuthentication=False,
                                     challengeName="CUSTOM_CHALLENGE")
        return event
    if ts == "CreateAuthChallenge_Authentication":
        event["response"].update(publicChallengeParameters={"type": "APPLE_TOKEN"},
                                 privateChallengeParameters={"type": "APPLE_TOKEN"})
        return event
    if ts == "VerifyAuthChallengeResponse_Authentication":
        try:
            claims = verify_apple(event["request"]["challengeAnswer"])
            expected = "apple_" + claims["sub"].replace(".", "").replace("-", "")[:64]
            event["response"]["answerCorrect"] = (event["userName"] == expected)
        except Exception as e:
            print(f"apple verify failed: {e}")
            event["response"]["answerCorrect"] = False
        return event

    # ---- API: POST /auth/apple/register (public, idempotent find-or-create) ----
    if event.get("routeKey") == "POST /auth/apple/register":
        try:
            b = json.loads(event.get("body") or "{}")
            claims = verify_apple(b["identityToken"])
        except Exception as e:
            print(f"register verify failed: {e}")
            return _resp(401, {"error": "invalid Apple token"})
        username = "apple_" + claims["sub"].replace(".", "").replace("-", "")[:64]
        email = claims.get("email", "")
        first = (b.get("firstName") or "").strip()
        last = (b.get("lastName") or "").strip()
        display = (f"{first} {last}").strip() or "Athlete"
        created = False
        try:
            u = IDP.admin_create_user(UserPoolId=POOL_ID, Username=username,
                MessageAction="SUPPRESS",
                UserAttributes=([{"Name": "preferred_username", "Value": display}] +
                    ([{"Name": "email", "Value": email},
                      {"Name": "email_verified", "Value": "true"}] if email else [])))
            asub = next(a["Value"] for a in u["User"]["Attributes"] if a["Name"] == "sub")
            created = True
        except IDP.exceptions.UsernameExistsException:
            g = IDP.admin_get_user(UserPoolId=POOL_ID, Username=username)
            asub = next(a["Value"] for a in g["UserAttributes"] if a["Name"] == "sub")
        if created:
            item = {"PK": f"USER#{asub}", "SK": "PROFILE", "itemType": "PROFILE",
                    "name": display, "username": username, "selfSignup": True,
                    "provider": "apple"}
            if first: item["firstName"] = first
            if last: item["lastName"] = last
            if email: item["email"] = email
            TABLE.put_item(Item=item)
        return _resp(200, {"username": username, "created": created})

    return _resp(404, {"error": "unknown event"})
