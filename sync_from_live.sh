#!/bin/bash
# sync_from_live.sh — pull DEPLOYED bytes into the repo. Never trusts local copies.
set -u
REGION="us-east-1"
cd "$(dirname "$0")" || exit 1
mkdir -p lambdas/athleteApi lambdas/wod dashboard

fail=0
for FN in getfit-athleteApi getfit-wod getfit-strava; do
  DIR="lambdas/${FN#getfit-}"
  URL=$(aws lambda get-function --function-name "$FN" --region "$REGION" \
        --query 'Code.Location' --output text) || { echo "ABORT: $FN fetch failed"; exit 1; }
  curl -s -o /tmp/$FN.zip "$URL"
  rm -rf "$DIR"/*; unzip -oq /tmp/$FN.zip -d "$DIR"
  python3 -m py_compile "$DIR/lambda_function.py" 2>/dev/null \
    && echo "[ok] $FN -> $DIR ($(wc -l < "$DIR/lambda_function.py") lines, compiles)" \
    || { echo "[!!] $FN pulled but does not compile"; fail=1; }
done

aws s3 cp s3://getfit-coach-446100759389/index.html dashboard/index.html \
  --region "$REGION" --quiet || { echo "ABORT: dashboard pull failed"; exit 1; }
grep -qF 'id="etype"' dashboard/index.html \
  && echo "[ok] dashboard -> dashboard/index.html (v3.19 marker present)" \
  || { echo "[!!] dashboard pulled but v3.19 marker missing"; fail=1; }

# markers in deployed lambda source — proves we captured the versions we think
grep -qF 'v43-signup-email' lambdas/athleteApi/lambda_function.py \
  && echo "[ok] athleteApi carries v43 marker" || { echo "[!!] v43 marker missing"; fail=1; }

echo
[ "$fail" = "0" ] && echo "SYNC CLEAN — ready to commit" || echo "SYNC COMPLETED WITH WARNINGS — read above before committing"
