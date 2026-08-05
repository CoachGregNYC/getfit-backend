#!/bin/bash
# Deploy one lambda from this repo, publishing a version.
#   ./scripts/deploy-lambda.sh getfit-athleteApi "what changed"
set -euo pipefail
FN="${1:?usage: deploy-lambda.sh <function-name> [description]}"
DESC="${2:-manual deploy}"
REGION="us-east-1"
SRC="$(cd "$(dirname "$0")/.." && pwd)/lambdas/$FN"
[ -d "$SRC" ] || { echo "no such function dir: $SRC"; exit 1; }
if [ -f "$SRC/VENDORED.md" ] && [ -f "$SRC/requirements.txt" ]; then
  echo "!! $FN has vendored deps. Run in $SRC first:"
  echo "     pip install -r requirements.txt -t ."
  read -rp "   Continue anyway? [y/N] " a; [ "$a" = "y" ] || exit 1
fi
TMP=$(mktemp -d); ( cd "$SRC" && zip -qr "$TMP/f.zip" . -x 'function-config.json' 'VENDORED.md' )
aws lambda update-function-code --function-name "$FN" --region "$REGION" \
  --zip-file "fileb://$TMP/f.zip" --query LastModified --output text
aws lambda wait function-updated --function-name "$FN" --region "$REGION"
V=$(aws lambda publish-version --function-name "$FN" --region "$REGION" \
    --description "$DESC" --query Version --output text)
echo "published version $V"
