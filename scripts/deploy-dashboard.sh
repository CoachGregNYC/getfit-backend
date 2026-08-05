#!/bin/bash
# Upload dashboard/index.html, backing up the live copy first.
set -euo pipefail
REGION="us-east-1"; B="getfit-coach-446100759389"; DIST="E39SWJE2X79PW4"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BK="$ROOT/dashboard/.backup-$(date -u +%Y%m%dT%H%M%SZ).html"
aws s3 cp "s3://$B/index.html" "$BK" --region "$REGION" --quiet && echo "backup -> $BK"
aws s3 cp "$ROOT/dashboard/index.html" "s3://$B/index.html" --region "$REGION" \
  --content-type "text/html" --cache-control "no-cache, must-revalidate" --quiet
aws cloudfront create-invalidation --distribution-id "$DIST" \
  --paths "/index.html" "/" --query 'Invalidation.Id' --output text
echo "deployed"
