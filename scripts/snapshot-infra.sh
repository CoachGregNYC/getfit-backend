#!/bin/bash
# Refresh infra/ snapshots from live AWS. Read-only.
set -euo pipefail
REGION="us-east-1"
ROOT="$(cd "$(dirname "$0")/.." && pwd)/infra"
aws dynamodb describe-table --table-name GetFit --region "$REGION" \
  --query 'Table.{Name:TableName,Keys:KeySchema,Attrs:AttributeDefinitions,GSI:GlobalSecondaryIndexes[].{Name:IndexName,Keys:KeySchema,Projection:Projection}}' \
  --output json > "$ROOT/dynamodb-GetFit.json"
aws apigatewayv2 get-routes --api-id tpvbxdx648 --region "$REGION" \
  --query 'sort_by(Items,&RouteKey)[].{Route:RouteKey,Target:Target,Auth:AuthorizationType}' \
  --output json > "$ROOT/apigateway-routes.json"
echo "infra snapshots refreshed — review with: git diff infra/"
