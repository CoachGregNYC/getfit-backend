# GET FIT — backend

Backend for the GET FIT coaching platform (Pin Oak Partners LLC).
Snapshot of deployed AWS resources taken 2026-08-05T23:10:45Z.

## Layout

    lambdas/       source of each Lambda + deployed config snapshot
    dashboard/     coach web dashboard (single-file SPA)
    infra/         read-only snapshots of DynamoDB, API Gateway, Cognito, EventBridge, SNS
    scripts/       deploy helpers
    docs/          architecture notes

## AWS

| Resource | Value |
|---|---|
| Account | 446100759389 |
| Region | us-east-1 |
| DynamoDB | `GetFit` (PK/SK, GSI1) |
| API Gateway | `tpvbxdx648` |
| Cognito pool | `us-east-1_zYTdHaJfo` |
| Dashboard | `s3://getfit-coach-446100759389/index.html` |
| Photos | `s3://getfit-photos-446100759389` |

## Deploying

    ./scripts/deploy-lambda.sh getfit-athleteApi
    ./scripts/deploy-dashboard.sh

Both publish a version / keep a backup. See `docs/CONVENTIONS.md`.

## Version numbering

The changelog counts releases (athleteApi "v38", wod "v5", dashboard "v3.18").
AWS published Lambda versions started at 1 on 2026-08-05 and are unrelated.
Keep both in `docs/CHANGELOG.md` until they're reconciled.
