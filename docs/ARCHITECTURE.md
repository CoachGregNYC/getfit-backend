# Architecture

    iOS app (SwiftUI/SwiftData)
      -> API Gateway tpvbxdx648 (JWT authorizer, Cognito)
        -> getfit-athleteApi   athlete + coach routes, logs, photos, plans
        -> getfit-wod          Concept2 WOD fetch/cache, subscribe, daily fan-out
        -> getfit-getPlans     plan catalog
        -> getfit-appleAuth    Sign in with Apple (Cognito CUSTOM_AUTH)
      -> DynamoDB GetFit  (single table, PK/SK + GSI1)
      -> S3 getfit-photos (workout photos, presigned access only)
      -> SNS -> APNs      (push)
      -> EventBridge      (daily WOD fan-out)

    Coach dashboard: single-file SPA in S3, hash-routed, CloudFront E39SWJE2X79PW4

## DynamoDB key patterns

    PK USER#<sub>     SK PROFILE | ENROLLMENT#<eid> | SCHED#<date>#<uuid>
                         BENCH#<movement>#<date> | SUB#<id> | RACE#<id>
    PK PLAN#<planId>  SK META | SESSION#<n>
    PK WOD#concept2   SK DATE#<iso>
    PK INVITE#<code>  SK META

## Known gaps

- `POST /photos/view` presigns any key with no ownership check (keys are UUIDs,
  so not practically exploitable, but an authenticated athlete could view
  another's photo given the key).
- APNS_PLATFORM_ARN on getfit-athleteApi points at APNS_SANDBOX while TestFlight
  builds use production APNs — verify delivery.
- Duplicate c2-wod enrollments cause the daily fan-out to over-report.
