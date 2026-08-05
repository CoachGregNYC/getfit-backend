# Conventions

- **Recon before patching.** Read deployed bytes; never assume local == deployed.
- **Anchor-asserted patches.** Match an exact anchor once, or abort.
- **Dry run first.** Mutating scripts default to dry run; `APPLY=1` to write.
- **Verify against reality.** Confirm in the simulator or the database, not in a log line.
- **Publish a version on every Lambda deploy** — it is the only rollback point.
- **Back up the dashboard before overwriting** — S3 has no versioning on this bucket.

## Data lessons

- Never delete a session that has a log (Build 7 data loss).
- Window-scope orphan cleanup; push before delete.
- `ConditionExpression` on every conditional write.
- DynamoDB returns `Decimal`; compare canonically or diffs lie.
- HTML-unescape WOD text at fetch *and* at deliver — cached items bypass the fetch fix.
