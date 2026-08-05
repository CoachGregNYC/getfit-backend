# Changelog

Reconstructed at first commit (2026-08-05T23:10:45Z); earlier entries are from notes,
not from git history.

## getfit-athleteApi
- v38 / AWS version 1 — `/coach/feed` returns `logPhotoKeys` + presigned `photoUrls`
- v37 — `POST /me/sessions` accepts log fields, returns `sk` (log-on-add)
- v34 and earlier — see notes

## getfit-wod
- v5 — erg interval parsing (ladders/pyramids → one prescription per piece);
       HTML entities in titles; uuid5 session keys
- **Not yet published as an AWS version.**

## dashboard
- v3.18 — photo thumbnails in Results Feed and Results view
- v3.17 — current at snapshot
- v3.16 — mobile feed mode (#/feed)
- v3.15 — mm:ss benchmarks, coach-added races
- v3.14 — athlete pages (hash routing)

## Data migrations
- 2026-08-05 — backfilled prescriptions on 4 pre-parser WOD days;
  cleaned `&raquo;` from the 08-04 cache item and 2 athlete sessions (11 items)
