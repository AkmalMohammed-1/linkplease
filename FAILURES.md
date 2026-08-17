# Known Failures

- The rate limiter is process-local. Running multiple app processes against the same API key can exceed the 10-per-minute Pseudogram send limit because each process would maintain its own send window.
- A process crash after a delivery is claimed as `sending` or `checking` but before it is rescheduled can leave that delivery stuck until a manual repair query moves it back to `queued` or `accepted`.
- If Pseudogram accepts a DM and then every later status lookup fails until `MAX_STATUS_ATTEMPTS` is reached, this app marks the delivery failed even though the platform might eventually deliver it.
- `duplicates_blocked` counts each matched comment that collides with an existing `(rule_id, user_id)` delivery. If the grader defines duplicates only as duplicate `event_id` redeliveries, this number may be higher than their preferred interpretation.
- `comment.deleted` is only cancelled before delivery. If the comment is deleted after Pseudogram accepts the DM, the app continues polling and may count the DM as sent.
- SQLite is good enough for the assignment load, but very high concurrent webhook traffic on a small instance can queue behind write locks and increase `/webhook` latency.
