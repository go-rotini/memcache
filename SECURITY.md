# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly by emailing **matthewcgetz@gmail.com**. Do not open a public issue.

You should receive a response within 72 hours. If accepted, a fix will be developed privately and released as a patch version.

## Resource Limits

This package defaults to safe behavior to mitigate denial-of-service attacks:

- **Bounded by construction**: `New()` requires either `WithMaxEntries` or `WithMaxBytes`. Unbounded caches require `NewUnbounded()` and are documented as such.
- **Maximum key size** can be capped via `WithMaxKeySize`.
- **Maximum value weight** can be capped via `WithMaxValueWeight`.
- **Maximum snapshot size** is capped at 256 MiB by default; configurable via `WithMaxSnapshotBytes`.
- **Loader rate limiting** via `WithLoaderRateLimit` and `WithLoaderTimeout` prevents thundering-herd amplification of failed upstream calls.
- **Hash function**: SipHash-2-4 with a per-cache random key resists HashDoS by default.

These limits can be configured at construction time but are set to safe defaults out of the box.
