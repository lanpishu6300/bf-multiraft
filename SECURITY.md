# Security Policy

**中文：** [SECURITY.zh-CN.md](SECURITY.zh-CN.md)

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` (0.1.x) | Yes |
| Older tags | Best effort |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

1. Prefer GitHub **Security Advisories** on [lanpishu6300/multiraft](https://github.com/lanpishu6300/multiraft/security/advisories/new) if available
2. Or private contact: **lanpishu6300@gmail.com** with subject `[SECURITY] multiraft`

Include:

- Affected crate / component
- Reproduction steps or PoC (private)
- Impact assessment (auth bypass, DoS, data leak, etc.)

We aim to acknowledge within **72 hours** and provide a remediation plan or fix timeline.

## Scope notes

- Demo admin HTTP and Raft gRPC are intended for lab / local clusters — treat exposure to untrusted networks as in scope if enabled by default in scripts.
- Dependency CVEs: prefer PRs bumping versions with a short risk note (respect the openraft exact pin unless the bump is intentional).
