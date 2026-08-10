# Security Policy

## Reporting a vulnerability

**Do not open a public issue.** Please report security vulnerabilities
directly to:

📧 **github@orangehorsetech.com**

We will respond within 72 hours and work with you on a fix and disclosure
timeline.

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | ✅ Preview — security fixes provided |
| < 0.2   | ❌ Not released |

## Scope

Security issues may include but are not limited to:

- Panic / denial of service through malformed Modbus frames
- Integer overflow in PDU size calculations
- Unsound use of `unsafe` (should not exist — `#![forbid(unsafe_code)]`)
- Timing attacks on CRC/LRC validation

## Disclosure policy

1. Reporter submits via email
2. We acknowledge within 72 hours
3. We develop and test a fix
4. We publish a patch release + advisory
5. Reporter credited in the advisory (unless anonymity requested)
