---
title: Authentication
description: OAuth flows, session lifecycle, and secret handling.
sidebar_position: 1
---

# Authentication

All connectors share the same `sf.auth.*` configuration, implemented once in `sf-core`.

## Flows

| Flow | `sf.auth.grant.type` | When to use |
|---|---|---|
| Client Credentials | `client_credentials` | Server-to-server with a single integration user (recommended) |
| JWT Bearer | `jwt_bearer` | Certificate-based auth, multiple usernames, no shared secret |
| Username-Password | `password` | Legacy orgs only — blocked by default in orgs created Summer '23+ |

Full setup walkthrough: [Salesforce Setup](../getting-started/salesforce-setup.md).

### Client Credentials details

`POST {sf.instance.url}/services/oauth2/token` with `grant_type=client_credentials`.
The token endpoint **must** be the org's My Domain host — the connectors reject
`login.salesforce.com`/`test.salesforce.com` at validation time. The Connected App needs
a "Run As" API-Only integration user.

### JWT Bearer details

The connector builds an RS256-signed JWT (`iss` = consumer key, `sub` = `sf.username`,
`aud` = `sf.jwt.audience`, 3-minute expiry, unique `jti`) and exchanges it with
`grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`. Key material
(`sf.jwt.keystore.path`) may be:

- a PEM file containing a PKCS#8 private key (`-----BEGIN PRIVATE KEY-----`),
- a JKS keystore, or
- a PKCS12 keystore (`.p12`/`.pfx`)

with `sf.jwt.keystore.password` for the keystore formats. Use
`sf.jwt.audience=https://test.salesforce.com` for sandbox orgs.

## Session lifecycle

- The access token and `instance_url` from the token response are cached per task.
- On any `401` / `INVALID_SESSION_ID`, the session is invalidated and **one** transparent
  re-authentication is attempted before the request is retried; concurrent callers share
  the refreshed session.
- The Pub/Sub gRPC channel attaches the session as call metadata (`accesstoken`,
  `instanceurl`, `tenantid`) on every RPC, so streams pick up refreshed tokens on
  reconnect.

## Secret handling

- Credentials are declared as Connect `PASSWORD`-type configs, so they are masked in the
  REST API and logs.
- Tokens are never logged; error messages never echo request bodies containing
  credentials.
- Use a Connect **config provider** to keep secrets out of connector JSON:

```properties
# worker
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

```json
"sf.consumer.secret": "${file:/secrets/sf.properties:consumer.secret}"
```
