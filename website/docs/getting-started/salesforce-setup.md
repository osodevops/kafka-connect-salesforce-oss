---
title: Salesforce Setup
description: Connected App, OAuth flows, and Change Data Capture configuration.
sidebar_position: 2
---

# Salesforce Setup

## 1. Create a Connected App

In **Setup → App Manager → New Connected App**:

1. Enable **OAuth Settings**.
2. Add the scopes **Manage user data via APIs (api)** and, for real-time streaming,
   **Perform requests at any time (refresh_token, offline_access)**.
3. Note the **Consumer Key** and **Consumer Secret**.

## 2. Choose an OAuth flow

### Client Credentials (recommended for server-to-server)

1. On the Connected App, enable **Client Credentials Flow**.
2. Assign a **Run As** user — use a dedicated, API-Only integration user.
3. Configure the connector with:

```properties
sf.auth.grant.type=client_credentials
sf.instance.url=https://YOUR_DOMAIN.my.salesforce.com
sf.consumer.key=...
sf.consumer.secret=...
```

:::warning My Domain required
`login.salesforce.com` and `test.salesforce.com` are **not valid** for the Client
Credentials flow. `sf.instance.url` must be the org's My Domain URL. The connector
rejects the login hosts at config-validation time.
:::

### JWT Bearer (recommended for multiple users / certificate-based auth)

1. Generate an RSA key pair and upload the X.509 certificate to the Connected App
   (**Use digital signatures**).
2. Pre-authorize the integration user's profile or permission set.
3. Configure:

```properties
sf.auth.grant.type=jwt_bearer
sf.instance.url=https://YOUR_DOMAIN.my.salesforce.com
sf.consumer.key=...
sf.username=integration@yourcompany.com
sf.jwt.keystore.path=/secrets/sf-private-key.pem
```

`sf.jwt.keystore.path` accepts a PEM (PKCS#8) private key, a JKS, or a PKCS12 keystore
(`sf.jwt.keystore.password` for the latter two). No refresh token is issued — the
connector re-mints the JWT automatically on expiry.

### Username-Password (legacy — avoid)

Supported for old orgs only (`sf.auth.grant.type=password` plus `sf.username`,
`sf.password`, `sf.password.token`). Salesforce blocks this flow by default in orgs
created Summer '23 or later; migrate to one of the flows above.

## 3. Enable Change Data Capture (source connector)

In **Setup → Change Data Capture**, add each object you plan to stream (e.g. Account,
Contact, Opportunity). Each selected entity publishes to its own channel
(`/data/AccountChangeEvent`, ...), which the source connector subscribes to per SObject.

:::tip
CDC events are retained for **72 hours**. Restarts within that window resume exactly
where they left off; longer outages trigger the connector's
[gap recovery](../concepts/offsets-and-recovery.md).
:::

## 4. Platform Events (Platform Event sink)

The Platform Event sink publishes to an **existing** platform event definition. Create it
under **Setup → Platform Events** — only Checkbox, Date, Date/Time, Number, Text, and
Text Area field types exist on platform events.
