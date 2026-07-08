---
title: Releasing
description: Release automation with release-please and Maven Central publishing.
sidebar_position: 3
---

# Releasing

Releases are fully automated:

1. **Conventional commits** (`feat:`, `fix:`, `feat!:` …) on `main` drive
   [release-please](https://github.com/googleapis/release-please), which maintains a
   release PR with the version bump and `CHANGELOG.md`.
2. **Merging the release PR** creates the `vX.Y.Z` tag and GitHub Release.
3. The **release workflow** then:
   - validates that the checked-in POM version matches the tag (refusing `-SNAPSHOT`),
   - builds and tests everything,
   - deploys all published modules to **Maven Central** (Sonatype Central Portal,
     `sh.oso` namespace) — signed, with sources and javadoc,
   - attaches the four connector plugin ZIPs to the GitHub Release.

## Required repository secrets

| Secret | Purpose |
|---|---|
| `CENTRAL_USERNAME` / `CENTRAL_TOKEN` | Sonatype Central Portal user token |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armored GPG private key for artifact signing |
| `MAVEN_GPG_PASSPHRASE` | Its passphrase |

## Manual deploy

For a one-off local deploy with the same profile:

```bash
export CENTRAL_USERNAME=... CENTRAL_TOKEN=... MAVEN_GPG_PASSPHRASE=...
mvn clean deploy -s release/m2-settings.xml -Prelease -DskipTests
```

## Maven coordinates

```xml
<dependency>
  <groupId>sh.oso</groupId>
  <artifactId>sf-core</artifactId>
  <version><!-- latest release --></version>
</dependency>
```

The connector artifacts (`connect-salesforce-source`, `connect-salesforce-sink`,
`connect-salesforce-pe-sink`, `connect-salesforce-streaming-source`) are published the
same way; most users want the plugin ZIPs from GitHub Releases instead.
