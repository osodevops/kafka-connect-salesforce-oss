# Contributing

Thanks for your interest in improving kafka-connect-salesforce!

## Getting started

- **Build**: JDK 17+ and Maven 3.6.3+, then `mvn clean verify` (add `-DskipE2E` if you don't have Docker).
- **Local dev loop**: everything runs against a local fake Salesforce — no org needed. See the
  [local testing guide](https://salesforcekafkaconnector.com/development/local-testing).
- **Docs site**: `cd website && npm install && npm start`.

## Pull requests

1. Fork, branch from `main`, keep changes focused.
2. Add or update tests — behaviour changes need coverage against the fake harness.
3. Use [conventional commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `chore:`) —
   release-please derives versions and the changelog from them. Breaking changes use `feat!:` or a
   `BREAKING CHANGE:` footer.
4. `mvn clean verify` must pass; CI runs the same plus the docker e2e tests.
5. Update the relevant docs page under `website/docs/` when config or behaviour changes.

## Clean-room policy

This project is a clean-room implementation built from **Salesforce's public API documentation only**.
Do not copy code, configuration strings, or documentation text from Confluent's proprietary connectors
(or any other proprietary implementation). PRs that appear derived from proprietary sources will be closed.

## Reporting issues

Use the issue templates. For security vulnerabilities, **do not open a public issue** — see
[SECURITY.md](SECURITY.md).
