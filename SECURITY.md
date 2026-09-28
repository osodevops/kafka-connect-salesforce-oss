# Security Policy

## Supported versions

Security fixes are provided for the current and the previous **minor** release
(`0.N` and `0.N-1`) and are backported to both. Older versions keep working but
receive no fixes. See [SUPPORT.md](SUPPORT.md) for the full support window.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

- Preferred: [GitHub private vulnerability reporting](https://github.com/osodevops/kafka-connect-salesforce-oss/security/advisories/new)
- Or email: security@oso.sh

You will receive an acknowledgement within two business days and a severity assessment
within five. Critical and high severity issues in a supported version are fixed, or a
mitigation published, within ten business days. We follow coordinated disclosure: we ask
for up to 90 days to ship a fix before details are published, and we credit reporters in
the advisory unless they prefer not to be named. Please include a description, reproduction
steps and an impact assessment.

## How fixes are published

- A GitHub Security Advisory on this repository, with a CVE where applicable.
- A patch release for every supported minor version, noted in the CHANGELOG, with the
  rebuilt plugin ZIPs attached to the GitHub release.
- Direct notification to support subscribers.

## Scope notes

- The connectors handle Salesforce credentials: they are declared as Connect `PASSWORD` configs
  (masked in the REST API and logs) and tokens are never logged. Reports of credential leakage
  paths are treated as high severity.
- Dependencies are monitored via Dependabot; bundled-dependency CVEs in the plugin ZIPs are
  addressed in patch releases.
