# Security Policy

## Supported versions

The latest minor release receives security fixes.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

- Preferred: [GitHub private vulnerability reporting](https://github.com/osodevops/kafka-connect-salesforce-oss/security/advisories/new)
- Or email: security@oso.sh

We aim to acknowledge reports within 3 business days. Please include a description, reproduction
steps, and impact assessment. We'll coordinate a fix and disclosure timeline with you.

## Scope notes

- The connectors handle Salesforce credentials: they are declared as Connect `PASSWORD` configs
  (masked in the REST API and logs) and tokens are never logged. Reports of credential leakage
  paths are treated as high severity.
- Dependencies are monitored via Dependabot; bundled-dependency CVEs in the plugin ZIPs are
  addressed in patch releases.
