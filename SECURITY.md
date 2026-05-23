# Security Policy

## Supported Versions

Mycelium is pre-1.0 and unreleased. Only the `main` branch is supported; old commits receive no fixes.

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security-impacting bugs.

Email reports to **benoitc@enki-multimedia.eu** with:

- A description of the issue and the impact you believe it has.
- Reproduction steps or a minimal proof-of-concept.
- The commit hash you tested against.
- Whether you would like to be credited in the fix commit / advisory.

Acknowledgement of a report happens within 5 business days. Triage and a fix or mitigation plan typically follow within 30 days for confirmed reports; complex issues may take longer and will be communicated.

If a fix lands publicly before disclosure is coordinated, the commit message will not point to the vulnerability until an advisory is published.

## Scope

In scope:
- The Ed25519 distribution authentication (`mycelium_dist_auth*`, `mycelium_dist_keys`).
- The QUIC dist carrier integration (`mycelium_dist_auth_callback`, `mycelium_discovery`).
- Multi-hop circuit framing (`mycelium_circuit*`, `mycelium_streams`).
- HyParView / Plumtree / OR-Map registry behaviour under adversarial peers.

Out of scope:
- Vulnerabilities in upstream `erlang_quic`, `hlc`, or other dependencies. Report those to their respective projects.
- DoS via legitimate but resource-intensive workloads.
- Issues requiring local code execution as the BEAM user.

## Known Limitations

These are documented design properties, not vulnerabilities:

- TOFU mode (`auth_trust_mode = tofu`) trusts the first key seen for a node. Use `strict` mode if you need to pre-pin keys.
- Mycelium has no built-in NAT traversal; bypass is left to an external relay/tunnel adapter (see `docs/external-relay.md`).
- The `cookie_only_nodes` whitelist and `auth_enabled = false` disable the Ed25519 handshake (and its TLS channel binding) for matching peers. These are reduced-assurance modes: the connection is then gated by the dist cookie over an unauthenticated TLS channel, with no protection against an active MITM. Use `cookie_only_nodes` only for c-nodes that genuinely cannot speak the auth protocol, and never with the default cookie (boot refuses that combination).
- The QUIC TLS certificate is self-signed (ECDSA P-256). Peer authentication and the anti-relay channel binding come from the Ed25519 layer, not from validating the TLS certificate.
