# Contributing to Floecat

Thank you for contributing to Floecat.

This document defines the project workflow and contributor expectations so contributions are consistent, reviewable, and aligned with community standards.

## Community Standards

All participation must follow the project Code of Conduct:

- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- [`SECURITY.md`](SECURITY.md) for private vulnerability reporting

This applies to issues, pull requests, discussions, and review interactions.

## Contribution Workflow

Floecat uses branch protections and CI checks on `main`.

1. Fork or branch
   - Internal contributors: create a feature branch from `main`.
   - External contributors: fork the repository and open a PR from your fork.
2. Develop
   - Keep commits focused and reviewable.
   - Add or update tests with code changes.
   - Run formatting and verification locally before pushing.
3. Open a pull request
   - Target `main`.
   - Ensure CI is green.
   - Address review feedback promptly and keep discussion technical.

## Local Quality Gates

Required tools: Java 25+, Maven, Make.

Run these checks before opening or updating a PR:

```bash
make fmt
make verify
```

Notes:

- `make fmt` applies Google Java Format.
- `make verify` runs build plus unit/integration checks.
- CI enforces formatting and verification checks.

## Commit and PR Guidelines

- Use conventional commits when possible (`feat:`, `fix:`, `docs:`, etc.).
- Keep PRs scoped; avoid mixing unrelated refactors.
- Include context in the PR description:
  - What changed
  - Why it changed
  - How it was tested
- Do not commit credentials, tokens, or secrets.

## Review and Merge Policy

- PRs require green checks.
- PRs require at least one approval.
- Merge strategy is squash merge.

## Licensing and Contribution Terms

This project is licensed under the Apache License, Version 2.0.

By submitting a pull request, you represent that you have the right to license your contribution to Yellowbrick Data, Inc. and the Apache Software Foundation, and you agree that your contribution will be licensed under the Apache License, Version 2.0.
