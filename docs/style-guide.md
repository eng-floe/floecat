# Documentation Style Guide

Use this guide for all files under `docs/` so pages stay consistent as the project grows.

## Page Structure

- Use exactly one `#` title at the top of each page.
- Follow with short sections using `##` and `###`.
- Keep section titles descriptive (`Configuration`, `Troubleshooting`, `Examples`).
- Prefer shorter sections over very long blocks.

## Writing Style

- Use direct, technical language.
- Prefer active voice: "Run `make test-site`" instead of "The command should be run."
- Define terms once, then reuse them consistently.
- Avoid marketing language in reference and operations pages.

## Commands and Code Blocks

- Always fence code blocks and include language hints:
  - `bash` for commands
  - `json`, `yaml`, `properties`, `protobuf` for config/examples
- Keep command examples copy/paste friendly.
- Use realistic paths and values where possible.

Example:

```bash
make test-site
make site-preview
```

## Links

- For links to other docs pages, use relative doc links (for example `operations.md`).
- For links to repository source files outside `docs/`, use GitHub `blob/tree` URLs.
- Avoid raw URLs as visible text; use descriptive labels.

## Snippets and Reuse

- Put shared content in `docs/_snippets/` when multiple pages need the same block.
- Include snippets with:

`--8<-- "_snippets/<name>.md"`

- Keep snippet names specific and stable (`quick-links.md`, `docs-by-area.md`).

## Lists and Tables

- Use lists for procedures and checks.
- Use tables for field/flag references.
- Keep table columns small and scannable.

## Maintenance Rules

- When adding a new page, add it to `nav:` in `mkdocs.yml` at the repository root.
- Run local validation before opening a PR:

```bash
make test-site
```

- If linting is configured, ensure markdown lint passes as well.
