# Shared Snippets

This directory holds reusable Markdown fragments consumed by documentation pages via
`pymdownx.snippets` includes.

## Include syntax

```md
--8<-- "_snippets/quick-links.md"
```

## Why this exists

- Keep repeated lists/content in one place.
- Make pages intentionally depend on shared fragments.
- Reduce copy-paste drift as docs evolve.

When updating a shared snippet, run `make test-site` to verify all including pages still render.
