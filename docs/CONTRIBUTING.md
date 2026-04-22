# Contributing to Floecat Docs

This documentation site is generated from the repository `docs/` directory.  
All changes go through normal pull requests; there is no direct in-site editing path to `main`.

## Quick Edit (single file)

1. Edit a Markdown file under `docs/` (for example `docs/operations.md`).
2. Run a local build check:

```bash
make test-site
```

3. Open or refresh local preview:

```bash
make site-preview
```

Preview URLs:

- `http://127.0.0.1:4000/floecat/`
- `http://127.0.0.1:4000/floecat/documentation/`

## Add a New Documentation Page

1. Create a new file in `docs/` (for example `docs/new-feature.md`).
2. Add it to `nav:` in [`mkdocs.yml`](https://github.com/eng-floe/floecat/blob/main/mkdocs.yml) so it appears in the left navigation.
3. Link it from at least one existing page (usually `docs/index.md` or a topic hub page).
4. Run `make test-site` and confirm the page renders in local preview.

## Reuse Content with Shared Snippets

Use shared snippets when text must stay in one source:

- Store shared fragments under `docs/_snippets/`.
- Include them from pages with:

`--8<-- "_snippets/your-snippet.md"`

Snippet files are excluded from nav/rendering as standalone pages by `mkdocs.yml`.

## Keep Links Healthy

- Prefer doc-to-doc relative links for docs pages (for example `operations.md`).
- For links to source code outside `docs/`, link to the GitHub repo URL.
- Before opening a PR, run:

```bash
make test-site
```

## PR Checklist

- Page appears in navigation (if it is a new top-level/section page).
- Headings are clear and follow a logical order (`#`, `##`, `###`).
- Code blocks include language fences (for example `bash`, `protobuf`, `json`).
- New links work in local preview.
- CI `Pages build check` passes.
