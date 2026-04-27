#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';

function fail(message) {
  console.error(`ERROR: ${message}`);
  process.exit(1);
}

function parseArgs(argv) {
  const args = {
    dryRun: false,
    siteDir: 'site-src',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case '--dry-run':
        args.dryRun = true;
        break;
      case '--site-dir':
        args.siteDir = argv[i + 1] ?? '';
        i += 1;
        break;
      case '-h':
      case '--help':
        console.log('Usage: sync-blog-discussions.mjs [--site-dir <dir>] [--dry-run]');
        process.exit(0);
        break;
      default:
        fail(`unknown arg: ${arg}`);
    }
  }
  return args;
}

function parseSimpleYaml(text) {
  const root = {};
  const stack = [{ indent: -1, value: root }];
  for (const rawLine of text.split(/\r?\n/)) {
    const withoutComment = rawLine.replace(/\s+#.*$/, '');
    if (!withoutComment.trim()) {
      continue;
    }
    const indent = rawLine.match(/^ */)?.[0].length ?? 0;
    const match = withoutComment.match(/^\s*([A-Za-z0-9_]+):(?:\s*(.*))?$/);
    if (!match) {
      continue;
    }
    const [, key, rawValue = ''] = match;
    while (stack.length > 1 && indent <= stack[stack.length - 1].indent) {
      stack.pop();
    }
    const parent = stack[stack.length - 1].value;
    const value = rawValue.trim();
    if (!value) {
      parent[key] = {};
      stack.push({ indent, value: parent[key] });
      continue;
    }
    parent[key] = parseScalar(value);
  }
  return root;
}

function parseScalar(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  if (value === 'true') {
    return true;
  }
  if (value === 'false') {
    return false;
  }
  return value;
}

function parseFrontMatter(fileText) {
  const match = fileText.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?/);
  if (!match) {
    return {};
  }
  return parseSimpleYaml(match[1]);
}

function listBlogPosts(siteDir, defaults) {
  const postsDir = path.join(siteDir, '_posts');
  const posts = [];
  for (const entry of fs.readdirSync(postsDir, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.endsWith('.md')) {
      continue;
    }
    const match = entry.name.match(/^(\d{4})-(\d{2})-(\d{2})-(.+)\.md$/);
    if (!match) {
      continue;
    }
    const [, year, month, day, slug] = match;
    const filePath = path.join(postsDir, entry.name);
    const frontMatter = parseFrontMatter(fs.readFileSync(filePath, 'utf8'));
    const published = frontMatter.published ?? true;
    const comments = frontMatter.comments ?? defaults.comments ?? false;
    if (!published || !comments) {
      continue;
    }
    const pathname = `/${year}/${month}/${day}/${slug}.html`;
    const title = typeof frontMatter.title === 'string' ? frontMatter.title : pathname;
    const date = typeof frontMatter.date === 'string' ? frontMatter.date : `${year}-${month}-${day}`;
    posts.push({
      date,
      filePath,
      pathname,
      slug,
      title,
      discussionTitle: `Comments: ${pathname}`,
      canonicalUrl: `https://eng-floe.github.io/floecat${pathname}`,
    });
  }
  posts.sort((a, b) => a.pathname.localeCompare(b.pathname));
  return posts;
}

async function githubGraphql(query, variables) {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    fail('missing required env var: GITHUB_TOKEN');
  }
  const response = await fetch('https://api.github.com/graphql', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'User-Agent': 'floecat-site-sync',
    },
    body: JSON.stringify({ query, variables }),
  });
  const payload = await response.json();
  if (!response.ok) {
    fail(`GitHub GraphQL request failed: ${response.status} ${JSON.stringify(payload)}`);
  }
  if (payload.errors?.length) {
    fail(`GitHub GraphQL errors: ${JSON.stringify(payload.errors)}`);
  }
  return payload.data;
}

async function fetchRepositoryAndDiscussions(owner, repo, categoryId) {
  const query = `
    query($owner: String!, $repo: String!, $categoryId: ID!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        id
        discussions(first: 100, after: $cursor, categoryId: $categoryId, orderBy: {field: CREATED_AT, direction: DESC}) {
          nodes {
            id
            title
            body
            url
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
  `;
  let repositoryId = null;
  const discussions = [];
  let cursor = null;
  while (true) {
    const data = await githubGraphql(query, { owner, repo, categoryId, cursor });
    const repository = data.repository;
    if (!repository) {
      fail(`repository not found: ${owner}/${repo}`);
    }
    repositoryId = repository.id;
    discussions.push(...repository.discussions.nodes);
    if (!repository.discussions.pageInfo.hasNextPage) {
      break;
    }
    cursor = repository.discussions.pageInfo.endCursor;
  }
  return { repositoryId, discussions };
}

function discussionExists(post, discussions) {
  return discussions.some((discussion) => {
    const title = discussion.title ?? '';
    const body = discussion.body ?? '';
    return title.includes(post.pathname) || body.includes(`floecat-blog-path: ${post.pathname}`);
  });
}

async function createDiscussion(repositoryId, categoryId, post) {
  const mutation = `
    mutation($input: CreateDiscussionInput!) {
      createDiscussion(input: $input) {
        discussion {
          id
          url
          title
        }
      }
    }
  `;
  const body = [
    `Discussion thread for [${post.title}](${post.canonicalUrl}).`,
    '',
    `Published: ${post.date}`,
    `Pathname: \`${post.pathname}\``,
    '',
    `<!-- floecat-blog-path: ${post.pathname} -->`,
  ].join('\n');
  const data = await githubGraphql(mutation, {
    input: {
      repositoryId,
      categoryId,
      title: post.discussionTitle,
      body,
    },
  });
  return data.createDiscussion.discussion;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const siteDir = path.resolve(args.siteDir);
  const siteConfigPath = path.join(siteDir, '_config.yml');
  if (!fs.existsSync(siteConfigPath)) {
    fail(`site config not found: ${siteConfigPath}`);
  }
  const config = parseSimpleYaml(fs.readFileSync(siteConfigPath, 'utf8'));
  const categoryId = config.comments?.giscus?.category_id;
  const mapping = config.comments?.giscus?.discussion_term;
  if (!categoryId) {
    fail(`missing comments.giscus.category_id in ${siteConfigPath}`);
  }
  if (mapping !== 'pathname') {
    fail(`expected comments.giscus.discussion_term=pathname, found: ${String(mapping)}`);
  }

  const defaults = { comments: true };
  const posts = listBlogPosts(siteDir, defaults);
  if (args.dryRun) {
    for (const post of posts) {
      console.log(`${post.discussionTitle} -> ${post.canonicalUrl}`);
    }
    return;
  }

  const repoEnv = process.env.REPO;
  if (!repoEnv || !repoEnv.includes('/')) {
    fail('missing required env var: REPO (expected owner/name)');
  }
  const [owner, repo] = repoEnv.split('/', 2);
  const { repositoryId, discussions } = await fetchRepositoryAndDiscussions(owner, repo, categoryId);
  const missingPosts = posts.filter((post) => !discussionExists(post, discussions));

  if (missingPosts.length === 0) {
    console.log('No missing blog discussions.');
    return;
  }

  for (const post of missingPosts) {
    const discussion = await createDiscussion(repositoryId, categoryId, post);
    console.log(`Created discussion for ${post.pathname}: ${discussion.url}`);
  }
}

main().catch((error) => {
  fail(error instanceof Error ? error.stack ?? error.message : String(error));
});
