# In-repo Wiki (bilingual)

Versioned wiki pages (GitHub Wiki alternative that stays in git).

| Language | Home |
|----------|------|
| English | [en/Home.md](./en/Home.md) |
| 中文 | [zh/Home.md](./zh/Home.md) |

### Publishing to GitHub Wiki

Source of truth is **this directory**. Sync to the Wiki tab:

```bash
./scripts/sync_github_wiki.sh
```

The script flattens `en/*.md` → `Page.md` and `zh/*.md` → `Zh-Page.md`, rewrites
cross-links to GitHub Wiki `[[wikilinks]]`, and pushes
`https://github.com/lanpishu6300/multiraft.wiki.git`.
