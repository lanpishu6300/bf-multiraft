#!/usr/bin/env bash
# Sync docs/wiki/{en,zh} → GitHub Wiki (flat pages + _Sidebar).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WIKI_URL="${WIKI_URL:-https://github.com/lanpishu6300/multiraft.wiki.git}"
TMP="${TMPDIR:-/tmp}/multiraft.wiki-sync.$$"
REPO_BLOB="https://github.com/lanpishu6300/multiraft/blob/main"

cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

git clone --depth 1 "$WIKI_URL" "$TMP"

# English pages keep original names; Chinese get Zh- prefix.
copy_lang() {
  local src_dir="$1" prefix="$2"
  local f base out
  for f in "$src_dir"/*.md; do
    [[ -f "$f" ]] || continue
    base="$(basename "$f" .md)"
    if [[ -n "$prefix" ]]; then
      out="${prefix}${base}.md"
    else
      out="${base}.md"
    fi
    cp "$f" "$TMP/$out"
  done
}

copy_lang "$ROOT/docs/wiki/en" ""
copy_lang "$ROOT/docs/wiki/zh" "Zh-"

# Rewrite relative / wiki-internal links for the flat GitHub Wiki.
python3 - <<'PY' "$TMP" "$REPO_BLOB"
import pathlib, re, sys

wiki = pathlib.Path(sys.argv[1])
blob = sys.argv[2]

# Map in-repo relative targets → wiki page names
EN = {
    "Home.md": "Home",
    "Getting-Started.md": "Getting-Started",
    "Architecture.md": "Architecture",
    "Consistency.md": "Consistency",
    "FAQ.md": "FAQ",
    "Roadmap.md": "Roadmap",
    "Related-Projects.md": "Related-Projects",
}
ZH = {k: "Zh-" + v if v != "Home" else "Zh-Home" for k, v in {
    "Home.md": "Home",
    "Getting-Started.md": "Getting-Started",
    "Architecture.md": "Architecture",
    "Consistency.md": "Consistency",
    "FAQ.md": "FAQ",
    "Roadmap.md": "Roadmap",
    "Related-Projects.md": "Related-Projects",
}.items()}

def to_wiki_link(label: str, page: str) -> str:
    if label.replace(" ", "-") == page or label == page:
        return f"[[{page}]]"
    return f"[[{label}|{page}]]"

def rewrite(text: str, is_zh: bool) -> str:
    # Cross-lang home
    text = text.replace("[en/Home.md](../en/Home.md)", "[[English|Home]]")
    text = text.replace("[zh/Home.md](../zh/Home.md)", "[[中文|Zh-Home]]")
    text = text.replace("**English：** [en/Home.md](../en/Home.md)", "**English：** [[English|Home]]")
    text = text.replace("**中文版：** [zh/Home.md](../zh/Home.md)", "**中文版：** [[中文|Zh-Home]]")
    text = text.replace("**中文：** [zh/Home.md](../zh/Home.md)", "**中文：** [[中文|Zh-Home]]")
    text = text.replace("**English：** [en/Consistency.md](../en/Consistency.md)",
                        "**English：** [[Consistency|Consistency]]")
    text = text.replace("**中文：** [zh/Consistency.md](../zh/Consistency.md)",
                        "**中文：** [[一致性|Zh-Consistency]]")
    text = text.replace("**English：** [en/Architecture.md](../en/Architecture.md)",
                        "**English：** [[Architecture]]")
    text = text.replace("**中文：** [zh/Architecture.md](../zh/Architecture.md)",
                        "**中文：** [[架构|Zh-Architecture]]")
    text = text.replace("**English：** [en/Getting-Started.md](../en/Getting-Started.md)",
                        "**English：** [[Getting Started|Getting-Started]]")
    text = text.replace("**中文：** [zh/Getting-Started.md](../zh/Getting-Started.md)",
                        "**中文：** [[快速开始|Zh-Getting-Started]]")
    text = text.replace("**English：** [en/FAQ.md](../en/FAQ.md)", "**English：** [[FAQ]]")
    text = text.replace("**中文：** [zh/FAQ.md](../zh/FAQ.md)", "**中文：** [[常见问题|Zh-FAQ]]")
    text = text.replace("**English：** [en/Roadmap.md](../en/Roadmap.md)", "**English：** [[Roadmap]]")
    text = text.replace("**中文：** [zh/Roadmap.md](../zh/Roadmap.md)", "**中文：** [[路线图|Zh-Roadmap]]")
    text = text.replace("**English：** [en/Related-Projects.md](../en/Related-Projects.md)",
                        "**English：** [[Related Projects|Related-Projects]]")
    text = text.replace("**中文：** [zh/Related-Projects.md](../zh/Related-Projects.md)",
                        "**中文：** [[相关项目|Zh-Related-Projects]]")

    # Same-lang ./Page.md links
    mapping = ZH if is_zh else EN
    labels_zh = {
        "Home": "首页",
        "Getting-Started": "快速开始",
        "Architecture": "架构",
        "Consistency": "一致性与测试",
        "FAQ": "常见问题",
        "Roadmap": "路线图",
        "Related-Projects": "相关项目",
    }
    labels_en = {
        "Home": "Home",
        "Getting-Started": "Getting Started",
        "Architecture": "Architecture",
        "Consistency": "Consistency & testing",
        "FAQ": "FAQ",
        "Roadmap": "Roadmap",
        "Related-Projects": "Related Projects",
    }

    # Resolve relative markdown links from docs/wiki/{en|zh}/Page.md
    wiki_cwd = pathlib.PurePosixPath("docs/wiki/zh" if is_zh else "docs/wiki/en")

    def repl_md(m):
        label, path = m.group(1), m.group(2)
        if path.startswith("http://") or path.startswith("https://"):
            return m.group(0)
        base = path.rsplit("/", 1)[-1]
        if base in mapping and (path.startswith("./") or path.startswith("../")):
            # Same-tree wiki page (./X or ../en|zh/X)
            if "/en/" in path or path.startswith("../en/"):
                page = EN.get(base, mapping.get(base))
            elif "/zh/" in path or path.startswith("../zh/"):
                page = ZH.get(base)
            else:
                page = mapping.get(base)
            if page:
                return to_wiki_link(label, page)
        # Repo file → canonical blob URL
        if path.startswith("."):
            resolved = pathlib.PurePosixPath(wiki_cwd / path)
            # normalize .. segments
            parts = []
            for p in resolved.parts:
                if p == "..":
                    if parts:
                        parts.pop()
                elif p != ".":
                    parts.append(p)
            rel = "/".join(parts)
            return f"[{label}]({blob}/{rel})"
        return m.group(0)

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", repl_md, text)
    return text

for path in wiki.glob("*.md"):
    if path.name == "_Sidebar.md":
        continue
    is_zh = path.name.startswith("Zh-")
    path.write_text(rewrite(path.read_text(), is_zh), encoding="utf-8")

sidebar = """## English
* [[Home]]
* [[Getting Started|Getting-Started]]
* [[Architecture]]
* [[Consistency & testing|Consistency]]
* [[FAQ]]
* [[Roadmap]]
* [[Related Projects|Related-Projects]]

## 中文
* [[首页|Zh-Home]]
* [[快速开始|Zh-Getting-Started]]
* [[架构|Zh-Architecture]]
* [[一致性与测试|Zh-Consistency]]
* [[常见问题|Zh-FAQ]]
* [[路线图|Zh-Roadmap]]
* [[相关项目|Zh-Related-Projects]]
"""
(wiki / "_Sidebar.md").write_text(sidebar, encoding="utf-8")
print("Rewrote wiki pages in", wiki)
PY

cd "$TMP"
git add -A
if git diff --cached --quiet; then
  echo "Wiki already up to date."
  exit 0
fi
git -c user.email="$(git -C "$ROOT" log -1 --format='%ae')" \
    -c user.name="$(git -C "$ROOT" log -1 --format='%an')" \
    commit -m "docs(wiki): sync from docs/wiki (en + zh)"
git push origin HEAD:master 2>/dev/null || git push origin HEAD:main
echo "Pushed GitHub Wiki: https://github.com/lanpishu6300/multiraft/wiki"
