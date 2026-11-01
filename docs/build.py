#!/usr/bin/env python3
"""Build the Postino documentation into a static site.

Usage: python3 docs/build.py [--out DIR] [--version X.Y.Z]

Reads the Markdown pages under docs/, writes HTML to site/ (or --out), and
fails when a page links to a file or heading that does not exist. Only the
Python standard library is needed.
"""

import argparse
import hashlib
import html
import json
import re
import shutil
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent
ROOT = DOCS.parent
REPO = "https://github.com/manuelfedele/postino"

NAV = [
    (
        "Getting started",
        [
            ("index.md", "Overview"),
            ("getting-started/install.md", None),
            ("getting-started/quickstart.md", None),
            ("getting-started/concepts.md", None),
        ],
    ),
    (
        "Guides",
        [
            ("guides/shared-state.md", None),
            ("guides/queues.md", None),
            ("guides/streams.md", None),
            ("guides/pubsub.md", None),
            ("guides/forum.md", None),
            ("guides/agents.md", None),
            ("guides/deployment.md", None),
            ("guides/security.md", None),
        ],
    ),
    (
        "Reference",
        [
            ("reference/commands.md", "Commands"),
            ("reference/http.md", None),
            ("reference/mcp.md", None),
            ("reference/cli.md", None),
            ("reference/configuration.md", None),
        ],
    ),
]

ALERTS = {
    "NOTE": "Note",
    "TIP": "Tip",
    "IMPORTANT": "Important",
    "WARNING": "Warning",
    "CAUTION": "Caution",
}


# Inline Markdown


def slugify(text, seen):
    text = re.sub(r"<[^>]+>", "", text)
    slug = re.sub(r"[^\w\- ]", "", html.unescape(text).lower()).strip()
    slug = re.sub(r" +", "-", slug)
    base, n = slug, 1
    while slug in seen:
        slug = f"{base}-{n}"
        n += 1
    seen.add(slug)
    return slug


def inline(text, links):
    """Render inline Markdown: code spans, links, and bold text."""
    out, pos = [], 0
    for m in re.finditer(r"(`+)(.+?)\1", text):
        out.append(spans(text[pos : m.start()], links))
        out.append(f"<code>{html.escape(m.group(2).strip())}</code>")
        pos = m.end()
    out.append(spans(text[pos:], links))
    return "".join(out)


def spans(text, links):
    text = html.escape(text, quote=False)

    def link(m):
        label, url = m.group(1), m.group(2)
        links.append(url)
        return f'<a href="{html.escape(rewrite(url))}">{label}</a>'

    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", link, text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    return text


def rewrite(url):
    if re.match(r"^[a-z]+:", url) or url.startswith("#"):
        return url
    path, _, anchor = url.partition("#")
    if path.endswith(".md"):
        path = path[:-3] + ".html"
    return path + ("#" + anchor if anchor else "")


def plain(text):
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    return re.sub(r"[`*]", "", text)


# Syntax highlighting

SH_COMMANDS = {
    "postino", "curl", "docker", "printf", "cargo", "brew", "systemctl",
    "launchctl", "export", "sudo", "chmod", "mv", "cp", "tar", "mkdir",
    "echo", "cd", "git", "claude", "codex", "ln", "install", "xattr",
}  # fmt: skip

SH_TOKEN = re.compile(
    r"(?P<comment>(?<!\S)#.*$)"
    r"|(?P<string>'[^']*'|\"(?:\\.|[^\"\\])*\")"
    r"|(?P<op>\|\||&&|[|;\\])"
    r"|(?P<flag>(?<![\w-])--?[A-Za-z][\w-]*)"
    r"|(?P<var>\$\{?\w+\}?)"
    r"|(?P<word>[^\s'\"|;&\\]+)",
    re.M,
)


def span(cls, text):
    return f'<span class="{cls}">{html.escape(text, quote=False)}</span>'


def highlight_sh(code):
    out, pos = [], 0
    command_position, continued = True, False
    for m in SH_TOKEN.finditer(code):
        gap = code[pos : m.start()]
        out.append(html.escape(gap, quote=False))
        if "\n" in gap:
            command_position, continued = command_position or not continued, False
        kind, text = m.lastgroup, m.group()
        if kind == "op":
            out.append(span("op", text))
            continued = text == "\\"
            command_position = command_position or not continued
        elif kind == "word" and command_position:
            out.append(
                span("cmd", text)
                if text in SH_COMMANDS
                else html.escape(text, quote=False)
            )
            command_position = "=" in text and not text.startswith("-")
        elif kind == "word":
            out.append(html.escape(text, quote=False))
        else:
            out.append(span(kind, text))
            command_position = False
        pos = m.end()
    out.append(html.escape(code[pos:], quote=False))
    return "".join(out)


JSON_TOKEN = re.compile(
    r"(?P<string>\"(?:\\.|[^\"\\])*\")(?P<colon>\s*:)?"
    r"|(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
    r"|(?P<literal>\btrue\b|\bfalse\b|\bnull\b)"
)


def highlight_json(code):
    out, pos = [], 0
    for m in JSON_TOKEN.finditer(code):
        out.append(html.escape(code[pos : m.start()], quote=False))
        if m.group("string"):
            cls = "key" if m.group("colon") else "string"
            out.append(span(cls, m.group("string")))
            out.append(html.escape(m.group("colon") or "", quote=False))
        elif m.group("number"):
            out.append(span("number", m.group()))
        else:
            out.append(span("literal", m.group()))
        pos = m.end()
    out.append(html.escape(code[pos:], quote=False))
    return "".join(out)


def highlight_ini(code):
    lines = []
    for line in code.split("\n"):
        if re.match(r"\s*[#;]", line):
            lines.append(span("comment", line))
        elif re.match(r"\s*\[.*\]\s*$", line):
            lines.append(span("key", line))
        elif "=" in line:
            key, _, value = line.partition("=")
            lines.append(span("key", key) + "=" + span("string", value))
        else:
            lines.append(html.escape(line, quote=False))
    return "\n".join(lines)


def highlight(lang, code):
    if lang in ("sh", "bash", "shell", "console"):
        return highlight_sh(code)
    if lang == "json":
        return highlight_json(code)
    if lang in ("ini", "toml", "systemd"):
        return highlight_ini(code)
    return html.escape(code, quote=False)


def code_block(lang, code):
    label = {"sh": "Shell", "json": "JSON", "ini": "INI", "text": "Output"}.get(
        lang, ""
    )
    return (
        f'<div class="code" data-lang="{html.escape(lang)}">'
        f'<pre tabindex="0"><code>{highlight(lang, code)}</code></pre>'
        f'<button class="copy" type="button" aria-label="Copy {label or "code"}">Copy</button>'
        "</div>"
    )


# Block Markdown


FENCE = re.compile(r"^```(\w*)(?:\s+\[([^\]]+)\])?\s*$")


class Page:
    def __init__(self, path):
        self.path = path
        self.source = (DOCS / path).read_text()
        self.ids = set()
        self.toc = []
        self.links = []
        self.sections = []
        self.title = ""
        self.description = ""
        self.parts = []
        self.body = self.blocks(self.source.split("\n"))

    def blocks(self, lines, nested=False):
        out, i = [], 0
        while i < len(lines):
            line = lines[i]
            if not line.strip():
                i += 1
                continue
            if FENCE.match(line):
                group, i = self.fences(lines, i)
                out.append(group)
                continue
            if m := re.match(r"^(#{1,4})\s+(.*?)\s*#*$", line):
                out.append(self.heading(len(m.group(1)), m.group(2)))
                i += 1
                continue
            if (
                line.startswith("|")
                and i + 1 < len(lines)
                and re.match(r"^\|[\s:|-]+\|$", lines[i + 1])
            ):
                end = i + 2
                while end < len(lines) and lines[end].startswith("|"):
                    end += 1
                out.append(self.table(lines[i:end]))
                i = end
                continue
            if line.startswith(">"):
                end = i
                while end < len(lines) and lines[end].startswith(">"):
                    end += 1
                out.append(
                    self.quote([re.sub(r"^> ?", "", line) for line in lines[i:end]])
                )
                i = end
                continue
            if m := re.match(r"^([-*]|\d+\.)\s+", line):
                ordered = m.group(1)[0].isdigit()
                items, i = self.list_items(lines, i, ordered)
                tag = "ol" if ordered else "ul"
                out.append(
                    f"<{tag}>" + "".join(f"<li>{x}</li>" for x in items) + f"</{tag}>"
                )
                continue
            end = i
            while (
                end < len(lines)
                and lines[end].strip()
                and not self.starts_block(lines[end])
            ):
                end += 1
            text = " ".join(part.strip() for part in lines[i:end])
            if not nested and not self.description and self.title:
                self.description = plain(text)
            self.section_text(plain(text))
            out.append(f"<p>{inline(text, self.links)}</p>")
            i = end
        if not nested:
            self.parts = out
        return "\n".join(out)

    @staticmethod
    def starts_block(line):
        return bool(
            FENCE.match(line)
            or re.match(r"^#{1,4}\s", line)
            or line.startswith(("|", ">"))
            or re.match(r"^([-*]|\d+\.)\s+", line)
        )

    def fences(self, lines, i):
        """Parse one fence, or a run of labeled fences that become tabs."""
        blocks = []
        while i < len(lines):
            m = FENCE.match(lines[i])
            if not m:
                break
            lang, label = m.group(1) or "text", m.group(2)
            end = i + 1
            while end < len(lines) and lines[end].rstrip() != "```":
                end += 1
            if end == len(lines):
                raise SystemExit(f"{self.path}: unclosed code fence at line {i + 1}")
            code = "\n".join(lines[i + 1 : end])
            self.section_text(code)
            blocks.append((lang, label, code))
            i = end + 1
            if not label:
                break
            nxt = i
            while nxt < len(lines) and not lines[nxt].strip():
                nxt += 1
            m = FENCE.match(lines[nxt]) if nxt < len(lines) else None
            if not (m and m.group(2)):
                break
            i = nxt
        if len(blocks) == 1 and not blocks[0][1]:
            return code_block(blocks[0][0], blocks[0][2]), i
        tabs, panels = [], []
        for n, (lang, label, code) in enumerate(blocks):
            label = label or lang
            selected = "true" if n == 0 else "false"
            tabs.append(
                f'<button role="tab" type="button" aria-selected="{selected}" '
                f'tabindex="{0 if n == 0 else -1}" data-tab="{html.escape(label)}">{html.escape(label)}</button>'
            )
            hidden = "" if n == 0 else " hidden"
            panels.append(
                f'<div role="tabpanel" data-tab="{html.escape(label)}"{hidden}>{code_block(lang, code)}</div>'
            )
        return (
            '<div class="tabs"><div class="tablist" role="tablist">'
            + "".join(tabs)
            + "</div>"
            + "".join(panels)
            + "</div>",
            i,
        )

    def heading(self, level, text):
        rendered = inline(text, self.links)
        if level == 1:
            self.title = plain(text)
            return f"<h1>{rendered}</h1>"
        slug = slugify(rendered, self.ids)
        if level <= 3:
            self.toc.append((level, slug, rendered))
        self.sections.append({"heading": plain(text), "anchor": slug, "text": []})
        return (
            f'<h{level} id="{slug}">{rendered}'
            f'<a class="anchor" href="#{slug}" aria-label="Link to this section"></a></h{level}>'
        )

    def section_text(self, text):
        if self.sections:
            self.sections[-1]["text"].append(text)
        else:
            self.sections.append({"heading": None, "anchor": "", "text": [text]})

    def table(self, rows):
        def cells(row):
            row = row.strip()
            row = row[1:] if row.startswith("|") else row
            row = row[:-1] if row.endswith("|") and not row.endswith("\\|") else row
            return [c.strip().replace("\\|", "|") for c in re.split(r"(?<!\\)\|", row)]

        head, spec, body = cells(rows[0]), cells(rows[1]), [cells(r) for r in rows[2:]]
        if [h.lower() for h in head] == ["if you want to", "read"]:
            return self.cards(body)
        aligns = []
        for s in spec:
            if s.startswith(":") and s.endswith(":"):
                aligns.append("center")
            elif s.endswith(":"):
                aligns.append("right")
            else:
                aligns.append("")

        def cell(tag, text, n):
            align = aligns[n] if n < len(aligns) else ""
            style = f' class="align-{align}"' if align else ""
            return f"<{tag}{style}>{inline(text, self.links)}</{tag}>"

        for row in body:
            self.section_text(" ".join(plain(c) for c in row))
        empty_head = all(not h for h in head[1:]) and len(head) > 1
        thead = (
            "<thead><tr>"
            + "".join(cell("th", h, n) for n, h in enumerate(head))
            + "</tr></thead>"
        )
        rows_html = "".join(
            "<tr>" + "".join(cell("td", c, n) for n, c in enumerate(r)) + "</tr>"
            for r in body
        )
        cls = ' class="headless"' if empty_head else ""
        return f'<div class="table"><table{cls}>{thead}<tbody>{rows_html}</tbody></table></div>'

    def cards(self, rows):
        """Render an "If you want to | Read" table as a grid of cards."""
        cards = []
        for want, read in rows:
            m = re.fullmatch(r"\[([^\]]+)\]\(([^)\s]+)\)", read)
            if not m:
                raise SystemExit(f"{self.path}: a Read cell must be a single link")
            label, url = m.groups()
            self.links.append(url)
            self.section_text(f"{plain(want)}: {label}")
            cards.append(
                f'<a class="card" href="{html.escape(rewrite(url))}">'
                f'<span class="eyebrow">{html.escape(group_of(resolve(self.path, url)))}</span>'
                f"<strong>{html.escape(label)}</strong>"
                f"<small>{inline(want, self.links)}</small></a>"
            )
        return f'<div class="cards">{"".join(cards)}</div>'

    def quote(self, lines):
        m = re.match(r"^\[!(\w+)\]\s*$", lines[0]) if lines else None
        if m and m.group(1) in ALERTS:
            kind = m.group(1)
            inner = self.blocks(lines[1:], nested=True)
            return (
                f'<div class="alert alert-{kind.lower()}" role="note">'
                f'<p class="alert-title">{ALERTS[kind]}</p>{inner}</div>'
            )
        return f"<blockquote>{self.blocks(lines, nested=True)}</blockquote>"

    def list_items(self, lines, i, ordered):
        items = []
        marker = r"^\d+\.\s+" if ordered else r"^[-*]\s+"
        while i < len(lines) and re.match(marker, lines[i]):
            text = re.sub(marker, "", lines[i])
            i += 1
            while (
                i < len(lines)
                and lines[i].startswith(("  ", "\t"))
                and lines[i].strip()
            ):
                text += " " + lines[i].strip()
                i += 1
            self.section_text(plain(text))
            items.append(inline(text, self.links))
        return items, i


# Site


def output_path(md):
    return md[:-3] + ".html"


def relative(target, frm):
    """A link from page `frm` to site path `target`."""
    depth = frm.count("/")
    return "../" * depth + target


def resolve(page_path, url):
    """The docs path a relative link from `page_path` points to."""
    path = url.partition("#")[0]
    if not path:
        return page_path
    parts = []
    for part in (Path(page_path).parent / path).as_posix().split("/"):
        if part == "..":
            if parts:
                parts.pop()
        elif part not in ("", "."):
            parts.append(part)
    return "/".join(parts)


def group_of(path):
    for group, entries in NAV:
        if any(p == path for p, _ in entries):
            return group
    return ""


def check_links(pages):
    errors = []
    by_path = {p.path: p for p in pages}
    for page in pages:
        for url in page.links:
            if re.match(r"^[a-z]+:", url):
                continue
            anchor = url.partition("#")[2]
            target = resolve(page.path, url)
            if target not in by_path:
                errors.append(f"{page.path}: link to missing page {url}")
            elif anchor and anchor not in by_path[target].ids:
                errors.append(f"{page.path}: link to missing heading {url}")
    return errors


ICONS = {
    "menu": '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16"/></svg>',
    "search": '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="6.5"/><path d="m16 16 4 4"/></svg>',
    "github": '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="fill" d="M12 2.5a9.5 9.5 0 0 0-3 18.5c.5.1.7-.2.7-.5v-1.7c-2.7.6-3.2-1.2-3.2-1.2-.5-1.1-1.1-1.4-1.1-1.4-.9-.6.1-.6.1-.6 1 .1 1.5 1 1.5 1 .9 1.5 2.3 1.1 2.9.8.1-.6.3-1.1.6-1.3-2.1-.2-4.3-1.1-4.3-4.7 0-1 .4-1.9 1-2.6-.1-.2-.4-1.2.1-2.5 0 0 .8-.3 2.6 1a9 9 0 0 1 4.8 0c1.8-1.3 2.6-1 2.6-1 .5 1.3.2 2.3.1 2.5.6.7 1 1.6 1 2.6 0 3.7-2.2 4.5-4.3 4.7.3.3.6.9.6 1.8v2.7c0 .3.2.6.7.5A9.5 9.5 0 0 0 12 2.5z"/></svg>',
}


def fingerprint(text):
    return hashlib.sha256(text.encode()).hexdigest()[:10]


def render(page, pages, flat, version, stamps, root=None):
    here = output_path(page.path)
    if root is None:
        root = relative("", here)

    def link(target):
        return root + target if root.startswith("/") else relative(target, here)

    nav = []
    for group, entries in NAV:
        items = []
        for path, label in entries:
            target = next(p for p in pages if p.path == path)
            current = ' aria-current="page"' if path == page.path else ""
            items.append(
                f'<li><a href="{link(output_path(path))}"{current}>{html.escape(label or target.title)}</a></li>'
            )
        nav.append(
            f'<div class="nav-group"><p class="eyebrow">{group}</p><ul>{"".join(items)}</ul></div>'
        )

    toc = "".join(
        f'<li class="toc-{level}"><a href="#{slug}">{text}</a></li>'
        for level, slug, text in page.toc
    )
    toc_html = (
        f'<aside class="toc" aria-label="On this page"><p class="eyebrow">On this page</p><ul>{toc}</ul></aside>'
        if toc
        else '<aside class="toc"></aside>'
    )

    n = flat.index(page.path)
    pager = []
    for cls, idx, word in (("prev", n - 1, "Previous"), ("next", n + 1, "Next")):
        if 0 <= idx < len(flat):
            other = next(p for p in pages if p.path == flat[idx])
            pager.append(
                f'<a class="{cls}" href="{link(output_path(other.path))}" rel="{cls}">'
                f'<span class="eyebrow">{word}</span><strong>{html.escape(nav_label(other))}</strong></a>'
            )
        else:
            pager.append("<span></span>")

    if page.path == "index.md":
        main = home(page)
    else:
        main = (
            f'<article class="prose panel"><p class="eyebrow">{group_of(page.path)}</p>\n'
            f"{page.body}\n</article>"
        )

    title = (
        "Postino documentation"
        if page.path == "index.md"
        else f"{page.title} | Postino"
    )
    description = (
        page.description[:200].rsplit(" ", 1)[0]
        if len(page.description) > 200
        else page.description
    )
    md = link(page.path)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<meta name="description" content="{html.escape(description)}">
<meta name="color-scheme" content="dark">
<meta name="theme-color" content="#081116">
<link rel="icon" href="{root}favicon.svg" type="image/svg+xml">
<link rel="alternate" type="text/markdown" href="{md}" title="This page as Markdown">
<link rel="stylesheet" href="{root}assets/style.css?v={stamps["style.css"]}">
<script src="{root}assets/search-index.js?v={stamps["search-index.js"]}" defer></script>
<script src="{root}assets/docs.js?v={stamps["docs.js"]}" defer></script>
</head>
<body data-root="{root}">
<a class="skip" href="#content">Skip to content</a>
<header class="top">
  <div class="top-inner">
    <button class="icon-button menu" type="button" aria-controls="nav" aria-expanded="false" aria-label="Show navigation">{ICONS["menu"]}</button>
    <a class="brand" href="{link("index.html")}" aria-label="Postino documentation home">
      <span class="mark" aria-hidden="true">PO</span>
      <span class="brand-text"><span class="kicker">In-memory server for agents</span><span class="word">Postino</span></span>
    </a>
    <span class="chip" title="Postino listens on port 2790 by default"><span class="dot"></span>Port 2790</span>
    <div class="search" role="search">
      {ICONS["search"]}
      <input type="search" id="search" placeholder="Search the docs" autocomplete="off" spellcheck="false" aria-label="Search the documentation" aria-controls="search-results" aria-expanded="false">
      <kbd>/</kbd>
      <div class="results" id="search-results" role="listbox" hidden></div>
    </div>
    <span class="version" title="Documented version">v{version}</span>
    <a class="icon-button" href="{REPO}" aria-label="Source on GitHub">{ICONS["github"]}</a>
  </div>
</header>
<div class="layout">
  <nav id="nav" class="sidebar" aria-label="Documentation">{"".join(nav)}</nav>
  <main id="content" class="content">
{main}
    <nav class="pager" aria-label="Previous and next page">{"".join(pager)}</nav>
    <footer class="foot">
      <span>Postino {version} · port 2790 · MIT license</span>
      <a href="{md}">View as Markdown</a>
      <a href="{REPO}/edit/main/docs/{page.path}">Edit this page</a>
    </footer>
  </main>
  {toc_html}
</div>
</body>
</html>
"""


def text_of(fragment):
    return html.unescape(re.sub(r"<[^>]+>", "", fragment)).strip()


def home(page):
    """The overview: a hero built from the page's opening, then the rest of it."""
    first = next(i for i, part in enumerate(page.parts) if part.startswith("<h2"))
    opening, rest = page.parts[:first], page.parts[first:]
    paragraphs = [p[3:-4] for p in opening if p.startswith("<p>")]
    code = next(p for p in opening if p.startswith('<div class="code"'))
    command = text_of(re.search(r"<code>(.*?)</code>", code, re.S).group(1))
    design = "".join(rest[rest.index(next(p for p in rest if 'id="design"' in p)) :])
    points = [
        text_of(x).rstrip(".") for x in re.findall(r"<strong>(.*?)</strong>", design)
    ]
    checks = "".join(f"<li>{html.escape(x)}</li>" for x in points)
    return f"""<section class="hero" aria-labelledby="page-title">
  <div class="intro">
    <p class="eyebrow">Postino documentation</p>
    <h1 id="page-title">Memory, streams, and a forum for your agents.</h1>
    <p class="lead">{paragraphs[0]}</p>
    <div class="release"><strong>$ {html.escape(command)}</strong><span>{paragraphs[1]}</span></div>
  </div>
  <aside class="trust" aria-label="Design">
    <p class="eyebrow">Design</p>
    <h2>One binary. Nothing else to run.</h2>
    <ul>{checks}</ul>
  </aside>
</section>
<article class="prose home">
{chr(10).join(rest)}
</article>"""


def nav_label(page):
    for _, entries in NAV:
        for path, label in entries:
            if path == page.path:
                return label or page.title
    return page.title


def search_index(pages):
    entries = []
    for page in pages:
        url = output_path(page.path)
        for section in page.sections:
            text = re.sub(r"\s+", " ", " ".join(section["text"])).strip()
            entries.append(
                {
                    "p": nav_label(page),
                    "h": section["heading"],
                    "u": url + ("#" + section["anchor"] if section["anchor"] else ""),
                    "t": text[:600],
                }
            )
    return entries


def llms_txt(pages):
    lines = [
        "# Postino",
        "",
        "> In-memory data and messaging server for agents, on port 2790: data structures,",
        "> streams, pub/sub, inboxes, broadcasts, and rooms over HTTP, Server-Sent Events, and MCP.",
        "",
    ]
    for group, entries in NAV:
        lines.append(f"## {group}")
        lines.append("")
        for path, _ in entries:
            page = next(p for p in pages if p.path == path)
            summary = page.description.split(". ")[0].rstrip(".")
            lines.append(f"- [{page.title}]({path}): {summary}")
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Build the Postino documentation.")
    parser.add_argument("--out", default=str(ROOT / "site"), help="output directory")
    parser.add_argument(
        "--version", help="version shown in the header (default: Cargo.toml)"
    )
    parser.add_argument(
        "--base", default="/postino/", help="URL path the site is served under"
    )
    args = parser.parse_args()
    version = args.version
    if not version:
        m = re.search(r'^version = "([^"]+)"', (ROOT / "Cargo.toml").read_text(), re.M)
        if not m:
            sys.exit("no version in Cargo.toml; pass --version")
        version = m.group(1)

    flat = [path for _, entries in NAV for path, _ in entries]
    listed = set(flat)
    found = {p.relative_to(DOCS).as_posix() for p in DOCS.rglob("*.md")}
    unlisted = sorted(found - listed)
    if unlisted:
        sys.exit(f"pages missing from NAV: {', '.join(unlisted)}")
    pages = [Page(path) for path in flat]
    search = (
        "window.POSTINO_SEARCH = "
        + json.dumps(search_index(pages), separators=(",", ":"))
        + ";\n"
    )
    stamps = {
        asset.name: fingerprint(asset.read_text())
        for asset in (DOCS / "theme").iterdir()
    }
    stamps["search-index.js"] = fingerprint(search)
    errors = check_links(pages)
    for page in pages:
        if not page.title:
            errors.append(f"{page.path}: no title")
    if errors:
        sys.exit("\n".join(errors))

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    (out / "assets").mkdir(parents=True)
    for page in pages:
        target = out / output_path(page.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render(page, pages, flat, version, stamps))
        shutil.copy(DOCS / page.path, out / page.path)
    for asset in (DOCS / "theme").iterdir():
        shutil.copy(asset, out / "assets" / asset.name)
    shutil.copytree(DOCS / "brand", out / "brand")
    (out / "assets" / "search-index.js").write_text(search)
    shutil.copy(DOCS / "brand" / "logo.svg", out / "favicon.svg")
    (out / "llms.txt").write_text(llms_txt(pages))
    (out / ".nojekyll").write_text("")
    # Served for any missing path, so its links are absolute.
    not_found = render(pages[0], pages, flat, version, stamps, root=args.base)
    not_found = re.sub(
        r'<section class="hero".*?</article>\s*<nav class="pager".*?</nav>',
        '<article class="prose panel"><p class="eyebrow">Not found</p><h1>Page not found</h1><p>There is no page at this address. '
        f'Start from the <a href="{args.base}index.html">overview</a>, or search for what you need.</p></article>',
        not_found,
        flags=re.S,
    ).replace(' aria-current="page"', "")
    (out / "404.html").write_text(not_found)
    print(f"built {len(pages)} pages into {out}")


if __name__ == "__main__":
    main()
