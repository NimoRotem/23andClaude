#!/usr/bin/env python3
"""
Parse pgs_reorganized.md and rebuild the PGS section of test_registry.py.

Replaces everything from '# ── PGS - Cancer' up to (but not including)
'# ── Monogenic' with entries parsed from the markdown file.
"""
import re
import json
import sys

MD_FILE = "pgs_reorganized.md"
REGISTRY_FILE = "test_registry.py"

def parse_markdown(path):
    """Parse the reorganized PGS markdown into a list of test entries."""
    with open(path) as f:
        lines = f.readlines()

    entries = []
    current_category = None
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        # Category header: ## PGS - Cancer
        m = re.match(r'^## (.+)$', line)
        if m:
            current_category = m.group(1).strip()
            i += 1
            continue

        # Entry starts with: - **Name** (`test_id`)
        m = re.match(r'^- \*\*(.+?)\*\* \(`(.+?)`\)$', line)
        if m and current_category:
            name = m.group(1).strip()
            test_id = m.group(2).strip()

            # Next line(s): description + type/params
            description = ""
            test_type = "pgs_score"
            params = {}
            i += 1

            # Collect continuation lines until next entry or blank line
            block_lines = []
            while i < len(lines):
                cl = lines[i].rstrip()
                if cl == '' or cl.startswith('- **') or cl.startswith('## ') or cl.startswith('### '):
                    break
                block_lines.append(cl.strip())
                i += 1

            # Parse block lines
            for bl in block_lines:
                # Skip catalog links
                if bl.startswith('[PGS Catalog]'):
                    continue
                # Extract type
                tm = re.search(r'`type:\s*(\w+)`', bl)
                if tm:
                    test_type = tm.group(1)
                # Extract params
                pm = re.search(r'`params:\s*(\{.+?\})`', bl)
                if pm:
                    try:
                        params = json.loads(pm.group(1))
                    except json.JSONDecodeError:
                        print(f"WARNING: Bad JSON in params for {test_id}: {pm.group(1)}", file=sys.stderr)
                # Description is anything before the backtick markers
                cleaned = re.sub(r'`type:\s*\w+`', '', bl)
                cleaned = re.sub(r'`params:\s*\{.+?\}`', '', cleaned)
                cleaned = cleaned.strip()
                if cleaned and not cleaned.startswith('[PGS Catalog]'):
                    if description:
                        description += " " + cleaned
                    else:
                        description = cleaned

            entries.append({
                'id': test_id,
                'category': current_category,
                'name': name,
                'description': description,
                'test_type': test_type,
                'params': params,
            })
            continue

        i += 1

    return entries


def escape_py_string(s):
    """Escape a string for Python single-quoted literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def format_params(params):
    """Format params dict as Python dict literal."""
    parts = []
    for k, v in params.items():
        if isinstance(v, str):
            parts.append(f"'{k}': '{escape_py_string(v)}'")
        elif isinstance(v, list):
            items = ", ".join(f"'{escape_py_string(x)}'" if isinstance(x, str) else repr(x) for x in v)
            parts.append(f"'{k}': [{items}]")
        else:
            parts.append(f"'{k}': {v!r}")
    return "{" + ", ".join(parts) + "}"


def generate_registry_lines(entries):
    """Generate Python _t() lines grouped by category."""
    output = []
    current_cat = None
    for e in entries:
        if e['category'] != current_cat:
            if current_cat is not None:
                output.append("")
            cat_label = e['category']
            # Generate section comment
            header = f"# \u2500\u2500 {cat_label} "
            header += "\u2500" * (66 - len(header))
            output.append(header)
            current_cat = cat_label

        tid = escape_py_string(e['id'])
        cat = escape_py_string(e['category'])
        name = escape_py_string(e['name'])
        desc = escape_py_string(e['description'])
        ttype = escape_py_string(e['test_type'])
        params = format_params(e['params'])

        output.append(f"_t('{tid}', '{cat}', '{name}',")
        output.append(f"   '{desc}',")
        output.append(f"   '{ttype}', {params})")
        output.append("")

    return "\n".join(output)


def rebuild_registry(entries):
    """Replace PGS section in test_registry.py with new entries."""
    with open(REGISTRY_FILE) as f:
        content = f.read()

    lines = content.split("\n")

    # Find PGS start: "# ── PGS - Cancer"
    pgs_start = None
    for i, line in enumerate(lines):
        if line.startswith("# \u2500\u2500 PGS - Cancer"):
            pgs_start = i
            break
    if pgs_start is None:
        print("ERROR: Could not find '# ── PGS - Cancer' in test_registry.py", file=sys.stderr)
        sys.exit(1)

    # Find PGS end: "# ── Monogenic"
    pgs_end = None
    for i in range(pgs_start + 1, len(lines)):
        if lines[i].startswith("# \u2500\u2500 Monogenic"):
            pgs_end = i
            break
    if pgs_end is None:
        print("ERROR: Could not find '# ── Monogenic' in test_registry.py", file=sys.stderr)
        sys.exit(1)

    print(f"Replacing lines {pgs_start+1}-{pgs_end} (PGS section)", file=sys.stderr)
    print(f"  Old PGS section: {pgs_end - pgs_start} lines", file=sys.stderr)

    new_pgs_lines = generate_registry_lines(entries)

    # Build new file
    before = "\n".join(lines[:pgs_start])
    after = "\n".join(lines[pgs_end:])
    new_content = before + "\n" + new_pgs_lines + "\n" + after

    return new_content


def main():
    entries = parse_markdown(MD_FILE)
    print(f"Parsed {len(entries)} PGS entries from {MD_FILE}", file=sys.stderr)

    # Count categories
    cats = {}
    for e in entries:
        cats[e['category']] = cats.get(e['category'], 0) + 1
    for cat, count in cats.items():
        print(f"  {cat}: {count}", file=sys.stderr)

    new_content = rebuild_registry(entries)

    # Back up old file
    import shutil
    shutil.copy2(REGISTRY_FILE, REGISTRY_FILE + ".bak")
    print(f"Backed up {REGISTRY_FILE} -> {REGISTRY_FILE}.bak", file=sys.stderr)

    with open(REGISTRY_FILE, "w") as f:
        f.write(new_content)
    print(f"Wrote updated {REGISTRY_FILE}", file=sys.stderr)

    # Syntax check
    import ast
    try:
        ast.parse(new_content)
        print("Syntax check: OK", file=sys.stderr)
    except SyntaxError as ex:
        print(f"SYNTAX ERROR: {ex}", file=sys.stderr)
        # Restore backup
        shutil.copy2(REGISTRY_FILE + ".bak", REGISTRY_FILE)
        print("Restored backup due to syntax error!", file=sys.stderr)
        sys.exit(1)

    # Verify by importing
    import importlib
    import importlib.util
    spec = importlib.util.spec_from_file_location("test_registry_check", REGISTRY_FILE)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
        print(f"Import check: OK — {len(mod.TESTS)} tests, {len(mod.CATEGORIES)} categories", file=sys.stderr)
        pgs_tests = [t for t in mod.TESTS if t.get('test_type') in ('pgs_score', 'rsid_pgs_score')]
        print(f"  PGS tests: {len(pgs_tests)}", file=sys.stderr)
        non_pgs = [t for t in mod.TESTS if t.get('test_type') not in ('pgs_score', 'rsid_pgs_score')]
        print(f"  Non-PGS tests: {len(non_pgs)}", file=sys.stderr)
    except Exception as ex:
        print(f"IMPORT ERROR: {ex}", file=sys.stderr)
        shutil.copy2(REGISTRY_FILE + ".bak", REGISTRY_FILE)
        print("Restored backup due to import error!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
