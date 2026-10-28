#!/usr/bin/env python3
"""Refuse text containing a blocked word.

The words are stored only as SHA-256 hashes, in
horizon-prism-rs/src/name_guard/blocked.sha256. A text's candidates are:
each run of letters and digits, every 8-character window of such a run, and
every dotted sequence of runs ("a.b.c" gives "a.b", "b.c" and "a.b.c"), all
lowercased. The name_guard test in the crate applies the same rule.

Usage:
  name-guard.py --staged         added lines of the staged diff
  name-guard.py --message FILE   a commit message
  name-guard.py --range REVS...  the messages of the commits rev-list REVS gives
  name-guard.py --all            every tracked file
Exit 1 and print where, without the word, when one is found.
"""
import hashlib
import os
import re
import subprocess
import sys

ROOT = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True).stdout.strip()
LIST = os.path.join(ROOT, "horizon-prism-rs", "src", "name_guard", "blocked.sha256")
WINDOW = 8


def blocked():
    out = set()
    with open(LIST, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line)
    return out


BLOCKED = blocked()
WORD = re.compile(r"[a-z0-9]+")
DOTTED = re.compile(r"[a-z0-9]+(?:\.[a-z0-9]+)+")


def candidates(text):
    text = text.lower()
    for m in WORD.finditer(text):
        w = m.group(0)
        yield w
        for i in range(len(w) - WINDOW + 1):
            yield w[i:i + WINDOW]
    for m in DOTTED.finditer(text):
        parts = m.group(0).split(".")
        for i in range(len(parts)):
            for j in range(i + 2, len(parts) + 1):
                yield ".".join(parts[i:j])


def hit(text):
    for c in candidates(text):
        if hashlib.sha256(c.encode()).hexdigest() in BLOCKED:
            return True
    return False


def git(*args):
    return subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True, check=True).stdout


def check_lines(label, lines):
    found = False
    for n, line in lines:
        if hit(line):
            print(f"name-guard: blocked word in {label}:{n}", file=sys.stderr)
            found = True
    return found


def main():
    args = sys.argv[1:]
    found = False
    if args == ["--staged"]:
        path, n = None, 0
        for line in git("diff", "--cached", "-U0", "--no-color", "--no-ext-diff").splitlines():
            if line.startswith("+++ "):
                path = line[6:] if line.startswith("+++ b/") else line[4:]
                if hit(path):
                    print(f"name-guard: blocked word in a file name: {path}", file=sys.stderr)
                    found = True
            elif line.startswith("@@"):
                n = int(re.search(r"\+(\d+)", line).group(1))
            elif line.startswith("+"):
                found |= check_lines(path, [(n, line[1:])])
                n += 1
    elif len(args) == 2 and args[0] == "--message":
        with open(args[1], encoding="utf-8") as f:
            found = check_lines("the commit message", enumerate(f.read().splitlines(), 1))
    elif len(args) >= 2 and args[0] == "--range":
        for sha in git("rev-list", *args[1:]).split():
            found |= check_lines(f"the message of {sha[:10]}", enumerate(git("log", "-1", "--format=%B", sha).splitlines(), 1))
    elif args == ["--all"]:
        for rel in git("ls-files").splitlines():
            if hit(rel):
                print(f"name-guard: blocked word in a file name: {rel}", file=sys.stderr)
                found = True
            try:
                with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
                    text = f.read()
            except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
                continue
            found |= check_lines(rel, enumerate(text.splitlines(), 1))
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
