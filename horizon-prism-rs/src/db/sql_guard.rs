//! Guards over the crate's own SQL text, read from `src/` at test time.
//!
//! Public tables name accounts by `dids` id and records by key, so their SQL
//! never takes a URI apart with `split_part`, never matches a URI with
//! `LIKE`, and never names a text DID or URI column of theirs. A follow,
//! block, like or repost pair check looks up each account's id on its own,
//! and a URI is built for a page's rows, not for every candidate row.
//!
//! The scan works on the text of the files, not on parsed Rust: comments
//! and test modules are blanked, every string literal's text is joined in
//! file order (so SQL split over several literals or `format!` pieces reads
//! as one), and a Rust statement boundary between two literals ends an SQL
//! statement. Each finding names its file and line. The allowlist below is
//! by file and check, with the exact number of findings expected, so a new
//! finding in an allowed file fails as well as one anywhere else.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use regex::Regex;

/// The allowed findings: (check, file under `src/`, how many, why).
const ALLOWED: &[(&str, &str, usize, &str)] = &[
    ("split_part", "admin/pages/labels.rs", 1, "the operator's labels keep their subject as text"),
    ("split_part", "hydration/labels.rs", 1, "the operator's labels keep their subject as text"),
    ("split_part", "indexing/narrowing.rs", 1, "constellation_counts keeps its target as text"),
    ("split_part", "hydration/notifications.rs", 2, "notification rows keep their record URI as text"),
    ("split_part", "private_tier/ingest.rs", 10, "the private tier keeps text URIs on its own rows"),
    ("split_part", "private_tier/schema.rs", 20, "the private tier keeps text URIs on its own rows"),
    ("split_part", "refract/purge.rs", 1, "notification rows keep their record URI as text"),
    ("split_part", "thread_outline/flush.rs", 4, "a stream id in worker state, not a URI"),
    ("uri_like", "admin/pages/labels.rs", 1, "the operator's labels keep their subject as text"),
    ("uri_like", "cli/commands/identity_compare.rs", 3, "samples accounts by DID method"),
    ("uri_like", "indexing/narrowing.rs", 1, "constellation_counts keeps its target as text"),
    ("uri_like", "hydration/notifications.rs", 3, "notification rows' own text, and an odd subject's text through ref_text"),
    ("uri_like", "refract/purge.rs", 1, "Refract's record reads keep their URI as text"),
];

/// The tables in the compact layout: ids, keys and references, no text DIDs
/// or URIs. `subscribed_lists` and `subscribed_list_items` are not in the
/// compact layout yet, so they are not listed.
const COMPACT: &[&str] = &[
    "posts",
    "post_search",
    "likes",
    "reposts",
    "follows",
    "blocks",
    "feed_items",
    "post_counts",
    "actor_counts",
    "record_tombstones",
    "actors",
    "lists",
    "list_items",
    "list_blocks",
    "list_optouts",
    "starter_packs",
    "starter_pack_counts",
    "threadgates",
    "postgates",
    "nearhorizon_poll_posts",
    "labeler_services",
    "actor_self_records",
    "member_post_refs",
    "odd_refs",
    "rkey_aliases",
];

/// Text DID and URI columns, and the other text copies of record fields,
/// that the compact tables do not have. `post_search` keeps `text`.
const DROPPED: &[&str] = &[
    "did",
    "author_did",
    "subject_did",
    "originator_did",
    "owner_did",
    "uri",
    "subject_uri",
    "post_uri",
    "quote_uri",
    "reply_parent_uri",
    "reply_root_uri",
    "root_uri",
    "parent_uri",
    "list_uri",
    "via_uri",
    "pack_uri",
    "subject_cid",
    "mention_dids",
    "embed_type",
    "media_type",
    "langs",
    "facets",
    "text",
];

/// Of [`DROPPED`], the names checked unqualified too; the others are common
/// words in SQL (`text` as a type, say).
const DROPPED_BARE: &[&str] = &[
    "did",
    "author_did",
    "subject_did",
    "originator_did",
    "owner_did",
    "uri",
    "subject_uri",
    "post_uri",
    "quote_uri",
    "reply_parent_uri",
    "reply_root_uri",
    "root_uri",
    "parent_uri",
    "list_uri",
    "via_uri",
    "pack_uri",
    "subject_cid",
    "mention_dids",
];

/// The pair tables whose existence checks must look up each id on its own.
const PAIR_TABLES: &str = "follows|blocks|likes|reposts";

/// Words that can follow a table name where an alias would be.
const NOT_ALIASES: &[&str] = &[
    "where", "on", "join", "left", "right", "inner", "outer", "cross", "full", "set", "values", "select", "using",
    "order", "group", "limit", "natural", "union", "as", "lateral", "returning", "for", "with", "and", "or", "not",
    "do", "default", "offset", "having", "except", "intersect", "tablesample",
];

/// One finding: which check, where, and the SQL around it.
#[derive(Debug)]
struct Finding {
    check: &'static str,
    file: String,
    line: usize,
    near: String,
}

/// A source file with comments and test modules blanked (same length, same
/// lines), and the SQL stream: every string literal's text in file order,
/// with `\0` where a Rust statement ends between two literals.
struct Source {
    rel: String,
    text: String,
    sql: String,
    /// For each byte of `sql`, its offset in `text`.
    at: Vec<usize>,
}

impl Source {
    fn line(&self, sql_offset: usize) -> usize {
        let at = self.at.get(sql_offset).copied().unwrap_or(self.text.len());
        self.text[..at].bytes().filter(|b| *b == b'\n').count() + 1
    }

    fn finding(&self, check: &'static str, sql_offset: usize) -> Finding {
        let from = floor(&self.sql, sql_offset.saturating_sub(40));
        let to = floor(&self.sql, (sql_offset + 110).min(self.sql.len()));
        let near = self.sql[from..to].replace('\0', " ").split_whitespace().collect::<Vec<_>>().join(" ");
        Finding { check, file: self.rel.clone(), line: self.line(sql_offset), near }
    }
}

fn floor(s: &str, mut i: usize) -> usize {
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// `pattern` compiled, once per pattern (a clone shares the compiled form).
fn re(pattern: &str) -> Regex {
    static CACHE: LazyLock<Mutex<HashMap<String, Regex>>> = LazyLock::new(Mutex::default);
    let mut cache = CACHE.lock().expect("pattern cache");
    cache.entry(pattern.to_string()).or_insert_with(|| Regex::new(pattern).expect("guard pattern")).clone()
}

/// The string or char literal starting at `i`, if one starts there, as
/// (content start, content end, literal end), ends exclusive.
fn literal_end(b: &[u8], i: usize) -> Option<(usize, usize, usize)> {
    let ident_before = i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
    let mut j = i;
    if !ident_before && b.get(j) == Some(&b'b') && matches!(b.get(j + 1), Some(b'r' | b'"')) {
        j += 1;
    }
    if !ident_before && b.get(j) == Some(&b'r') {
        let mut k = j + 1;
        while b.get(k) == Some(&b'#') {
            k += 1;
        }
        if b.get(k) == Some(&b'"') {
            let hashes = k - j - 1;
            let start = k + 1;
            let mut e = start;
            while e < b.len() {
                if b[e] == b'"' && b[e + 1..].iter().take(hashes).filter(|c| **c == b'#').count() == hashes {
                    return Some((start, e, e + 1 + hashes));
                }
                e += 1;
            }
            return Some((start, b.len(), b.len()));
        }
    }
    if b.get(j) == Some(&b'"') {
        let start = j + 1;
        let mut e = start;
        while e < b.len() && b[e] != b'"' {
            e += if b[e] == b'\\' { 2 } else { 1 };
        }
        let e = e.min(b.len());
        return Some((start, e, (e + 1).min(b.len())));
    }
    if b[i] == b'\'' {
        // A char literal: an escape, or one (maybe multi-byte) char, then a
        // quote. Anything else is a lifetime.
        if b.get(i + 1) == Some(&b'\\') {
            let close = b[i + 2..].iter().take(12).position(|c| *c == b'\'')?;
            let e = i + 2 + close;
            return Some((i + 1, e, e + 1));
        }
        let lead = *b.get(i + 1)?;
        let len = match lead {
            0x00..=0x7f => 1,
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            _ => 4,
        };
        if b.get(i + 1 + len) == Some(&b'\'') {
            return Some((i + 1, i + 1 + len, i + 2 + len));
        }
    }
    None
}

/// `text` with comments blanked, newlines kept.
fn strip_comments(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = b.to_vec();
    let mut i = 0;
    while i < b.len() {
        if let Some((_, _, end)) = literal_end(b, i) {
            i = end;
            continue;
        }
        if b[i..].starts_with(b"//") {
            while i < b.len() && b[i] != b'\n' {
                out[i] = b' ';
                i += 1;
            }
            continue;
        }
        if b[i..].starts_with(b"/*") {
            let mut depth = 0;
            while i < b.len() {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    out[i] = b' ';
                    out[i + 1] = b' ';
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    out[i] = b' ';
                    out[i + 1] = b' ';
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if b[i] != b'\n' {
                        out[i] = b' ';
                    }
                    i += 1;
                }
            }
            continue;
        }
        i += 1;
    }
    String::from_utf8(out).expect("whole chars blanked")
}

/// `text` (comments already blanked) with each inline `#[cfg(test)] mod`
/// block blanked, and the names of the out-of-line test modules it declares.
fn strip_test_modules(text: &str) -> (String, Vec<String>) {
    let head = re(r"#\[cfg\(test\)\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*([{;])");
    let b = text.as_bytes();
    let mut out = b.to_vec();
    let mut outlined = Vec::new();
    for m in head.captures_iter(text) {
        let whole = m.get(0).expect("match");
        if &m[2] == ";" {
            outlined.push(m[1].to_string());
            continue;
        }
        let mut depth = 0usize;
        let mut i = whole.end() - 1;
        while i < b.len() {
            if let Some((_, _, end)) = literal_end(b, i) {
                i = end;
                continue;
            }
            match b[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        for c in &mut out[whole.start()..=i.min(b.len() - 1)] {
            if *c != b'\n' {
                *c = b' ';
            }
        }
    }
    (String::from_utf8(out).expect("whole chars blanked"), outlined)
}

/// The SQL stream of `text`: see [`Source`].
fn sql_stream(text: &str) -> (String, Vec<usize>) {
    let b = text.as_bytes();
    let mut sql = Vec::new();
    let mut at = Vec::new();
    let mut statement_ended = false;
    let mut i = 0;
    while i < b.len() {
        let raw = b[i] == b'r' || (b[i] == b'b' && b.get(i + 1) == Some(&b'r'));
        if let Some((start, end, after)) = literal_end(b, i).filter(|_| b[i] != b'\'') {
            sql.push(if statement_ended { b'\0' } else { b' ' });
            at.push(i);
            let mut k = start;
            while k < end {
                if !raw && b[k] == b'\\' && k + 1 < end {
                    // An escape: a line continuation reads as a space.
                    sql.push(if b[k + 1] == b'\n' { b' ' } else { b[k + 1] });
                    at.push(k);
                    k += 2;
                    continue;
                }
                sql.push(b[k]);
                at.push(k);
                k += 1;
            }
            statement_ended = false;
            i = after;
            continue;
        }
        if let Some((_, _, after)) = literal_end(b, i) {
            i = after;
            continue;
        }
        if matches!(b[i], b';' | b'{' | b'}') {
            statement_ended = true;
        }
        i += 1;
    }
    sql.push(b'\0');
    at.push(b.len());
    // An escape can put a lone byte of a multi-byte char in; read lossily.
    (String::from_utf8_lossy(&sql).into_owned(), at)
}

/// Every non-test `.rs` file under `src/`.
fn sources() -> Vec<Source> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    let mut dirs = vec![root.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read src") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    let mut read: Vec<(PathBuf, String, Vec<String>)> = Vec::new();
    let mut excluded: Vec<PathBuf> = Vec::new();
    for path in files {
        let raw = std::fs::read_to_string(&path).expect("read source");
        let (text, outlined) = strip_test_modules(&strip_comments(&raw));
        // An out-of-line test module `x` is `x.rs` or `x/` beside a `mod.rs`
        // or `lib.rs`, and under the file's own directory otherwise.
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
        let base = if matches!(stem, "mod" | "lib" | "main") {
            path.parent().expect("parent").to_path_buf()
        } else {
            path.with_extension("")
        };
        for name in &outlined {
            excluded.push(base.join(format!("{name}.rs")));
            excluded.push(base.join(name));
        }
        read.push((path, text, outlined));
    }
    read.into_iter()
        .filter(|(path, _, _)| !excluded.iter().any(|x| path == x || path.starts_with(x)))
        .map(|(path, text, _)| {
            let rel = path.strip_prefix(&root).expect("under src").to_string_lossy().replace('\\', "/");
            let (sql, at) = sql_stream(&text);
            Source { rel, text, sql, at }
        })
        .collect()
}

/// The innermost parenthesised group around `i` in `sql`, as
/// `(start, end)` with `sql[start] == '('` and `sql[end] == ')'`, or the
/// SQL statement around it, from its opening `\0` to its closing one.
fn enclosing(sql: &[u8], i: usize) -> (usize, usize) {
    let mut depth = 0usize;
    let mut j = i;
    while j > 0 {
        j -= 1;
        match sql[j] {
            b')' => depth += 1,
            b'(' if depth == 0 => return (j, close_of(sql, j)),
            b'(' => depth -= 1,
            b'\0' => return (j, statement_end(sql, i)),
            _ => {}
        }
    }
    (0, statement_end(sql, i))
}

fn statement_end(sql: &[u8], i: usize) -> usize {
    sql[i..].iter().position(|c| *c == b'\0').map_or(sql.len(), |p| i + p)
}

/// The `)` closing the `(` at `open`, or the statement's end.
fn close_of(sql: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    for (k, c) in sql.iter().enumerate().skip(open) {
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return k;
                }
            }
            b'\0' => return k,
            _ => {}
        }
    }
    sql.len()
}

/// The group `(start, end)` (see [`enclosing`]) with every nested group
/// blanked, as text of the same length.
fn top_level(sql: &str, (start, end): (usize, usize)) -> String {
    let b = sql.as_bytes();
    let paren = b.get(start) == Some(&b'(');
    let end = if paren { (end + 1).min(b.len()) } else { end };
    let base = usize::from(paren);
    let mut depth = 0usize;
    let mut out = Vec::with_capacity(end - start);
    for &c in &b[start..end] {
        if c == b'(' {
            depth += 1;
        }
        out.push(if depth > base && c != b'\n' { b' ' } else { c });
        if c == b')' {
            depth = depth.saturating_sub(1);
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn split_part_findings(s: &Source) -> Vec<Finding> {
    re(r"(?i)\bsplit_part\s*\(").find_iter(&s.sql).map(|m| s.finding("split_part", m.start())).collect()
}

/// `LIKE` or `ILIKE` on a column named for a URI, DID or subject, or with a
/// pattern that starts like one.
fn uri_like_findings(s: &Source) -> Vec<Finding> {
    let like = re(r"(?i)([\w.]+)\s*\)?\s+(?:NOT\s+)?I?LIKE\s+(\S+)");
    let named = re(r"(?i)(uri|did|subject|raw)$");
    let pattern = re(r"(?i)^'(at:|did:|%/)");
    like.captures_iter(&s.sql)
        .filter(|c| named.is_match(&c[1]) || pattern.is_match(&c[2]))
        .map(|c| s.finding("uri_like", c.get(0).expect("match").start()))
        .collect()
}

/// A dropped text column named on a compact table: qualified by the
/// table's name or alias within the alias's scope, unqualified where the
/// table is its statement's only source, or in an `INSERT` column list.
fn text_column_findings(s: &Source) -> Vec<Finding> {
    let b = s.sql.as_bytes();
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let dropped = DROPPED.join("|");
    let sources = re(r"(?i)\b(?:FROM|JOIN|UPDATE|INTO)\s+[\w{]");
    let comma_join = re(r"(?i)\bFROM\s+\w+(?:\s+\w+)?\s*,");
    let bare = re(&format!(r"(?i)(?:^|[^\w.:'{{$])({})\b", DROPPED_BARE.join("|")));
    let as_before = re(r"(?i)\bAS\s*$");
    for table in COMPACT {
        let keeps = |col: &str| *table == "post_search" && matches!(col.to_ascii_lowercase().as_str(), "text" | "mention_dids");
        let binding = re(&format!(r"(?i)\b(?:FROM|JOIN|UPDATE|INTO)\s+{table}\b(?:\s+(?:AS\s+)?(\w+))?"));
        for m in binding.captures_iter(&s.sql) {
            let at = m.get(0).expect("match").start();
            let alias = m.get(1).map(|a| a.as_str()).filter(|a| !NOT_ALIASES.contains(&a.to_ascii_lowercase().as_str()));
            let scope = enclosing(b, at);
            let region_end = if b.get(scope.0) == Some(&b'(') { scope.1 + 1 } else { scope.1 }.min(b.len());
            let region = &s.sql[scope.0..region_end];
            for name in std::iter::once(*table).chain(alias) {
                let qualified = re(&format!(r"(?:^|[^\w.]){}\.({dropped})\b", regex::escape(name)));
                let rebinds = re(&format!(r"(?i)\b(?:FROM|JOIN)\s+\w+\s+(?:AS\s+)?{}\b", regex::escape(name)));
                for q in qualified.captures_iter(region) {
                    let col = q.get(1).expect("column");
                    if keeps(col.as_str()) {
                        continue;
                    }
                    let pos = scope.0 + col.start();
                    // Shadowed when a nested group between here and the
                    // binding's scope binds the same name again.
                    let mut group = enclosing(b, pos);
                    let mut shadowed = false;
                    while group.0 > scope.0 && b.get(group.0) == Some(&b'(') {
                        if rebinds.is_match(&top_level(&s.sql, group)) {
                            shadowed = true;
                            break;
                        }
                        group = enclosing(b, group.0);
                    }
                    if !shadowed && seen.insert(pos) {
                        out.push(s.finding("text_column", pos));
                    }
                }
            }
            let top = top_level(&s.sql, scope);
            if sources.find_iter(&top).count() == 1 && !comma_join.is_match(&top) {
                for c in bare.captures_iter(&top) {
                    let col = c.get(1).expect("column");
                    let after = top[col.end()..].trim_start();
                    if keeps(col.as_str()) || after.starts_with('(') || as_before.is_match(&top[..col.start()]) {
                        continue;
                    }
                    let pos = scope.0 + col.start();
                    if seen.insert(pos) {
                        out.push(s.finding("text_column", pos));
                    }
                }
            }
        }
    }
    let insert = re(r"(?i)\bINSERT\s+INTO\s+(\w+)\s*\(([^)]*)\)");
    for c in insert.captures_iter(&s.sql) {
        let table = &c[1];
        if !COMPACT.contains(&table) {
            continue;
        }
        for col in c[2].split(',').map(|x| x.trim().to_ascii_lowercase()) {
            let kept = table == "post_search" && matches!(col.as_str(), "text" | "mention_dids");
            if DROPPED.contains(&col.as_str()) && !kept {
                out.push(s.finding("text_column", c.get(0).expect("match").start()));
            }
        }
    }
    out
}

/// An `EXISTS` over a pair table that reads two `dids` rows at its own
/// level: correlated and inside an `OR`, it can be planned as a hashed
/// subplan over the whole table. Each id is looked up by a scalar subquery
/// instead.
fn pair_check_findings(s: &Source) -> Vec<Finding> {
    let b = s.sql.as_bytes();
    let pair = re(&format!(r"(?i)\bFROM\s+(?:{PAIR_TABLES})\b"));
    let dids = re(r"(?i)(?:\bJOIN|\bFROM|,)\s+dids\b");
    re(r"(?i)\bEXISTS\s*\(")
        .find_iter(&s.sql)
        .filter_map(|m| {
            let open = m.end() - 1;
            let top = top_level(&s.sql, (open, close_of(b, open)));
            (pair.is_match(&top) && dids.find_iter(&top).count() >= 2).then(|| s.finding("pair_check", m.start()))
        })
        .collect()
}

/// `at_uri` or `ref_text` in a derived table or a `UNION` arm under an
/// `ORDER BY … LIMIT`: it runs for every candidate row, not for the page.
fn per_row_uri_findings(s: &Source) -> Vec<Finding> {
    let b = s.sql.as_bytes();
    let union = re(r"(?i)\bUNION\b");
    let order_limit = re(r"(?is)\bORDER\s+BY\b.*\bLIMIT\b");
    let limit = re(r"(?i)\bLIMIT\b");
    let subquery = re(r"(?i)^\(\s*SELECT\b");
    let derived = re(r"(?i)(?:\bFROM|\bJOIN|\bUNION(?:\s+ALL)?|\(|,|\bLATERAL)\s*$");
    let mut out = Vec::new();
    for m in re(r"(?i)\b(?:at_uri|ref_text)\s*\(").find_iter(&s.sql) {
        let mut group = enclosing(b, m.start());
        let mut crossed = false;
        loop {
            let top = top_level(&s.sql, group);
            let after_union = union.find_iter(&top).last().map(|u| &top[u.end()..]);
            if after_union.is_some_and(|rest| order_limit.is_match(rest)) || (crossed && order_limit.is_match(&top)) {
                out.push(s.finding("per_row_uri", m.start()));
                break;
            }
            if limit.is_match(&top) || b.get(group.0) != Some(&b'(') {
                break;
            }
            let head_end = floor(&s.sql, (group.0 + 400).min(s.sql.len()));
            if subquery.is_match(&s.sql[group.0..head_end]) {
                let from = floor(&s.sql, group.0.saturating_sub(40));
                if !derived.is_match(&s.sql[from..group.0]) {
                    break;
                }
                crossed = true;
            }
            let outer = enclosing(b, group.0);
            if outer == group {
                break;
            }
            group = outer;
        }
    }
    out
}

/// Runs every check and compares the findings with [`ALLOWED`].
fn violations(checks: &[&'static str]) -> Vec<String> {
    let mut found: BTreeMap<(&'static str, String), Vec<Finding>> = BTreeMap::new();
    for s in sources() {
        let all = [
            split_part_findings(&s),
            uri_like_findings(&s),
            text_column_findings(&s),
            pair_check_findings(&s),
            per_row_uri_findings(&s),
        ];
        for f in all.into_iter().flatten().filter(|f| checks.contains(&f.check)) {
            found.entry((f.check, f.file.clone())).or_default().push(f);
        }
    }
    let mut problems = Vec::new();
    for ((check, file), findings) in &found {
        let allowed = ALLOWED.iter().find(|(c, f, _, _)| c == check && f == file);
        let expected = allowed.map_or(0, |a| a.2);
        if findings.len() != expected {
            let mut text = format!("{check}: {file} has {} finding(s), {expected} allowed", findings.len());
            for f in findings {
                text.push_str(&format!("\n  {}:{}: {}", f.file, f.line, f.near));
            }
            problems.push(text);
        }
    }
    for (check, file, count, why) in ALLOWED.iter().filter(|a| checks.contains(&a.0)) {
        if !found.contains_key(&(*check, (*file).to_string())) {
            problems.push(format!("{check}: {file} is allowed {count} ({why}) but has none; drop the entry"));
        }
    }
    problems
}

#[test]
fn public_sql_reads_ids_and_keys_not_uri_text() {
    let problems = violations(&["split_part", "uri_like", "text_column"]);
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}

#[test]
fn pair_checks_look_up_each_id_and_uris_are_built_per_page() {
    let problems = violations(&["pair_check", "per_row_uri"]);
    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}

#[test]
fn every_allowance_has_a_reason() {
    for (check, file, count, why) in ALLOWED {
        assert!(*count > 0 && !why.is_empty(), "{check} {file}");
    }
}

/// The checks find what they are for, so a pass means something: each
/// shape below is one the guards must refuse, and its fixed form passes.
#[test]
fn the_checks_find_their_shapes() {
    let source = |code: &str| {
        let (text, _) = strip_test_modules(&strip_comments(code));
        let (sql, at) = sql_stream(&text);
        Source { rel: "x.rs".into(), text, sql, at }
    };
    let count = |code: &str, f: fn(&Source) -> Vec<Finding>| f(&source(code)).len();
    // Split over literals and a `format!`, the SQL still reads as one.
    let split = r#"let q = format!("SELECT p.author_did FROM posts p \
        WHERE {} ", "x"); let r = "SELECT split_part(" ;"#;
    assert_eq!(count(split, text_column_findings), 1);
    assert_eq!(count(split, split_part_findings), 1);
    assert_eq!(count(r#"let q = "SELECT 1 FROM posts p " ; // p.uri"#, text_column_findings), 0, "comments are not SQL");
    assert_eq!(count(r#"const Q: &str = "DELETE FROM likes WHERE uri = $1";"#, text_column_findings), 1);
    assert_eq!(count(r#"const Q: &str = "SELECT at_uri(author, 2::smallint, rkey) AS uri FROM likes";"#, text_column_findings), 0);
    assert_eq!(count(r#"const Q: &str = "INSERT INTO follows (author, rkey, subject_did) VALUES (1, 2, 3)";"#, text_column_findings), 1);
    // A nested group that binds the alias again is that table's.
    let shadow = r#"const Q: &str = "SELECT 1 FROM posts p WHERE EXISTS (SELECT 1 FROM uri_rows p WHERE p.uri = $1)";"#;
    assert_eq!(count(shadow, text_column_findings), 0);
    assert_eq!(count(r#"const Q: &str = "SELECT 1 FROM thread_mutes t WHERE t.root_uri LIKE 'at://%'";"#, uri_like_findings), 1);
    assert_eq!(count(r#"const Q: &str = "SELECT 1 FROM actors a WHERE a.handle ILIKE $2";"#, uri_like_findings), 0);
    assert_eq!(count("#[cfg(test)]\nmod tests {\n const Q: &str = \"SELECT split_part(\"; fn f() { let c = '{'; }\n}\n", split_part_findings), 0);
    let joined = r#"const Q: &str = "x OR EXISTS (SELECT 1 FROM follows f JOIN dids a ON a.id = f.author JOIN dids b ON b.id = f.subject WHERE a.did = $1 AND b.did = n.author_did)";"#;
    let listed = r#"const Q: &str = "NOT EXISTS (SELECT 1 FROM blocks b, dids x, dids y WHERE x.did = $1 AND y.did = n.author_did AND b.author = x.id AND b.subject = y.id)";"#;
    let fixed = r#"const Q: &str = "EXISTS (SELECT 1 FROM follows f WHERE f.author = (SELECT id FROM dids WHERE did = $1) AND f.subject = (SELECT id FROM dids WHERE did = n.author_did))";"#;
    assert_eq!(count(joined, pair_check_findings), 1);
    assert_eq!(count(listed, pair_check_findings), 1);
    assert_eq!(count(fixed, pair_check_findings), 0);
    let per_row = r#"const Q: &str = "SELECT t.uri FROM (SELECT at_uri(p.author, 1::smallint, p.rkey) AS uri, p.created_at FROM posts p \
        UNION ALL SELECT at_uri(r.author, 3::smallint, r.rkey), r.created_at FROM reposts r) t ORDER BY t.created_at DESC LIMIT $1";"#;
    let arms = r#"const Q: &str = "SELECT at_uri(p.author, 1::smallint, p.rkey), p.created_at FROM posts p UNION ALL SELECT at_uri(r.author, 3::smallint, r.rkey), r.created_at FROM reposts r ORDER BY 2 DESC LIMIT $1";"#;
    let per_page = r#"const Q: &str = "SELECT at_uri(z.author, 1::smallint, z.rkey) FROM (SELECT p.author, p.rkey, p.created_at FROM posts p \
        UNION ALL SELECT r.author, r.rkey, r.created_at FROM reposts r ORDER BY 3 DESC LIMIT $1) z";"#;
    assert_eq!(count(per_row, per_row_uri_findings), 2);
    assert_eq!(count(arms, per_row_uri_findings), 2);
    assert_eq!(count(per_page, per_row_uri_findings), 0);
}
