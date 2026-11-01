//! The admin pages' fonts, compiled in and served from
//! `/admin/static/fonts/{file}`, so no page loads a font from another site.
//!
//! Every family is under the SIL Open Font License 1.1. Each licence text is
//! compiled in beside its fonts and served with them, and a `const`
//! assertion refuses to build with any other licence text. Every file is
//! the upstream release's own, unmodified; [`FONT_SOURCES`] names where each
//! came from and its blake3, which the tests recompute. File names carry the
//! upstream version, so an update changes the URL and the year-long cache
//! never serves a stale copy.

use axum::extract::Path;
use axum::extract::rejection::PathRejection;
use axum::http::HeaderValue;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::response::{IntoResponse, Response};

/// What every licence text must contain.
pub const OFL_TITLE: &str = "SIL OPEN FONT LICENSE Version 1.1";

/// `needle` occurs in `hay`; usable in a `const` assertion.
const fn contains(hay: &str, needle: &str) -> bool {
    let (h, n) = (hay.as_bytes(), needle.as_bytes());
    let mut i = 0;
    while i + n.len() <= h.len() {
        let mut j = 0;
        while j < n.len() && h[i + j] == n[j] {
            j += 1;
        }
        if j == n.len() {
            return true;
        }
        i += 1;
    }
    false
}

const OFL_ATKINSON: &str = include_str!("static/fonts/OFL-atkinson-hyperlegible-1.006.txt");
const OFL_JETBRAINS: &str = include_str!("static/fonts/OFL-jetbrains-mono-2.304.txt");
const OFL_PLEX_SANS: &str = include_str!("static/fonts/OFL-ibm-plex-sans-1.1.0.txt");
const OFL_PLEX_MONO: &str = include_str!("static/fonts/OFL-ibm-plex-mono-2.5.0.txt");

const _: () = assert!(contains(OFL_ATKINSON, OFL_TITLE), "the Atkinson Hyperlegible licence is not the SIL OFL 1.1");
const _: () = assert!(contains(OFL_JETBRAINS, OFL_TITLE), "the JetBrains Mono licence is not the SIL OFL 1.1");
const _: () = assert!(contains(OFL_PLEX_SANS, OFL_TITLE), "the IBM Plex Sans licence is not the SIL OFL 1.1");
const _: () = assert!(contains(OFL_PLEX_MONO, OFL_TITLE), "the IBM Plex Mono licence is not the SIL OFL 1.1");

/// A WOFF2 font's content type.
pub const WOFF2: &str = "font/woff2";
/// A licence text's content type.
pub const TEXT: &str = "text/plain; charset=utf-8";

/// Every file served under `/admin/static/fonts/`: its name, content type
/// and bytes.
pub const FONTS: &[(&str, &str, &[u8])] = &[
    ("atkinson-hyperlegible-1.006-400.woff2", WOFF2, include_bytes!("static/fonts/atkinson-hyperlegible-1.006-400.woff2")),
    ("atkinson-hyperlegible-1.006-400-italic.woff2", WOFF2, include_bytes!("static/fonts/atkinson-hyperlegible-1.006-400-italic.woff2")),
    ("atkinson-hyperlegible-1.006-700.woff2", WOFF2, include_bytes!("static/fonts/atkinson-hyperlegible-1.006-700.woff2")),
    ("OFL-atkinson-hyperlegible-1.006.txt", TEXT, OFL_ATKINSON.as_bytes()),
    ("jetbrains-mono-2.304-400.woff2", WOFF2, include_bytes!("static/fonts/jetbrains-mono-2.304-400.woff2")),
    ("jetbrains-mono-2.304-600.woff2", WOFF2, include_bytes!("static/fonts/jetbrains-mono-2.304-600.woff2")),
    ("OFL-jetbrains-mono-2.304.txt", TEXT, OFL_JETBRAINS.as_bytes()),
    ("ibm-plex-sans-1.1.0-400.woff2", WOFF2, include_bytes!("static/fonts/ibm-plex-sans-1.1.0-400.woff2")),
    ("ibm-plex-sans-1.1.0-600.woff2", WOFF2, include_bytes!("static/fonts/ibm-plex-sans-1.1.0-600.woff2")),
    ("ibm-plex-sans-1.1.0-700.woff2", WOFF2, include_bytes!("static/fonts/ibm-plex-sans-1.1.0-700.woff2")),
    ("OFL-ibm-plex-sans-1.1.0.txt", TEXT, OFL_PLEX_SANS.as_bytes()),
    ("ibm-plex-mono-2.5.0-400.woff2", WOFF2, include_bytes!("static/fonts/ibm-plex-mono-2.5.0-400.woff2")),
    ("ibm-plex-mono-2.5.0-600.woff2", WOFF2, include_bytes!("static/fonts/ibm-plex-mono-2.5.0-600.woff2")),
    ("OFL-ibm-plex-mono-2.5.0.txt", TEXT, OFL_PLEX_MONO.as_bytes()),
];

/// Where a file in [`FONTS`] came from.
#[derive(Debug, Clone, Copy)]
pub struct FontSource {
    /// Its name in [`FONTS`].
    pub file: &'static str,
    /// The upstream download: the file itself, or the release archive.
    pub url: &'static str,
    /// Its path in the upstream repository or archive.
    pub path: &'static str,
    /// The release tag or commit it was taken from.
    pub pin: &'static str,
    /// The blake3 of its bytes, hex.
    pub blake3: &'static str,
}

const ATKINSON_COMMIT: &str = "1cb311624b2ddf88e9e37873999d165a8cd28b46";
const ATKINSON_RAW: &str = "https://raw.githubusercontent.com/googlefonts/atkinson-hyperlegible/1cb311624b2ddf88e9e37873999d165a8cd28b46";
const JETBRAINS_ZIP: &str = "https://github.com/JetBrains/JetBrainsMono/releases/download/v2.304/JetBrainsMono-2.304.zip";
const PLEX_SANS_ZIP: &str = "https://github.com/IBM/plex/releases/download/%40ibm%2Fplex-sans%401.1.0/ibm-plex-sans.zip";
const PLEX_MONO_ZIP: &str = "https://github.com/IBM/plex/releases/download/%40ibm%2Fplex-mono%402.5.0/ibm-plex-mono.zip";

/// Every file's upstream, one row per entry of [`FONTS`].
pub const FONT_SOURCES: &[FontSource] = &[
    FontSource {
        file: "atkinson-hyperlegible-1.006-400.woff2",
        url: ATKINSON_RAW,
        path: "fonts/webfonts/AtkinsonHyperlegible-Regular.woff2",
        pin: ATKINSON_COMMIT,
        blake3: "f9833bc7333cbe6cc9f68ac42e797cb467645f382d18bb82a83e9deed1ef8165",
    },
    FontSource {
        file: "atkinson-hyperlegible-1.006-400-italic.woff2",
        url: ATKINSON_RAW,
        path: "fonts/webfonts/AtkinsonHyperlegible-Italic.woff2",
        pin: ATKINSON_COMMIT,
        blake3: "2cef501319940926ca65b437afb57a5ed10a465a711e8a137e24a6df9bd025f7",
    },
    FontSource {
        file: "atkinson-hyperlegible-1.006-700.woff2",
        url: ATKINSON_RAW,
        path: "fonts/webfonts/AtkinsonHyperlegible-Bold.woff2",
        pin: ATKINSON_COMMIT,
        blake3: "33d20aa6f953e5526a7850176c8ca1bf0213867584553ac3ff60a9aa5a34fe4e",
    },
    FontSource {
        file: "OFL-atkinson-hyperlegible-1.006.txt",
        url: ATKINSON_RAW,
        path: "OFL.txt",
        pin: ATKINSON_COMMIT,
        blake3: "26775333bda09e6b85f94bd9b7e45a4cec45eb2bbbfb1f616a85d1ddcf8faac3",
    },
    FontSource {
        file: "jetbrains-mono-2.304-400.woff2",
        url: JETBRAINS_ZIP,
        path: "fonts/webfonts/JetBrainsMono-Regular.woff2",
        pin: "v2.304",
        blake3: "81f5c64e66c5883553a4554cec01397190bddf4195a8bab0f7b59b4079959760",
    },
    FontSource {
        file: "jetbrains-mono-2.304-600.woff2",
        url: JETBRAINS_ZIP,
        path: "fonts/webfonts/JetBrainsMono-SemiBold.woff2",
        pin: "v2.304",
        blake3: "860fb4e8b8fe8751cff2eb5fed02c8afebd5940ed96b2ea6a280fabb727749e3",
    },
    FontSource {
        file: "OFL-jetbrains-mono-2.304.txt",
        url: JETBRAINS_ZIP,
        path: "OFL.txt",
        pin: "v2.304",
        blake3: "4ad21e838c2a993c5cabc052011205bdda92a83189805e75d163ef27bebc2d5e",
    },
    FontSource {
        file: "ibm-plex-sans-1.1.0-400.woff2",
        url: PLEX_SANS_ZIP,
        path: "ibm-plex-sans/fonts/split/woff2/IBMPlexSans-Regular-Latin1.woff2",
        pin: "@ibm/plex-sans@1.1.0",
        blake3: "8b497026c598b5097fa15fd37a481c245452a03df8a1814dd7aecae83fa662ea",
    },
    FontSource {
        file: "ibm-plex-sans-1.1.0-600.woff2",
        url: PLEX_SANS_ZIP,
        path: "ibm-plex-sans/fonts/split/woff2/IBMPlexSans-SemiBold-Latin1.woff2",
        pin: "@ibm/plex-sans@1.1.0",
        blake3: "0eb202d92834450ee9e46599ba7aa0731bd58be6dd78027d724e628b1617f5a1",
    },
    FontSource {
        file: "ibm-plex-sans-1.1.0-700.woff2",
        url: PLEX_SANS_ZIP,
        path: "ibm-plex-sans/fonts/split/woff2/IBMPlexSans-Bold-Latin1.woff2",
        pin: "@ibm/plex-sans@1.1.0",
        blake3: "c22084a0e6b46830ceb7ceea4a2869853da55ee04432a140a87ffbffd3fa9e86",
    },
    FontSource {
        file: "OFL-ibm-plex-sans-1.1.0.txt",
        url: PLEX_SANS_ZIP,
        path: "ibm-plex-sans/LICENSE.txt",
        pin: "@ibm/plex-sans@1.1.0",
        blake3: "1a5700cba4852d89c93831cb7f708916054f831ccc0aac926fb5043a9a13c537",
    },
    FontSource {
        file: "ibm-plex-mono-2.5.0-400.woff2",
        url: PLEX_MONO_ZIP,
        path: "ibm-plex-mono/fonts/split/woff2/IBMPlexMono-Regular-Latin1.woff2",
        pin: "@ibm/plex-mono@2.5.0",
        blake3: "bfe41feab289e09a630e33a21a1ed5f63b645e149ba3cddc40c37bce632463ab",
    },
    FontSource {
        file: "ibm-plex-mono-2.5.0-600.woff2",
        url: PLEX_MONO_ZIP,
        path: "ibm-plex-mono/fonts/split/woff2/IBMPlexMono-SemiBold-Latin1.woff2",
        pin: "@ibm/plex-mono@2.5.0",
        blake3: "53ad9c414488c9e32d2af0f140e3743e54d6b9ca49fb62d13cc8a49dd2da8542",
    },
    FontSource {
        file: "OFL-ibm-plex-mono-2.5.0.txt",
        url: PLEX_MONO_ZIP,
        path: "ibm-plex-mono/LICENSE.txt",
        pin: "@ibm/plex-mono@2.5.0",
        blake3: "1a5700cba4852d89c93831cb7f708916054f831ccc0aac926fb5043a9a13c537",
    },
];

/// Each family's licence file and the copyright line it carries.
pub const LICENCES: &[(&str, &str)] = &[
    ("OFL-atkinson-hyperlegible-1.006.txt", "Copyright 2020 Braille Institute of America, Inc."),
    ("OFL-jetbrains-mono-2.304.txt", "Copyright 2020 The JetBrains Mono Project Authors (https://github.com/JetBrains/JetBrainsMono)"),
    ("OFL-ibm-plex-sans-1.1.0.txt", "Copyright © 2017 IBM Corp. with Reserved Font Name \"Plex\""),
    ("OFL-ibm-plex-mono-2.5.0.txt", "Copyright © 2017 IBM Corp. with Reserved Font Name \"Plex\""),
];

/// `GET /admin/static/fonts/{file}`: a font or a licence, cached for a
/// year; anything else, or a segment that is not text, is the plain 404
/// page.
pub async fn serve(file: Result<Path<String>, PathRejection>) -> Response {
    let Ok(Path(file)) = file else {
        return super::pages::not_found().await;
    };
    let Some(&(_, content_type, body)) = FONTS.iter().find(|(name, _, _)| *name == file) else {
        return super::pages::not_found().await;
    };
    (
        [
            (CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (CACHE_CONTROL, HeaderValue::from_static("public, max-age=31536000, immutable")),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::CSS;

    /// The total the files may reach: their measured size rounded up to the
    /// next 64 KiB.
    const CAP: usize = 384 * 1024;

    /// The weights the stylesheet uses, and no others.
    const BUNDLED: &[&str] = &[
        "atkinson-hyperlegible-1.006-400.woff2",
        "atkinson-hyperlegible-1.006-400-italic.woff2",
        "atkinson-hyperlegible-1.006-700.woff2",
        "jetbrains-mono-2.304-400.woff2",
        "jetbrains-mono-2.304-600.woff2",
        "ibm-plex-sans-1.1.0-400.woff2",
        "ibm-plex-sans-1.1.0-600.woff2",
        "ibm-plex-sans-1.1.0-700.woff2",
        "ibm-plex-mono-2.5.0-400.woff2",
        "ibm-plex-mono-2.5.0-600.woff2",
    ];

    fn file(name: &str) -> &'static [u8] {
        FONTS.iter().find(|(n, _, _)| *n == name).map(|(_, _, b)| *b).unwrap_or_else(|| panic!("{name} not in FONTS"))
    }

    /// Every licence is the OFL 1.1 with its family's copyright line, and
    /// every file is byte for byte the upstream one recorded for it.
    #[test]
    fn licences_and_sources_match_upstream() {
        assert!(contains(OFL_ATKINSON, OFL_TITLE) && contains(OFL_PLEX_MONO, OFL_TITLE));
        assert!(!contains(OFL_ATKINSON, "SIL OPEN FONT LICENSE Version 1.2"));
        for (name, copyright) in LICENCES {
            let text = std::str::from_utf8(file(name)).expect("utf-8");
            assert!(text.contains(OFL_TITLE), "{name}");
            assert!(text.lines().any(|l| l.trim_end() == *copyright), "{name}: no line {copyright:?}");
        }
        assert_eq!(FONT_SOURCES.len(), FONTS.len());
        for (name, _, bytes) in FONTS {
            let source = FONT_SOURCES.iter().find(|s| s.file == *name).unwrap_or_else(|| panic!("{name} has no source"));
            assert_eq!(blake3::hash(bytes).to_hex().as_str(), source.blake3, "{name}");
            assert!(source.url.starts_with("https://") && !source.path.is_empty() && !source.pin.is_empty(), "{name}");
        }
    }

    /// Each family's licence is served under a name carrying its version,
    /// the same version as its fonts.
    #[test]
    fn each_family_has_its_versioned_licence() {
        for family in ["atkinson-hyperlegible-1.006", "jetbrains-mono-2.304", "ibm-plex-sans-1.1.0", "ibm-plex-mono-2.5.0"] {
            let licence = format!("OFL-{family}.txt");
            assert!(FONTS.iter().any(|(n, t, _)| *n == licence && *t == TEXT), "{licence}");
            assert!(LICENCES.iter().any(|(n, _)| *n == licence), "{licence}");
            assert!(FONTS.iter().any(|(n, t, _)| n.starts_with(&format!("{family}-")) && *t == WOFF2), "{family}");
        }
    }

    /// Only the weights the pages use are bundled, and all of them are
    /// small enough.
    #[test]
    fn only_the_used_weights_under_the_cap() {
        let fonts: Vec<&str> = FONTS.iter().filter(|(_, t, _)| *t == WOFF2).map(|(n, _, _)| *n).collect();
        assert_eq!(fonts, BUNDLED);
        assert!(FONTS.iter().all(|(n, t, _)| (*t == WOFF2) == n.ends_with(".woff2") && (*t == TEXT) == n.ends_with(".txt")));
        let total: usize = FONTS.iter().map(|(_, _, b)| b.len()).sum();
        assert!(total <= CAP, "{total} bytes");
        for (name, _, bytes) in FONTS {
            if name.ends_with(".woff2") {
                assert_eq!(&bytes[..4], b"wOF2", "{name}");
            }
        }
    }

    /// The stylesheet names exactly the fonts served, and each once.
    #[test]
    fn the_stylesheet_uses_every_font() {
        let urls: Vec<&str> = CSS.split("url(/admin/static/fonts/").skip(1).map(|rest| &rest[..rest.find(')').expect("url end")]).collect();
        for url in &urls {
            assert!(FONTS.iter().any(|(n, t, _)| n == url && *t == WOFF2), "{url} is not served");
        }
        for name in BUNDLED {
            assert_eq!(urls.iter().filter(|u| *u == name).count(), 1, "{name}");
        }
        assert_eq!(CSS.matches("@font-face").count(), BUNDLED.len());
        assert_eq!(CSS.matches("font-display: swap").count(), BUNDLED.len());
    }

    /// Both policies let the pages load fonts from this origin only.
    #[test]
    fn the_policies_allow_own_fonts() {
        for csp in [crate::admin::CSP, crate::admin::CSP_LIVE] {
            assert!(csp.split("; ").any(|d| d == "font-src 'self'"), "{csp}");
            assert!(csp.starts_with("default-src 'none'; "), "{csp}");
        }
    }
}
