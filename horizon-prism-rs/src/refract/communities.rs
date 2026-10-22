//! The communities list (refract design §3): matching a host to its home
//! community, and the entry hash a live change is diffed by (§3.2).

use crate::config::{LabelSource, RefractCommunity};
use crate::constellation::matcher::host_matches;

/// The communities as the live settings hold them now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Communities {
    list: Vec<RefractCommunity>,
}

impl Communities {
    #[must_use]
    pub fn new(list: Vec<RefractCommunity>) -> Self {
        Self { list }
    }

    /// Every entry, in list order.
    #[must_use]
    pub fn all(&self) -> &[RefractCommunity] {
        &self.list
    }

    /// The home community of a PDS host: the first **enabled** entry whose
    /// patterns match it (§3.1, first match wins in list order).
    #[must_use]
    pub fn home(&self, host: &str) -> Option<&RefractCommunity> {
        self.list.iter().filter(|c| c.enabled).find(|c| c.pds_patterns.iter().any(|p| host_matches(host, p)))
    }

    /// The entry named `name`, enabled or not.
    #[must_use]
    pub fn by_name(&self, name: &str) -> Option<&RefractCommunity> {
        self.list.iter().find(|c| c.name == name)
    }

    /// The labeler of the community named `name`, when it has one.
    #[must_use]
    pub fn labeler_of(&self, name: &str) -> Option<&str> {
        self.by_name(name).and_then(|c| c.labeler_did.as_deref())
    }

    /// The enabled communities whose labeler is read directly (§4.5).
    pub fn labeler_sourced(&self) -> impl Iterator<Item = &RefractCommunity> {
        self.list.iter().filter(|c| c.enabled && c.labeler_did.is_some() && c.label_source == Some(LabelSource::Labeler))
    }
}

/// The entry hash (§3.2): the sorted patterns, tier, AppView network,
/// labeler, label source, sorted gone labels and `enabled`. The name is
/// not in it, so a rename keeps the hash.
#[must_use]
pub fn entry_hash(c: &RefractCommunity) -> String {
    let mut patterns: Vec<String> = c.pds_patterns.iter().map(|p| p.to_ascii_lowercase()).collect();
    patterns.sort();
    patterns.dedup();
    let mut gone = c.gone_labels.clone();
    gone.sort();
    gone.dedup();
    let canonical = serde_json::json!({
        "pds_patterns": patterns,
        "tier": c.tier,
        "appview_network": c.appview_network,
        "labeler_did": c.labeler_did,
        "label_source": c.label_source.map(LabelSource::as_str),
        "gone_labels": gone,
        "enabled": c.enabled,
    });
    blake3::hash(canonical.to_string().as_bytes()).to_hex()[..32].to_string()
}

/// Whether two entries read labels the same way: same labeler, source and
/// AppView network. Otherwise a change drops the old labels (§3.2).
#[must_use]
pub fn same_label_reader(a: &RefractCommunity, b: &RefractCommunity) -> bool {
    a.labeler_did == b.labeler_did && a.label_source == b.label_source && a.appview_network == b.appview_network
}

#[cfg(test)]
mod tests {
    use super::*;

    fn north() -> RefractCommunity {
        RefractCommunity {
            name: "north".into(),
            pds_patterns: vec!["*.north.example".into(), "north.example".into()],
            tier: 2,
            appview_network: None,
            labeler_did: Some("did:plc:north".into()),
            label_source: Some(LabelSource::Labeler),
            gone_labels: vec!["!takedown".into()],
            enabled: true,
        }
    }

    #[test]
    fn first_enabled_match_wins() {
        let mut catchall = north();
        catchall.name = "catchall".into();
        catchall.pds_patterns = vec!["*.north.example".into()];
        catchall.enabled = false;
        let c = Communities::new(vec![catchall, north(), RefractCommunity::example()]);
        assert_eq!(c.home("pds.north.example").map(|c| c.name.as_str()), Some("north"), "a disabled entry never matches");
        assert_eq!(c.home("community-a.example").map(|c| c.name.as_str()), Some("community-a"));
        assert!(c.home("pds.other.example").is_none());
        assert_eq!(c.labeler_sourced().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["north"]);
        assert_eq!(c.labeler_of("community-a"), Some("did:plc:cccccccccccccccccccccccc"));
    }

    /// §3.2: the name is not hashed; order of patterns and labels is not;
    /// every other field is.
    #[test]
    fn entry_hash_ignores_name_and_order() {
        let a = north();
        let mut renamed = north();
        renamed.name = "renamed".into();
        renamed.pds_patterns.reverse();
        assert_eq!(entry_hash(&a), entry_hash(&renamed));
        for change in [
            |c: &mut RefractCommunity| c.tier = 1,
            |c: &mut RefractCommunity| c.enabled = false,
            |c: &mut RefractCommunity| c.gone_labels.push("spam".into()),
            |c: &mut RefractCommunity| c.pds_patterns.pop().map(drop).unwrap_or_default(),
            |c: &mut RefractCommunity| c.label_source = Some(LabelSource::Appview),
            |c: &mut RefractCommunity| c.labeler_did = None,
        ] {
            let mut b = north();
            change(&mut b);
            assert_ne!(entry_hash(&a), entry_hash(&b), "{b:?}");
        }
        let mut b = north();
        b.gone_labels.push("spam".into());
        assert!(same_label_reader(&a, &b));
        b.labeler_did = Some("did:plc:other".into());
        assert!(!same_label_reader(&a, &b));
    }
}
