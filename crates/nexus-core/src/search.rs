use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SearchScope {
    Thread,
    Series,
    PatchItem,
}

impl SearchScope {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchScope::Thread => "thread",
            SearchScope::Series => "series",
            SearchScope::PatchItem => "patch_item",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "thread" => Some(SearchScope::Thread),
            "series" => Some(SearchScope::Series),
            "patch_item" => Some(SearchScope::PatchItem),
            _ => None,
        }
    }

    pub fn index_kind(self) -> MeiliIndexKind {
        match self {
            SearchScope::Thread => MeiliIndexKind::ThreadDocs,
            SearchScope::Series => MeiliIndexKind::PatchSeriesDocs,
            SearchScope::PatchItem => MeiliIndexKind::PatchItemDocs,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MeiliIndexKind {
    PatchItemDocs,
    PatchSeriesDocs,
    ThreadDocs,
}

impl MeiliIndexKind {
    pub fn uid(self) -> &'static str {
        match self {
            MeiliIndexKind::PatchItemDocs => "patch_item_docs",
            MeiliIndexKind::PatchSeriesDocs => "patch_series_docs",
            MeiliIndexKind::ThreadDocs => "thread_docs",
        }
    }

    pub fn spec(self) -> &'static MeiliIndexSpec {
        match self {
            MeiliIndexKind::PatchItemDocs => &PATCH_ITEM_DOCS_SPEC,
            MeiliIndexKind::PatchSeriesDocs => &PATCH_SERIES_DOCS_SPEC,
            MeiliIndexKind::ThreadDocs => &THREAD_DOCS_SPEC,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeiliIndexSpec {
    pub uid: &'static str,
    pub primary_key: &'static str,
    pub searchable_attributes: &'static [&'static str],
    pub filterable_attributes: &'static [&'static str],
    pub sortable_attributes: &'static [&'static str],
    pub displayed_attributes: &'static [&'static str],
    pub ranking_rules: &'static [&'static str],
    pub default_facets: &'static [&'static str],
    pub highlight_attributes: &'static [&'static str],
    pub crop_attributes: &'static [&'static str],
    pub crop_length: usize,
    pub author_filter_field: &'static str,
}

impl MeiliIndexSpec {
    pub fn settings_json(&self) -> serde_json::Value {
        json!({
            "searchableAttributes": self.searchable_attributes,
            "filterableAttributes": self.filterable_attributes,
            "sortableAttributes": self.sortable_attributes,
            "displayedAttributes": self.displayed_attributes,
            "rankingRules": self.ranking_rules
        })
    }
}

const RANKING_RULES_DEFAULT: &[&str] = &[
    "words",
    "typo",
    "proximity",
    "attribute",
    "sort",
    "exactness",
];

const RANKING_RULES_SORT_FIRST: &[&str] = &[
    "sort",
    "words",
    "typo",
    "proximity",
    "attribute",
    "exactness",
];

const PATCH_ITEM_DOCS_SPEC: MeiliIndexSpec = MeiliIndexSpec {
    uid: "patch_item_docs",
    primary_key: "id",
    searchable_attributes: &["subject", "commit_subject", "file_paths", "diff_search"],
    filterable_attributes: &[
        "id",
        "list_keys",
        "author_email",
        "has_diff",
        "patch_series_id",
        "version_num",
        "ordinal",
        "is_rfc",
        "date_ts",
        "year",
        "month",
    ],
    sortable_attributes: &["date_ts", "id", "ordinal"],
    displayed_attributes: &[
        "id",
        "scope",
        "title",
        "subject",
        "snippet",
        "author_email",
        "date_utc",
        "date_ts",
        "list_keys",
        "has_diff",
        "patch_series_id",
        "series_version_id",
        "version_num",
        "ordinal",
        "is_rfc",
        "file_paths",
        "message_id_primary",
        "route",
    ],
    ranking_rules: RANKING_RULES_DEFAULT,
    default_facets: &["list_keys", "author_email", "has_diff"],
    highlight_attributes: &["subject", "commit_subject", "diff_search"],
    crop_attributes: &["diff_search"],
    crop_length: 24,
    author_filter_field: "author_email",
};

const PATCH_SERIES_DOCS_SPEC: MeiliIndexSpec = MeiliIndexSpec {
    uid: "patch_series_docs",
    primary_key: "id",
    searchable_attributes: &[
        "canonical_subject",
        "cover_abstract",
        "patch_subjects_joined",
    ],
    filterable_attributes: &[
        "id",
        "list_keys",
        "author_email",
        "has_diff",
        "is_merged",
        "merge_state",
        "latest_version_num",
        "date_ts",
        "year",
        "month",
    ],
    sortable_attributes: &["date_ts", "id"],
    displayed_attributes: &[
        "id",
        "scope",
        "title",
        "canonical_subject",
        "snippet",
        "author_email",
        "date_utc",
        "date_ts",
        "list_keys",
        "has_diff",
        "is_merged",
        "merge_state",
        "latest_version_num",
        "latest_version_id",
        "is_rfc_latest",
        "merged_in_tag",
        "merged_in_release",
        "merged_version_id",
        "merged_commit_id",
        "route",
    ],
    ranking_rules: RANKING_RULES_SORT_FIRST,
    default_facets: &["list_keys", "author_email", "has_diff", "is_merged", "merge_state"],
    highlight_attributes: &[
        "canonical_subject",
        "cover_abstract",
        "patch_subjects_joined",
    ],
    crop_attributes: &["cover_abstract", "patch_subjects_joined"],
    crop_length: 30,
    author_filter_field: "author_email",
};

const THREAD_DOCS_SPEC: MeiliIndexSpec = MeiliIndexSpec {
    uid: "thread_docs",
    primary_key: "id",
    searchable_attributes: &["subject", "participants_joined", "snippet_corpus"],
    filterable_attributes: &[
        "id",
        "list_keys",
        "author_emails",
        "has_diff",
        "date_ts",
        "year",
        "month",
    ],
    sortable_attributes: &["date_ts", "id"],
    displayed_attributes: &[
        "id",
        "scope",
        "title",
        "subject",
        "snippet",
        "date_utc",
        "date_ts",
        "created_at",
        "last_activity_at",
        "list_key",
        "list_keys",
        "message_count",
        "starter_name",
        "starter_email",
        "participants",
        "author_emails",
        "has_diff",
        "route",
    ],
    ranking_rules: RANKING_RULES_SORT_FIRST,
    default_facets: &["list_keys", "author_emails", "has_diff"],
    highlight_attributes: &["subject", "participants_joined", "snippet_corpus"],
    crop_attributes: &["snippet_corpus"],
    crop_length: 32,
    author_filter_field: "author_emails",
};

#[cfg(test)]
mod tests {
    use super::{MeiliIndexKind, SearchScope};

    #[test]
    fn search_scope_defaults_map_to_expected_indexes() {
        assert_eq!(SearchScope::Thread.index_kind(), MeiliIndexKind::ThreadDocs);
        assert_eq!(
            SearchScope::Series.index_kind(),
            MeiliIndexKind::PatchSeriesDocs
        );
        assert_eq!(
            SearchScope::PatchItem.index_kind(),
            MeiliIndexKind::PatchItemDocs
        );
    }

    #[test]
    fn search_scope_parser_rejects_removed_email_scope() {
        assert_eq!(SearchScope::parse("email"), None);
    }

    #[test]
    fn every_index_has_settings_builder() {
        for kind in [
            MeiliIndexKind::PatchItemDocs,
            MeiliIndexKind::PatchSeriesDocs,
            MeiliIndexKind::ThreadDocs,
        ] {
            let settings = kind.spec().settings_json();
            assert!(settings.get("searchableAttributes").is_some());
            assert!(settings.get("filterableAttributes").is_some());
            assert!(settings.get("sortableAttributes").is_some());
            assert!(settings.get("displayedAttributes").is_some());
        }
    }
}
