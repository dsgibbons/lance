use std::ops::Range;

use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagContents {
    pub version: u64,
    pub manifest_size: usize,
}

pub fn base_tags_path(base_path: &Path) -> Path {
    base_path.child("_refs").child("tags")
}

pub fn tag_path(base_path: &Path, tag: &str) -> Path {
    base_tags_path(base_path).child(format!("{}.json", tag))
}

pub fn check_valid_ref(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::InvalidRef {
            message: "Ref cannot be empty".to_string(),
        });
    }

    for component in s.split('/') {
        if component.starts_with('.') || component.ends_with(".lock") {
            return Err(Error::InvalidRef {
                message: "Slash-separated ref cannot begin with a dot or end with .lock"
                    .to_string(),
            });
        }
    }

    if s.contains("..") {
        return Err(Error::InvalidRef {
            message: "Ref cannot have two consecutive dots".to_string(),
        });
    }

    if s.chars()
        .any(|c| c.is_control() || c == ' ' || c == '~' || c == '^' || c == ':')
    {
        return Err(Error::InvalidRef {
            message: "Ref cannot have ASCII control characters, space, ~, ^, or :".to_string(),
        });
    }

    if s.contains('?') || s.contains('*') || s.contains('[') {
        return Err(Error::InvalidRef {
            message: "Ref cannot have question-mark, asterisk, or open bracket".to_string(),
        });
    }

    if s.starts_with('/') || s.ends_with('/') || s.contains("//") {
        return Err(Error::InvalidRef {
            message: "Ref cannot begin or end with a slash or contain multiple consecutive slashes"
                .to_string(),
        });
    }

    if s.ends_with("..") {
        return Err(Error::InvalidRef {
            message: "Ref cannot end with a dot".to_string(),
        });
    }

    if s.contains("@{") {
        return Err(Error::InvalidRef {
            message: "Ref cannot contain a sequence @{".to_string(),
        });
    }

    if s == "@" {
        return Err(Error::InvalidRef {
            message: "Ref cannot be the single character @".to_string(),
        });
    }

    if s.contains('\\') {
        return Err(Error::InvalidRef {
            message: "Ref cannot contain a backslash".to_string(),
        });
    }

    Ok(())
}

impl TagContents {
    pub async fn from_path(path: &Path, object_store: &ObjectStore) -> Result<Self> {
        let tag_reader = object_store.open(path).await?;
        let tag_bytes = tag_reader
            .get_range(Range {
                start: 0,
                end: tag_reader.size().await?,
            })
            .await?;
        Ok(serde_json::from_str(
            String::from_utf8(tag_bytes.to_vec()).unwrap().as_str(),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::assert_contains;

    use rstest::rstest;

    #[rstest]
    fn test_ok_ref(
        #[values(
            "deeply/nested/ref",
            "nested/ref.extension",
            "ref",
            "ref.extension",
            "ref_with_underscores"
        )]
        r: &str,
    ) {
        check_valid_ref(r).unwrap();
    }

    #[rstest]
    fn test_err_ref(
        #[values(
            "",
            "../ref",
            ".ref",
            "/ref",
            "@",
            "nested//ref",
            "nested\\ref",
            "ref*",
            "ref.lock",
            "ref/",
            "ref?",
            "ref@{ref",
            "ref[",
            "ref^",
            "~/ref"
        )]
        r: &str,
    ) {
        assert_contains!(
            check_valid_ref(r).err().unwrap().to_string(),
            "Ref is invalid. Ref must confirm to git ref formatting rules"
        );
    }
}
