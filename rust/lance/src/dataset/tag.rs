use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagContents {
    pub version: u64,
    pub manifest_size: usize,
}
