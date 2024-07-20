// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str;
use std::sync::Arc;

use crate::RT;
use pyo3::prelude::*;
use pyo3::{
    exceptions::{PyIOError, PyValueError},
    pyclass,
    types::PyDict,
    PyObject, PyResult,
};

use self::commit::PyCommitLock;

/// Lance Dataset that will be wrapped by another class in Python
#[pyclass(name = "_Tags", module = "_lib")]
#[derive(Clone)]
pub struct Tags {
    pub(crate) ds: Arc<LanceDataset>,
}

#[pymethods]
impl Tags {
    #[new]
    fn new(dataset: LanceDataset) -> PyResult<Self> {
        Ok(Self {
            ds: Arc::new(dataset),
        })
    }

    pub fn __copy__(&self) -> Self {
        self.clone()
    }

    fn list(self_: PyRef<'_, Self>) -> PyResult<PyObject> {
        let tags = self_
            .ds
            .tags
            .list()
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Python::with_gil(|py| {
            let pytags = PyDict::new(py);
            for (k, v) in tags.iter() {
                let dict = PyDict::new(py);
                dict.set_item("version", v.version).unwrap();
                dict.set_item("manifest_size", v.manifest_size).unwrap();
                dict.to_object(py);
                pytags.set_item(k, dict).unwrap();
            }
            Ok(pytags.to_object(py))
        })
    }

    fn create(&mut self, tag: String, version: u64) -> PyResult<()> {
        let mut new_self = self.ds.tags.as_ref().clone();
        RT.block_on(None, new_self.create(tag.as_str(), version))?
            .map_err(|err| match err {
                lance::Error::NotFound { .. } => PyValueError::new_err(err.to_string()),
                lance::Error::RefConflict { .. } => PyValueError::new_err(err.to_string()),
                lance::Error::VersionNotFound { .. } => PyValueError::new_err(err.to_string()),
                _ => PyIOError::new_err(err.to_string()),
            })?;
        self.tags = Arc::new(new_self);
        Ok(())
    }

    fn delete(&mut self, tag: String) -> PyResult<()> {
        let mut new_self = self.ds.tags.as_ref().clone();
        RT.block_on(None, new_self.delete(tag.as_str()))?
            .map_err(|err| match err {
                lance::Error::NotFound { .. } => PyValueError::new_err(err.to_string()),
                lance::Error::RefNotFound { .. } => PyValueError::new_err(err.to_string()),
                _ => PyIOError::new_err(err.to_string()),
            })?;
        self.tags = Arc::new(new_self);
        Ok(())
    }
}
