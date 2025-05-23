// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{env, path::PathBuf};

use lance_core::{Error, Result};
use serde::{Deserialize, Serialize};
use snafu::location;

#[cfg(feature = "tokenizer-lindera")]
mod lindera;

#[cfg(feature = "tokenizer-jieba")]
mod jieba;

/// Tokenizer configs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizerConfig {
    /// base tokenizer:
    /// - `simple`: splits tokens on whitespace and punctuation
    /// - `whitespace`: splits tokens on whitespace
    /// - `raw`: no tokenization
    /// - `lindera/*`: Lindera tokenizer
    /// - `jieba/*`: Jieba tokenizer
    ///
    /// `simple` is recommended for most cases and the default value
    base_tokenizer: String,

    /// language for stemming and stop words
    /// this is only used when `stem` or `remove_stop_words` is true
    language: tantivy::tokenizer::Language,

    /// maximum token length
    /// - `None`: no limit
    /// - `Some(n)`: remove tokens longer than `n`
    max_token_length: Option<usize>,

    /// whether lower case tokens
    lower_case: bool,

    /// whether apply stemming
    stem: bool,

    /// whether remove stop words
    remove_stop_words: bool,

    /// ascii folding
    ascii_folding: bool,
}

impl Default for TokenizerConfig {
    fn default() -> Self {
        Self::new("simple".to_owned(), tantivy::tokenizer::Language::English)
    }
}

impl TokenizerConfig {
    pub fn new(base_tokenizer: String, language: tantivy::tokenizer::Language) -> Self {
        Self {
            base_tokenizer,
            language,
            max_token_length: Some(40),
            lower_case: true,
            stem: false,
            remove_stop_words: false,
            ascii_folding: false,
        }
    }

    pub fn base_tokenizer(mut self, base_tokenizer: String) -> Self {
        self.base_tokenizer = base_tokenizer;
        self
    }

    pub fn language(mut self, language: &str) -> Result<Self> {
        // need to convert to valid JSON string
        let language = serde_json::from_str(format!("\"{}\"", language).as_str())?;
        self.language = language;
        Ok(self)
    }

    pub fn max_token_length(mut self, max_token_length: Option<usize>) -> Self {
        self.max_token_length = max_token_length;
        self
    }

    pub fn lower_case(mut self, lower_case: bool) -> Self {
        self.lower_case = lower_case;
        self
    }

    pub fn stem(mut self, stem: bool) -> Self {
        self.stem = stem;
        self
    }

    pub fn remove_stop_words(mut self, remove_stop_words: bool) -> Self {
        self.remove_stop_words = remove_stop_words;
        self
    }

    pub fn ascii_folding(mut self, ascii_folding: bool) -> Self {
        self.ascii_folding = ascii_folding;
        self
    }

    pub fn build(&self) -> Result<tantivy::tokenizer::TextAnalyzer> {
        let mut builder = build_base_tokenizer_builder(&self.base_tokenizer)?;
        if let Some(max_token_length) = self.max_token_length {
            builder = builder.filter_dynamic(tantivy::tokenizer::RemoveLongFilter::limit(
                max_token_length,
            ));
        }
        if self.lower_case {
            builder = builder.filter_dynamic(tantivy::tokenizer::LowerCaser);
        }
        if self.stem {
            builder = builder.filter_dynamic(tantivy::tokenizer::Stemmer::new(self.language));
        }
        if self.remove_stop_words {
            let stop_word_filter = tantivy::tokenizer::StopWordFilter::new(self.language)
                .ok_or_else(|| {
                    Error::invalid_input(
                        format!(
                            "removing stop words for language {:?} is not supported yet",
                            self.language
                        ),
                        location!(),
                    )
                })?;
            builder = builder.filter_dynamic(stop_word_filter);
        }
        if self.ascii_folding {
            builder = builder.filter_dynamic(tantivy::tokenizer::AsciiFoldingFilter);
        }
        Ok(builder.build())
    }
}

fn build_base_tokenizer_builder(name: &str) -> Result<tantivy::tokenizer::TextAnalyzerBuilder> {
    match name {
        "simple" => Ok(tantivy::tokenizer::TextAnalyzer::builder(
            tantivy::tokenizer::SimpleTokenizer::default(),
        )
        .dynamic()),
        "whitespace" => Ok(tantivy::tokenizer::TextAnalyzer::builder(
            tantivy::tokenizer::WhitespaceTokenizer::default(),
        )
        .dynamic()),
        "raw" => Ok(tantivy::tokenizer::TextAnalyzer::builder(
            tantivy::tokenizer::RawTokenizer::default(),
        )
        .dynamic()),
        #[cfg(feature = "tokenizer-lindera")]
        s if s.starts_with("lindera/") => {
            let Some(home) = language_model_home() else {
                return Err(Error::invalid_input(
                    format!("unknown base tokenizer {}", name),
                    location!(),
                ));
            };
            lindera::LinderaBuilder::load(&home.join(s))?.build()
        }
        #[cfg(feature = "tokenizer-jieba")]
        s if s.starts_with("jieba/") || s == "jieba" => {
            let s = if s == "jieba" { "jieba/default" } else { s };
            let Some(home) = language_model_home() else {
                return Err(Error::invalid_input(
                    format!("unknown base tokenizer {}", name),
                    location!(),
                ));
            };
            jieba::JiebaBuilder::load(&home.join(s))?.build()
        }
        _ => Err(Error::invalid_input(
            format!("unknown base tokenizer {}", name),
            location!(),
        )),
    }
}

pub const LANCE_LANGUAGE_MODEL_HOME_ENV_KEY: &str = "LANCE_LANGUAGE_MODEL_HOME";

pub const LANCE_LANGUAGE_MODEL_DEFAULT_DIRECTORY: &str = "lance/language_models";

pub const LANCE_LANGUAGE_MODEL_CONFIG_FILE: &str = "config.json";

pub fn language_model_home() -> Option<PathBuf> {
    match env::var(LANCE_LANGUAGE_MODEL_HOME_ENV_KEY) {
        Ok(p) => Some(PathBuf::from(p)),
        Err(_) => dirs::data_local_dir().map(|p| p.join(LANCE_LANGUAGE_MODEL_DEFAULT_DIRECTORY)),
    }
}

#[cfg(feature = "tokenizer-common")]
trait TokenizerBuilder: Sized {
    type Config: serde::de::DeserializeOwned + Default;
    fn load(p: &std::path::Path) -> Result<Self> {
        if !p.is_dir() {
            return Err(Error::io(
                format!("{} is not a valid directory", p.display()),
                location!(),
            ));
        }
        use std::{fs::File, io::BufReader};
        let config_path = p.join(LANCE_LANGUAGE_MODEL_CONFIG_FILE);
        let config = if config_path.exists() {
            let file = File::open(config_path)?;
            let reader = BufReader::new(file);
            serde_json::from_reader::<BufReader<File>, Self::Config>(reader)?
        } else {
            Self::Config::default()
        };
        Self::new(config, p)
    }

    fn new(config: Self::Config, root: &std::path::Path) -> Result<Self>;

    fn build(&self) -> Result<tantivy::tokenizer::TextAnalyzerBuilder>;
}
