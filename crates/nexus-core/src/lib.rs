pub mod config;
pub mod search;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),
}
