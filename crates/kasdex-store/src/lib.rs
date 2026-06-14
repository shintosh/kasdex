#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store backend is not implemented yet")]
    NotImplemented,
}
