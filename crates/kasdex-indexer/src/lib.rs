#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexerState {
    Mocked,
    Backfilling,
    Tailing,
}
