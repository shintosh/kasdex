#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeProbeStatus {
    pub network: String,
    pub is_pruned: bool,
}
