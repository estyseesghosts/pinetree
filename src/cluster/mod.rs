//! Phase 4 placeholder: multi-node cluster coordination.
//!
//! Nothing here is implemented yet. Selecting `mode = "Cluster"` is
//! rejected at startup (`Config::validate`), and no code imports these
//! submodules. Do not treat their presence as working cluster support.

pub mod account_directory;
pub mod internal_protocol;
pub mod routing;
