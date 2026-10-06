//! S3 service events.
//!
//! Events sent from the S3 background service task back to the UI layer.
//! Re-exports from `crate::types` since `SyncContext` also references `S3Event`.

pub use crate::types::S3Event;
