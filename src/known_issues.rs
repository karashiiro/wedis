use std::collections::HashMap;

use tracing::warn;

pub fn warn_known_issues() {
    let mut clients_with_known_issues = HashMap::new();

    // Is a 100ms timeout too short? https://github.com/tidwall/redcon.rs/blob/master/src/lib.rs#L480
    clients_with_known_issues.insert("npm:redis-cli", "will immediately time out");

    warn!("Clients with known issues: {:?}", clients_with_known_issues);
}
