pub struct ConnectionContext {
    lib_name: String,
    lib_version: String,
}

impl ConnectionContext {
    pub fn new() -> Self {
        ConnectionContext {
            lib_name: "".to_string(),
            lib_version: "".to_string(),
        }
    }

    pub fn set_lib_name(&mut self, lib_name: &str) {
        self.lib_name = lib_name.to_owned()
    }

    pub fn set_lib_version(&mut self, lib_version: &str) {
        self.lib_version = lib_version.to_owned()
    }
}
