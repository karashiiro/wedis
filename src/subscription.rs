#[cfg(test)]
use mockall::automock;

use submap::SubMap;

use crate::connection::Connection;

pub struct PubSubServer {
    smap: SubMap<u64>,
}

impl PubSubServer {
    pub fn new() -> Self {
        let smap: SubMap<u64> = SubMap::new();
        Self { smap }
    }
}

#[cfg_attr(test, automock)]
pub trait MessageBus {
    fn register_client(&mut self, client: &mut dyn Connection);

    fn unregister_client(&mut self, client: &mut dyn Connection);

    fn subscribe(&mut self, client: &mut dyn Connection, channel: &str);

    fn publish(&mut self, channel: &str, message: &[u8]);
}

impl MessageBus for PubSubServer {
    fn register_client(&mut self, client: &mut dyn Connection) {
        self.smap.register_client(&client.connection_id());
    }

    fn unregister_client(&mut self, client: &mut dyn Connection) {
        self.smap.unregister_client(&client.connection_id());
    }

    fn subscribe(&mut self, client: &mut dyn Connection, channel: &str) {
        self.smap.subscribe(channel, &client.connection_id());
    }

    fn publish(&mut self, channel: &str, message: &[u8]) {
        let subscribers = self.smap.get_subscribers(channel);
        for s in subscribers {
            //
        }
    }
}
