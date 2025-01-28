use omnipaxos_kv::common::kv::{KVCommand, Key, Value};
use std::collections::HashMap;

pub struct Database {
    db: HashMap<Key, Value>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<Value>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => Some(self.db.get(&key).map(|v| v.clone())),
        }
    }
}
