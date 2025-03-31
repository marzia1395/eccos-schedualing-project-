use std::time::Duration;

use crate::data_collection::DataCollection;
use log::info;
use omnipaxos::{
    ballot_leader_election::Ballot, messages::Message, storage::Storage, util::LogEntry, OmniPaxos,
    OmniPaxosConfig,
};
use omnipaxos_kv::common::{kv::*, messages::*};
use omnipaxos_storage::memory_storage::MemoryStorage;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct Partition {
    server_id: NodeId,
    key_range: KeyRange,
    omnipaxos: OmniPaxosInstance,
    msg_buffer: Vec<Message<Command>>,
    current_decided_idx: usize,
    initial_leader: NodeId,
    data_collection: DataCollection,
    //scheduling_strategy: SchedulingStrategy,
}

impl Partition {
    pub fn new(
        key_range: KeyRange,
        initial_leader: NodeId,
        op_config: OmniPaxosConfig,
        server_id: NodeId,
        //scheduling_strategy: SchedulingStrategy,
    ) -> Self {
        let mut storage: MemoryStorage<Command> = MemoryStorage::default();
        let init_leader_ballot = Ballot {
            config_id: 0,
            n: 1,
            priority: 0,
            pid: initial_leader,
        };

        storage
            .set_promise(init_leader_ballot)
            .expect("Failed to write to storage");

        let omnipaxos = op_config.build(storage).unwrap();
        let msg_buffer = vec![];
        let current_decided_idx = 0;
        let data_collection = DataCollection::new();

        Partition {
            server_id,
            key_range,
            omnipaxos,
            msg_buffer,
            current_decided_idx,
            initial_leader,
            data_collection,
            //scheduling_strategy,
        }
    }

    pub fn initial_leader(&self) -> NodeId {
        self.initial_leader
    }

    pub fn tick(&mut self) {
        self.omnipaxos.tick();
    }

    pub fn get_outgoing_msgs(&mut self) -> Vec<(NodeId, ClusterMessage)> {
        self.omnipaxos.take_outgoing_messages(&mut self.msg_buffer);
        let key = self.key_range().start_key().clone();
        let mut outgoing_msgs = vec![];

        for msg in self.msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage((key.clone(), msg.clone()));
            outgoing_msgs.push((to, cluster_msg));
        }

        outgoing_msgs
    }

    pub fn is_init_leader_elected(&self) -> bool {
        let status = self.omnipaxos.get_current_leader();
        if let Some((curr_leader, is_accept_phase)) = status {
            info!(
                "{}: Leader status - current leader: {}, accept_phase: {}",
                self.server_id, curr_leader, is_accept_phase
            );
            if curr_leader == self.server_id && is_accept_phase {
                info!("{}: Leader fully initialized", self.server_id);
                return true;
            }
        }
        false
    }

    pub fn leader_takeover(&mut self) {
        info!(
            "{} {}-{}: Attempting to take leadership",
            self.server_id,
            self.key_range.start_key(),
            self.key_range.end_key()
        );
        self.omnipaxos.try_become_leader();
    }

    pub fn get_decided_cmds(&mut self) -> Vec<Command> {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx >= new_decided_idx {
            return vec![];
        }

        let decided_entries = self
            .omnipaxos
            .read_decided_suffix(self.current_decided_idx)
            .unwrap();

        let decided_commands: Vec<Command> = decided_entries
            .into_iter()
            .filter_map(|e| match e {
                LogEntry::Decided(cmd) => {
                    self.data_collection.commit_entry();
                    Some(cmd)
                }
                _ => unreachable!(),
            })
            .collect();

        self.current_decided_idx = new_decided_idx;
        decided_commands
    }

    pub async fn append_to_log(&mut self, cmd: Command) {
        self.omnipaxos
            .append(cmd)
            .expect("Append to Omnipaxos log failed");
        let delay = Duration::from_millis(10 + (self.key_range.start_key() % 20) as u64);
        tokio::time::sleep(delay).await;
    }

    pub fn key_range(&self) -> &KeyRange {
        &self.key_range
    }

    pub fn handle_incoming(&mut self, msg: Message<Command>) {
        self.omnipaxos.handle_incoming(msg);
    }

    pub fn count_committed_entries(&self) -> usize {
        self.data_collection.count_committed_entries()
    }
}
