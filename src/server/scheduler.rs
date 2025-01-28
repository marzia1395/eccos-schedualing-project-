use omnipaxos::util::NodeId;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    // TODO: Add more strategies here
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

// TODO: Add more strategies here
