use omnipaxos::messages::{
    sequence_paxos::{PaxosMessage, PaxosMsg},
    Message,
};
use omnipaxos::util::NodeId;
use omnipaxos_kv::common::kv::Key;
use omnipaxos_kv::common::messages::Ballot as KvBallot;
use omnipaxos_kv::common::messages::ClusterMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SchedulingStrategy {
    FCFS,
    // TODO: Add more strategies here
     Fairness,
}

// NOTE: Message buffer is already fcfs
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    ()
}

// TODO: Add more strategies here
pub fn fairness_priority(
    msgs: &mut Vec<(NodeId, ClusterMessage)>,
    committed: &HashMap<Key, usize>,
    deltas: &HashMap<Key, usize>,
) {
    if deltas.is_empty() {
        return;
    }

    let &max_delta = deltas.values().max().unwrap_or(&0);
    let _median_delta = {
        let mut vals: Vec<usize> = deltas.values().cloned().collect();
        vals.sort();
        vals[vals.len() / 2]
    };

    let &_max_committed = committed.values().max().unwrap_or(&0);
    let median_committed = {
        let mut vals: Vec<usize> = committed.values().cloned().collect();
        vals.sort();
        vals[vals.len() / 2]
    };

    const MAX_BOOST: usize = 2_000_000;
    const URGENCY_BOOST: usize = 300_000;

    msgs.sort_by_key(|(_, msg)| {
        if let ClusterMessage::OmniPaxosMessage((key, Message::SequencePaxos(paxos))) = msg {
            let delta = deltas.get(key).copied().unwrap_or(0);
            let committed_count = committed.get(key).copied().unwrap_or(usize::MAX);

            let lag = max_delta.saturating_sub(delta);
            let lag_ratio = (lag as f64) / (max_delta.max(1) as f64);
            let fairness_boost = (lag_ratio.powi(2) * MAX_BOOST as f64) as usize;

            let penalty = if committed_count > median_committed + 10 {
                (committed_count - median_committed) * 20
            } else {
                0
            };

            let urgency = match paxos.msg {
                PaxosMsg::Prepare(_)
                | PaxosMsg::Promise(_)
                | PaxosMsg::Accepted(_)
                | PaxosMsg::AcceptDecide(_)
                | PaxosMsg::AcceptSync(_) => URGENCY_BOOST,
                _ => 0,
            };

            let score = committed_count
                .saturating_sub(fairness_boost)
                .saturating_sub(urgency)
                .saturating_add(penalty);

            (score, *key)
        } else {
            (usize::MAX, usize::MAX)
        }
    });
}
