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
    ThroughputPriority,
    Fairness,
    TargetedThroughput,
}

const TARGET_PARTITIONS: &[Key] = &[0, 1, 2, 3, 4];
//const TARGET_BOOST: usize = 500;
//const NON_TARGET_PENALTY: usize = 50;

// FCFS does nothing
pub fn fcfs(_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {}

pub fn throughput_priority(
    msg_buffer: &mut Vec<(u64, ClusterMessage)>,
    committed_entries: &HashMap<Key, usize>,
    quorum_size: usize,
) {
    let mut quorum_tracker: HashMap<(Key, KvBallot), usize> = HashMap::new();

    // Count how many times each ballot has been accepted for each key
    for (_, msg) in msg_buffer.iter() {
        if let ClusterMessage::OmniPaxosMessage((
            key,
            Message::SequencePaxos(PaxosMessage {
                msg: PaxosMsg::Accepted(a),
                ..
            }),
        )) = msg
        {
            let ballot = KvBallot {
                config_id: a.n.config_id,
                n: a.n.n,
                priority: a.n.priority,
                pid: a.n.pid,
            };
            *quorum_tracker.entry((*key, ballot)).or_insert(0) += 1;
        }
    }

    msg_buffer.sort_by_key(|(_, msg)| {
        if let ClusterMessage::OmniPaxosMessage((key, Message::SequencePaxos(paxos))) = msg {
            let committed = committed_entries.get(key).copied().unwrap_or(usize::MAX);

            match &paxos.msg {
                PaxosMsg::Accepted(a) => {
                    let ballot = KvBallot {
                        config_id: a.n.config_id,
                        n: a.n.n,
                        priority: a.n.priority,
                        pid: a.n.pid,
                    };
                    let accept_count = quorum_tracker.get(&(*key, ballot)).copied().unwrap_or(0);
                    let quorum_distance = quorum_size.saturating_sub(accept_count + 1);

                    let quorum_boost = (quorum_distance * 500_000).min(2_000_000);
                    let commit_boost = (5000_usize).saturating_sub(committed.min(5000));

                    // Lower score = higher priority
                    committed.saturating_sub(commit_boost).saturating_sub(quorum_boost)
                }

                PaxosMsg::AcceptDecide(_) => {
                    let commit_boost = (5000_usize).saturating_sub(committed.min(5000));
                    committed.saturating_sub(commit_boost / 2).saturating_sub(500_000)
                }

                _ => committed + 5_000_000, // Everything else is deprioritized
            }
        } else {
            usize::MAX
        }
    });
}

pub fn fairness_priority(
    msgs: &mut Vec<(NodeId, ClusterMessage)>,
    committed: &HashMap<Key, usize>,
    deltas: &HashMap<Key, usize>,
) {
    if deltas.is_empty() || committed.is_empty() {
        return;
    }

    let &max_delta = deltas.values().max().unwrap_or(&0);
    let median_committed = {
        let mut vals: Vec<usize> = committed.values().cloned().collect();
        vals.sort();
        vals[vals.len() / 2]
    };

    const MAX_BOOST: usize = 3_000_000; // increased
    const URGENCY_BOOST: usize = 100_000; // decreased (fairness cares less about message type)

    msgs.sort_by_key(|(_, msg)| {
        if let ClusterMessage::OmniPaxosMessage((key, Message::SequencePaxos(paxos))) = msg {
            let delta = deltas.get(key).copied().unwrap_or(0);
            let committed_count = committed.get(key).copied().unwrap_or(usize::MAX);

            let lag = max_delta.saturating_sub(delta);
            let lag_ratio = (lag as f64) / (max_delta.max(1) as f64);
            let fairness_boost = (lag_ratio.powi(3) * MAX_BOOST as f64) as usize; // more aggressive

            let penalty = if committed_count > median_committed {
                (committed_count - median_committed).pow(2) * 10 // harsher penalty
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


pub fn targeted_throughput(
    buffer: &mut Vec<(u64, ClusterMessage)>,
    committed_map: &HashMap<Key, usize>,
    write_quorum: usize,
) {
    buffer.sort_by_key(|(_, msg)| {
        match msg {
            ClusterMessage::OmniPaxosMessage((key, Message::SequencePaxos(paxos))) => {
                let committed = committed_map.get(key).copied().unwrap_or(0);
                let base_score = match paxos.msg {
                    PaxosMsg::Accepted(_) | PaxosMsg::AcceptDecide(_) => {
                        write_quorum.saturating_sub(committed)
                    }
                    _ => 10_000,
                };

                if TARGET_PARTITIONS.contains(key) {
                    base_score.saturating_sub(200) // heavy boost
                } else {
                    base_score + 2_000 // heavier penalty
                }
            }
            _ => 20_000,
        }
    });
}

