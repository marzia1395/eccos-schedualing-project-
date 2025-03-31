use crate::{
    configs::OmniPaxosServerConfig,
    database::Database,
    network::Network,
    partition::Partition,
    scheduler::{self, SchedulingStrategy},
};
use chrono::Utc;
use csv::Writer;
use log::*;
use omnipaxos::{util::NodeId, OmniPaxosConfig};
use omnipaxos_kv::common::kv::Key;
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::{fs::File, io::Write, time::Duration};

const NETWORK_BATCH_SIZE: usize = 5000;
const LEADER_WAIT: Duration = Duration::from_secs(2);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    partitions: Vec<Partition>,
    peers: Vec<NodeId>,
    config: OmniPaxosServerConfig,
    committed_history: HashMap<Key, VecDeque<usize>>,
    #[allow(dead_code)]
    pub num_partitions: u64,
    #[allow(dead_code)]
    pub partition_size: u64,
}
const WINDOW_SIZE: usize = 5;

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosServerConfig) -> Self {
        let id = config.server_id;
        let database = Database::new();

        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            config.nodes.clone(),
            config.num_clients,
            config.local_deployment.unwrap_or(false),
            NETWORK_BATCH_SIZE,
        )
        .await;
    
        // Clone config for OmniPaxosConfig conversion
        let op_config: OmniPaxosConfig = config.clone().into();

        let step_size = config.partition_size as usize;
        let num_partitions = config.num_partitions as usize;

        let mut partitions = Vec::with_capacity(num_partitions);
        for i in (0..(num_partitions * step_size)).step_by(step_size) {
            let start_key = i;
            let end_key = i + step_size - 1;
            let key_range = KeyRange::new([start_key, end_key]);
            let partition = Partition::new(
                key_range,
                config.initial_leader,
                op_config.clone(),
                config.server_id,
            );
            partitions.push(partition);
        }

        let peers = config.get_peers(config.server_id);

        OmniPaxosServer {
            id,
            database,
            network,
            partitions,
            peers,
            num_partitions: config.num_partitions,
            partition_size: config.partition_size,
            config,
            committed_history: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election and instead force an initial leader
        // Once the leader is established it chooses a synchronization point which the
        // followers relay to their clients to begin the experiment.
        // HACK: There is only one node that can be the initial leader. More fine grained leader
        // access leads to messy code due to threading and async.
        // TODO: Implement more fine grained control
        if self
            .partitions
            .get(0)
            .expect("At least one partition is specified")
            .initial_leader()
            == self.id
        {
            self.become_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
                .await;
            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
            self.send_cluster_start_signals(experiment_sync_start);
            self.send_client_start_signals(experiment_sync_start);
        }
        // Main event loop
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    let mut outgoing_msg_buffer = vec![];
                    for partition in self.partitions.iter_mut() {
                        let mut outgoing_msgs = {
                            partition.tick();
                            partition.get_outgoing_msgs()
                        };
                        outgoing_msg_buffer.append(&mut outgoing_msgs);
                    }
                    self.send_outgoing(outgoing_msg_buffer);
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    let end_experiment = self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    if end_experiment {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    let end_experiment = self.handle_client_messages(&mut client_msg_buf).await;
                    if end_experiment {
                        if self
                            .partitions
                            .get(0)
                            .expect("At least one partition is specified")
                            .initial_leader() == self.id {
                                for peer in self.peers.iter() {
                                    self.network.send_to_cluster(*peer, ClusterMessage::LeaderStopSignal);
                                }
                        }
                        break;
                    }
                },
            }
        }
        info!("Ending Experiment and writing stats");

        match self.save_output() {
            Ok(_) => info!("save_output() completed successfully"),
            Err(e) => error!("save_output() failed: {}", e),
        }
    }

    fn send_outgoing(&mut self, mut msg_buffer: Vec<(NodeId, ClusterMessage)>) {
        match self.config.out_scheduling_strategy {
            SchedulingStrategy::FCFS => scheduler::fcfs(&mut msg_buffer),
            SchedulingStrategy::ThroughputPriority => {
                let mut committed_map = HashMap::new();
                for partition in &self.partitions {
                    let key = partition.key_range().start_key();
                    committed_map.insert(key, partition.count_committed_entries());
                }
                scheduler::throughput_priority(
                    &mut msg_buffer,
                    &committed_map,
                    self.config
                        .initial_flexible_quorum
                        .unwrap()
                        .write_quorum_size,
                );
            }
            SchedulingStrategy::Fairness => {
                let committed_map = self.partition_committed_entries();
                let window_deltas = self.fairness_window_stats();
                scheduler::fairness_priority(&mut msg_buffer, &committed_map, &window_deltas);
            }
            SchedulingStrategy::TargetedThroughput => {
                let mut committed_map = HashMap::new();
                for partition in &self.partitions {
                    let key = partition.key_range().start_key();
                    committed_map.insert(key, partition.count_committed_entries());
                }
                scheduler::targeted_throughput(
                    &mut msg_buffer,
                    &committed_map,
                    self.config
                        .initial_flexible_quorum
                        .unwrap()
                        .write_quorum_size,
                );
            }
        }

        for (to, msg) in msg_buffer {
            self.network.send_to_cluster(to, msg);
        }
    }

    // Ensures cluster is connected and leader is promoted before returning.
    async fn become_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick() => {
                    if self.partitions.iter().all(|partition| partition.is_init_leader_elected()) {
                        break;
                    }

                    let mut outgoing_msg_buffer = vec![];
                    for partition in self.partitions.iter_mut() {
                        if partition.is_init_leader_elected() {
                            continue
                        }

                        let mut outgoing_msgs = {
                            partition.leader_takeover();
                            partition.get_outgoing_msgs()
                        };
                        outgoing_msg_buffer.append(&mut outgoing_msgs);
                    }
                    self.send_outgoing(outgoing_msg_buffer);
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    _ = self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self, decided_commands: Vec<Command>) {
        for decided_command in decided_commands {
            let read = self.database.handle_command(decided_command.kv_cmd);

            // NOTE: Only respond to client if server is the issuer
            if decided_command.coordinator_id != self.id {
                continue;
            }

            let response = match read {
                Some(read_result) => ServerMessage::Read(decided_command.id, read_result),
                None => ServerMessage::Write(decided_command.id),
            };
            let to = decided_command.client_id;

            self.network.send_to_client(to, response);
        }
    }

    fn get_partition(&mut self, key: &Key) -> &mut Partition {
        let idx = key / self.config.partition_size as usize;
        self.partitions
            .get_mut(idx)
            .expect("Should yield a partition")
    }

    async fn handle_client_messages(
        &mut self,
        messages: &mut Vec<(ClientId, ClientMessage)>,
    ) -> bool {
        let id = self.id;
        let mut outgoing_msg_buffer = vec![];

        for (from, client_msg) in messages.drain(..) {
            match client_msg {
                ClientMessage::EndExperiment => return true,
                ClientMessage::Append(command_id, kv_command) => {
                    let mut outgoing_msgs = {
                        let key = match &kv_command {
                            KVCommand::Put(key, _)
                            | KVCommand::Get(key)
                            | KVCommand::Delete(key) => key,
                        };

                        let partition = self.get_partition(&key);

                        let command = Command {
                            client_id: from,
                            coordinator_id: id,
                            id: command_id,
                            kv_cmd: kv_command,
                        };

                        partition.append_to_log(command).await;
                        partition.get_outgoing_msgs()
                    };

                    outgoing_msg_buffer.append(&mut outgoing_msgs);
                }
            }
        }
        self.send_outgoing(outgoing_msg_buffer);
        return false;
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut outgoing_msg_buffer = vec![];
        trace!("Incoming Queue: {:?}", messages);

        match self.config.in_scheduling_strategy {
            SchedulingStrategy::FCFS => scheduler::fcfs(messages),
            SchedulingStrategy::ThroughputPriority => {
                let mut committed_map = HashMap::new();
                for partition in &self.partitions {
                    let key = partition.key_range().start_key();
                    committed_map.insert(key, partition.count_committed_entries());
                }
                scheduler::throughput_priority(
                    messages,
                    &committed_map,
                    self.config
                        .initial_flexible_quorum
                        .unwrap()
                        .write_quorum_size,
                );
            }
            SchedulingStrategy::Fairness => {
                let committed_map = self.partition_committed_entries();
                let window_deltas = self.fairness_window_stats();
                scheduler::fairness_priority(messages, &committed_map, &window_deltas);
            }
            SchedulingStrategy::TargetedThroughput => {
                let mut committed_map = HashMap::new();
                for partition in &self.partitions {
                    let key = partition.key_range().start_key();
                    committed_map.insert(key, partition.count_committed_entries());
                }
                scheduler::targeted_throughput(
                    messages,
                    &committed_map,
                    self.config
                        .initial_flexible_quorum
                        .unwrap()
                        .write_quorum_size,
                );
            }
        }
        for (_from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage((key, msg)) => {
                    let (decided_commands, mut outgoing_msgs) = {
                        let partition = self.get_partition(&key);
                        partition.handle_incoming(msg);
                        (partition.get_decided_cmds(), partition.get_outgoing_msgs())
                    };

                    self.handle_decided_entries(decided_commands);
                    outgoing_msg_buffer.append(&mut outgoing_msgs);
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    info!("LeaderStartSignal received! Syncing at {}", start_time);
                    self.send_client_start_signals(start_time)
                }
                ClusterMessage::LeaderStopSignal => {
                    info!("LeaderStopSignal received! Stopping Leader");
                    return true;
                }
            }
        }
        self.send_outgoing(outgoing_msg_buffer);
        false
    }

    fn partition_committed_entries(&self) -> HashMap<Key, usize> {
        let mut map = HashMap::new();
        for partition in &self.partitions {
            let key = partition.key_range().start_key();
            map.insert(key, partition.count_committed_entries());
        }
        map
    }

    pub fn fairness_window_stats(&mut self) -> HashMap<Key, usize> {
        let mut deltas: HashMap<Key, usize> = HashMap::new();

        for partition in &mut self.partitions {
            let key = partition.key_range().start_key();
            let current_committed = partition.count_committed_entries();

            let history = self
                .committed_history
                .entry(key)
                .or_insert_with(VecDeque::new);

            let delta = if let Some(prev) = history.back() {
                current_committed.saturating_sub(*prev)
            } else {
                0
            };

            // Maintain window size
            if history.len() == WINDOW_SIZE {
                history.pop_front();
            }
            history.push_back(current_committed);

            deltas.insert(key, delta);
        }

        deltas
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&self) -> Result<(), std::io::Error> {
        println!("Writing CSV to: {}", self.config.output_filepath);
        self.to_json(self.config.summary_filepath.clone())?;
        self.to_csv(self.config.output_filepath.clone())?;

        Ok(())
    }

    fn to_json(&self, file_path: String) -> Result<(), std::io::Error> {
        if let Some(parent) = Path::new(&file_path).parent() {
            fs::create_dir_all(parent)?; // Ensure directory exists
        }

        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&file_path)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()
    }

    fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        if let Some(parent) = Path::new(&file_path).parent() {
            fs::create_dir_all(parent)?; // Ensure directory exists
        }

        let file = File::create(&file_path)?;
        let mut writer = Writer::from_writer(file);

        let mut total = 0;
        for partition in self.partitions.iter() {
            let count = partition.count_committed_entries();
            writer.write_record(&[
                &format!("{}", partition.key_range().start_key()),
                &format!("{}", partition.key_range().end_key()),
                &format!("{}", count),
            ])?;
            total += count;
        }

        writer.write_record(&["total", "", &format!("{}", total)])?;
        writer.flush()?;
        Ok(())
    }
}
