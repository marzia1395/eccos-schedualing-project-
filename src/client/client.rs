use crate::{data_collection::ClientData, network::Network};
use chrono::Utc;
use std::fs;
use std::path::Path;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use rand::{
    distributions::Distribution, distributions::WeightedIndex, rngs::SmallRng, Rng, SeedableRng,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub request_config: RequestConfig,
    pub local_deployment: Option<bool>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
    pub seed: Option<u64>, 
}

const NETWORK_BATCH_SIZE: usize = 1000;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            vec![config.server_id],
            local_deployment,
            NETWORK_BATCH_SIZE,
        )
        .await;

        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                let now = Utc::now();
                let time_until_scheduled_start_ms = start_time - now.timestamp_millis();
                if time_until_scheduled_start_ms > 0 {
                    sleep(Duration::from_millis(time_until_scheduled_start_ms as u64)).await;
                } else {
                    warn!("Started after synchronization point!");
                }
            }
            _ => panic!("Error waiting for start signal"),
        };
    
        info!("{}: Start sending requests", self.id);
        let seed = self.config.seed.unwrap_or(self.id as u64); // fallback to id if no seed provided
        let mut rng = SmallRng::seed_from_u64(seed);
        let request_config = self.config.request_config;
    
        let (tx_wakeup, mut rx_wakeup) = mpsc::channel(1);
        let (tx_end, mut rx_end) = mpsc::channel(1);
    
        //  Spawn task to stop after experiment duration
        let experiment_duration = request_config.get_experiment_duration();
        let tx_end_clone = tx_end.clone();
        tokio::spawn(async move {
            sleep(experiment_duration).await;
            let _ = tx_end_clone.send(true).await;
        });
    
        // Generate inter-arrival wake-ups for request generation
        let _wake_up_task = tokio::spawn({
            let mut rng = rng.clone();
            let request_config = request_config.clone();
            let tx_wakeup = tx_wakeup.clone();
    
            async move {
                loop {
                    let wakeup_duration = request_config.compute_inter_arrival_time(&mut rng);
                    sleep(wakeup_duration).await;
                    if tx_wakeup.send(true).await.is_err() {
                        break; // Channel closed = end experiment
                    }
                }
            }
        });
    
        // Main event loop
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                },
                _ = rx_wakeup.recv() => {
                    let is_write = rng.gen::<f64>() > request_config.read_ratio;
                    let (key, val) = request_config.generate_kv_request(&mut rng);
                    self.send_request(key, val, is_write).await;
                },
                _ = rx_end.recv() => {
                    info!("{}: Experiment time elapsed, shutting down client", self.id);
                    break;
                }
            }
        }
    
        info!(
            "{}: Client finished: collected {} responses with avg latency of {}ms",
            self.id,
            self.client_data.response_count(),
            self.client_data.avg_latency_ms()
        );
    
        self.network
            .send(self.active_server, ClientMessage::EndExperiment)
            .await;
        self.network.shutdown().await;
        self.save_results().expect("Failed to save results");
    }
    

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Received {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
            }
        }
    }

    async fn send_request(&mut self, key: Key, val: Value, is_write: bool) {
        let cmd = if is_write {
            KVCommand::Put(key, val)
        } else {
            KVCommand::Get(key)
        };

        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!(
            "Sending {} request: key={}, value={}, id={}",
            if is_write { "Put" } else { "Get" },
            key,
            val,
            self.next_request_id
        );

        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        false
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        // Ensure the directory for summary file exists
        if let Some(parent) = Path::new(&self.config.summary_filepath).parent() {
            fs::create_dir_all(parent)?;
        }
    
        self.client_data.save_summary(self.config.clone())?;
    
        // Ensure the directory for output CSV exists
        if let Some(parent) = Path::new(&self.config.output_filepath).parent() {
            fs::create_dir_all(parent)?;
        }
    
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
    
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone, Serialize, Copy)]
#[serde(tag = "type", content = "weights")]
pub enum SkewType {
    Uniform,
    Weighted([i32; 10]), // Adjust array size based on number of partitions or keys
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestConfig {
    duration_sec: u64,
    requests_per_sec: f64,
    read_ratio: f64,
    key_range: [Key; 2],
    skew: SkewType,
}

impl RequestConfig {
    pub fn get_experiment_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    pub fn compute_inter_arrival_time(&self, rng: &mut SmallRng) -> Duration {
        let u: f64 = rng.gen();
        let lambda = self.requests_per_sec;
        let interval = -((1.0 - u).ln()) / lambda;
        Duration::from_secs_f64(interval)
    }

    pub fn generate_kv_request(&self, rng: &mut SmallRng) -> (Key, Value) {
        let val = rng.gen_range(0..=10);
        let key = match &self.skew {
            SkewType::Uniform => rng.gen_range(self.key_range[0]..=self.key_range[1]),
            SkewType::Weighted(weights) => {
                assert_eq!(
                    (self.key_range[1] - self.key_range[0] + 1) % weights.len(),
                    0,
                    "Key range must divide evenly across number of weights"
                );
                let dist = WeightedIndex::new(weights).unwrap();
                let partition = dist.sample(rng);
                let partition_size = (self.key_range[1] - self.key_range[0] + 1) / weights.len();
                let partition_start = self.key_range[0] + partition * partition_size;
                let offset = rng.gen_range(0..partition_size);
                partition_start + offset
            }
        };
        (key, val)
    }  
}
