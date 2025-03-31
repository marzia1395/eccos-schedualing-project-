pub mod messages {
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};

    use super::{
        kv::{Command, CommandId, KVCommand, Key, Value},
        utils::Timestamp,
    };

    pub mod ballot_leader_election {
        use super::NodeId;
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Copy, Eq, Debug, Default, PartialEq, Hash, Serialize, Deserialize)]
        pub struct Ballot {
            pub config_id: u32,
            pub n: u32,
            pub priority: u32,
            pub pid: NodeId,
        }
    }

    pub use ballot_leader_election::Ballot;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
    }

    pub type Payload = (Key, OmniPaxosMessage<Command>);

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(Payload),
        LeaderStartSignal(Timestamp),
        LeaderStopSignal,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Append(CommandId, KVCommand),
        EndExperiment,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Write(CommandId),
        Read(CommandId, Option<Value>),
        StartSignal(Timestamp),
    }

    impl ServerMessage {
        pub fn command_id(&self) -> CommandId {
            match self {
                ServerMessage::Write(id) => *id,
                ServerMessage::Read(id, _) => *id,
                ServerMessage::StartSignal(_) => unimplemented!(),
            }
        }
    }
}

pub mod kv {
    use omnipaxos::{macros::Entry, storage::Snapshot};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;
    pub type InstanceId = NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub kv_cmd: KVCommand,
    }

    pub type Key = usize;
    pub type Value = usize;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct KeyRange {
        start_key: Key,
        end_key: Key,
    }

    impl KeyRange {
        pub fn new(key_pair: [Key; 2]) -> Self {
            KeyRange {
                start_key: key_pair[0],
                end_key: key_pair[1],
            }
        }

        pub fn start_key(&self) -> Key {
            self.start_key
        }

        pub fn end_key(&self) -> Key {
            self.end_key
        }

        pub fn in_between(&self, key: &Key) -> bool {
            let key = *key;
            self.start_key <= key && key <= self.end_key
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum KVCommand {
        Put(Key, Value),
        Delete(Key),
        Get(Key),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<Key, Value>,
        deleted_keys: Vec<Key>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys = Vec::new();
            for e in entries {
                match &e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key.clone(), value.clone());
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key.clone());
                        }
                    }
                    KVCommand::Get(_) => (),
                }
            }
            // remove keys that were put back
            deleted_keys.retain(|k| !snapshotted.contains_key(k));
            Self {
                snapshotted,
                deleted_keys,
            }
        }

        fn merge(&mut self, delta: Self) {
            for (k, v) in delta.snapshotted {
                self.snapshotted.insert(k, v);
            }
            for k in delta.deleted_keys {
                self.snapshotted.remove(&k);
            }
            self.deleted_keys.clear();
        }

        fn use_snapshots() -> bool {
            true
        }
    }
}

pub mod utils {
    use super::{kv::NodeId, messages::*};
    use std::net::{SocketAddr, ToSocketAddrs};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

    pub type Timestamp = i64;

    pub fn get_node_addr(
        cluster_name: &String,
        node: NodeId,
        is_local: bool,
    ) -> Result<SocketAddr, std::io::Error> {
        let dns_name = if is_local {
            // format!("s{node}:800{node}")
            format!("localhost:800{node}")
        } else {
            format!("{cluster_name}-server-{node}.internal.zone.:800{node}")
        };
        let address = dns_name.to_socket_addrs()?.next().unwrap();
        Ok(address)
    }

    pub type RegistrationConnection = Framed<
        CodecFramed<TcpStream, LengthDelimitedCodec>,
        RegistrationMessage,
        RegistrationMessage,
        Bincode<RegistrationMessage, RegistrationMessage>,
    >;

    pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
        let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
        Framed::new(length_delimited, Bincode::default())
    }

    pub type FromNodeConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClusterMessage,
        (),
        Bincode<ClusterMessage, ()>,
    >;
    pub type ToNodeConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClusterMessage,
        Bincode<(), ClusterMessage>,
    >;

    pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromNodeConnection::new(stream, Bincode::default()),
            ToNodeConnection::new(sink, Bincode::default()),
        )
    }

    // pub type ServerConnection = Framed<
    //     CodecFramed<TcpStream, LengthDelimitedCodec>,
    //     ServerMessage,
    //     ClientMessage,
    //     Bincode<ServerMessage, ClientMessage>,
    // >;

    pub type FromServerConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ServerMessage,
        (),
        Bincode<ServerMessage, ()>,
    >;

    pub type ToServerConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClientMessage,
        Bincode<(), ClientMessage>,
    >;

    pub type FromClientConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClientMessage,
        (),
        Bincode<ClientMessage, ()>,
    >;

    pub type ToClientConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ServerMessage,
        Bincode<(), ServerMessage>,
    >;

    pub fn frame_clients_connection(
        stream: TcpStream,
    ) -> (FromServerConnection, ToServerConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromServerConnection::new(stream, Bincode::default()),
            ToServerConnection::new(sink, Bincode::default()),
        )
    }

    // pub fn frame_clients_connection(stream: TcpStream) -> ServerConnection {
    //     let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    //     Framed::new(length_delimited, Bincode::default())
    // }

    pub fn frame_servers_connection(
        stream: TcpStream,
    ) -> (FromClientConnection, ToClientConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromClientConnection::new(stream, Bincode::default()),
            ToClientConnection::new(sink, Bincode::default()),
        )
    }
}
