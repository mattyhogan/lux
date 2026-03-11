mod cmd;
mod pubsub;
mod resp;
mod snapshot;
mod store;

use bytes::BytesMut;
use cmd::CmdResult;
use pubsub::Broker;
use resp::Parser;
use store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    let store = Arc::new(Store::new());
    let broker = Broker::new();

    match snapshot::load(&store) {
        Ok(0) => println!("no snapshot found"),
        Ok(n) => println!("loaded {n} keys from snapshot"),
        Err(e) => eprintln!("snapshot load error: {e}"),
    }

    tokio::spawn(snapshot::background_save_loop(store.clone()));

    println!("lux v{} ready on {addr}", env!("CARGO_PKG_VERSION"));

    loop {
        let (socket, peer) = listener.accept().await?;
        let store = store.clone();
        let broker = broker.clone();
        socket.set_nodelay(true).ok();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, store, broker).await {
                if e.kind() != std::io::ErrorKind::ConnectionReset {
                    eprintln!("connection error {peer}: {e}");
                }
            }
        });
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    _peer: std::net::SocketAddr,
    store: Arc<Store>,
    broker: Broker,
) -> std::io::Result<()> {
    let mut read_buf = vec![0u8; 65536];
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut pending = BytesMut::new();
    let mut subscriptions: HashMap<String, broadcast::Receiver<pubsub::Message>> = HashMap::new();
    let mut sub_mode = false;

    loop {
        if sub_mode {
            tokio::select! {
                result = socket.read(&mut read_buf) => {
                    let n = match result {
                        Ok(0) => return Ok(()),
                        Ok(n) => n,
                        Err(e) => return Err(e),
                    };
                    pending.extend_from_slice(&read_buf[..n]);
                    let mut parser = Parser::new(&pending);
                    while let Ok(Some(args)) = parser.parse_command() {
                        if args.is_empty() { continue; }
                        match args[0].to_uppercase().as_str() {
                            "SUBSCRIBE" => {
                                for ch in &args[1..] {
                                    if !subscriptions.contains_key(ch) {
                                        let rx = broker.subscribe(ch).await;
                                        subscriptions.insert(ch.clone(), rx);
                                    }
                                    resp::write_array_header(&mut write_buf, 3);
                                    resp::write_bulk(&mut write_buf, "subscribe");
                                    resp::write_bulk(&mut write_buf, ch);
                                    resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                                }
                            }
                            "UNSUBSCRIBE" => {
                                let channels: Vec<String> = if args.len() > 1 {
                                    args[1..].to_vec()
                                } else {
                                    subscriptions.keys().cloned().collect()
                                };
                                for ch in &channels {
                                    subscriptions.remove(ch);
                                    resp::write_array_header(&mut write_buf, 3);
                                    resp::write_bulk(&mut write_buf, "unsubscribe");
                                    resp::write_bulk(&mut write_buf, ch);
                                    resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                                }
                                if subscriptions.is_empty() {
                                    sub_mode = false;
                                }
                            }
                            "PING" => {
                                if args.len() > 1 {
                                    resp::write_bulk(&mut write_buf, &args[1]);
                                } else {
                                    resp::write_pong(&mut write_buf);
                                }
                            }
                            _ => {
                                resp::write_error(&mut write_buf, "ERR only SUBSCRIBE, UNSUBSCRIBE, and PING are allowed in subscribe mode");
                            }
                        }
                    }
                    let consumed = parser.pos();
                    let _ = pending.split_to(consumed);
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                }
                msg = async {
                    for (_ch, rx) in subscriptions.iter_mut() {
                        if let Ok(msg) = rx.try_recv() {
                            return Some(msg);
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    for (_ch, rx) in subscriptions.iter_mut() {
                        if let Ok(msg) = rx.try_recv() {
                            return Some(msg);
                        }
                    }
                    None
                } => {
                    if let Some(msg) = msg {
                        resp::write_array_header(&mut write_buf, 3);
                        resp::write_bulk(&mut write_buf, "message");
                        resp::write_bulk(&mut write_buf, &msg.channel);
                        resp::write_bulk(&mut write_buf, &msg.payload);
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                }
            }
        } else {
            let n = match socket.read(&mut read_buf).await {
                Ok(0) => return Ok(()),
                Ok(n) => n,
                Err(e) => return Err(e),
            };

            pending.extend_from_slice(&read_buf[..n]);
            let mut parser = Parser::new(&pending);

            while let Ok(Some(args)) = parser.parse_command() {
                if args.is_empty() {
                    continue;
                }
                match cmd::execute(&store, &broker, &args, &mut write_buf) {
                    CmdResult::Written => {}
                    CmdResult::Subscribe { channels } => {
                        for ch in &channels {
                            let rx = broker.subscribe(ch).await;
                            subscriptions.insert(ch.clone(), rx);
                            resp::write_array_header(&mut write_buf, 3);
                            resp::write_bulk(&mut write_buf, "subscribe");
                            resp::write_bulk(&mut write_buf, ch);
                            resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                        }
                        sub_mode = true;
                        break;
                    }
                    CmdResult::Publish { channel, message } => {
                        let count = broker.publish(&channel, message).await;
                        resp::write_integer(&mut write_buf, count);
                    }
                }
            }

            let consumed = parser.pos();
            let _ = pending.split_to(consumed);

            if !write_buf.is_empty() {
                socket.write_all(&write_buf).await?;
                write_buf.clear();
            }
        }
    }
}
