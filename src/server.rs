use std::{future::Future, io::Cursor, net::SocketAddr};

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Sender},
        mpsc,
    },
};
use tracing::{error, info};

use crate::connection::ConnectionContext;

// https://github.com/tokio-rs/mini-redis/blob/master/src/server.rs

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("unknown error")]
    Unknown(String),
}

pub struct Server {
    listener: TcpListener,
    notify_shutdown: Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

pub trait Handler {
    fn on_open(&self, connection: &mut Conn) -> impl Future<Output = ()> + Send;

    fn on_close(
        &self,
        connection: &mut Conn,
        err: Option<&ServerError>,
    ) -> impl Future<Output = ()> + Send;

    fn on_command(
        &self,
        connection: &mut Conn,
        args: Vec<Vec<u8>>,
    ) -> impl Future<Output = ()> + Send;
}

pub struct Conn {
    addr: SocketAddr,
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    pub context: Option<Box<ConnectionContext>>,
}

impl Conn {
    pub fn new(socket: TcpStream) -> Result<Self, ServerError> {
        Ok(Self {
            addr: socket.peer_addr()?,
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
            context: None,
        })
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn read_frame(&mut self) -> Result<Option<String>, ServerError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(ServerError::Unknown("connection reset by peer".into()));
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<String>, ServerError> {
        let mut _buf = Cursor::new(&self.buffer[..]);
        todo!()
    }

    pub async fn write_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        todo!()
    }

    pub fn write_bulk(&mut self, msg: &[u8]) {
        todo!()
    }

    pub fn write_array(&mut self, count: usize) {
        todo!()
    }

    pub fn write_string(&mut self, msg: &str) {
        todo!()
    }

    pub fn write_integer(&mut self, x: i64) {
        todo!()
    }

    pub fn write_error(&mut self, err: &str) {
        todo!()
    }

    pub fn write_null(&mut self) {
        todo!()
    }

    pub fn connection_id(&mut self) -> i64 {
        todo!()
    }
}

#[derive(Debug)]
struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;

        self.is_shutdown = true;
    }
}

struct ConnWrap {
    connection: Conn,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl ConnWrap {
    async fn run<H: Handler>(&mut self, handler: &H) -> Result<(), ServerError> {
        let result = self.run_inner(handler).await;
        handler
            .on_close(&mut self.connection, result.as_ref().err())
            .await;
        result
    }

    async fn run_inner<H: Handler>(&mut self, handler: &H) -> Result<(), ServerError> {
        handler.on_open(&mut self.connection).await;

        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            handler.on_command(&mut self.connection, vec![]).await;
        }

        Ok(())
    }
}

impl Server {
    async fn run<H: Send + Sync + Clone + Handler + 'static>(
        &self,
        handler: H,
    ) -> Result<(), ServerError> {
        info!("accepting inbound connections");

        loop {
            let socket = self.accept().await?;
            let mut conn = ConnWrap {
                connection: Conn::new(socket)?,
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            let handler_clone = handler.clone();
            tokio::spawn(async move {
                if let Err(err) = conn.run(&handler_clone).await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    async fn accept(&self) -> Result<TcpStream, ServerError> {
        match self.listener.accept().await {
            Ok((socket, _)) => Ok(socket),
            Err(err) => Err(err.into()),
        }
    }
}

pub async fn serve<H: Send + Sync + Clone + Handler + 'static>(
    handler: H,
    shutdown: impl Future,
) -> Result<(), ServerError> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Serving at {}", addr);

    let (notify_shutdown, _) = broadcast::channel::<()>(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

    let server = Server {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run(handler) => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    let Server {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
