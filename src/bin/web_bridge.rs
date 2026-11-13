
use futures_util::{SinkExt, StreamExt};
use std::error::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("WebSocket bridge on port 8080");

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                eprintln!("Bridge error: {e}");
            }
        });
    }
}

async fn handle_client(
    socket: tokio::net::TcpStream,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ws = accept_async(socket).await?;
    let tcp = TcpStream::connect("127.0.0.1:5000").await?;

    let (mut ws_tx, mut ws_rx) = ws.split();
    let (mut tcp_read, mut tcp_write) = tcp.into_split();

    let browser_to_server = async {
        while let Some(message) = ws_rx.next().await {
            let message = message?;

            if let Message::Text(text) = message {
                // Existing protocol expects newline-delimited input.
                tcp_write.write_all(text.as_bytes()).await?;
                tcp_write.write_all(b"\n").await?;
            }
        }

        Ok::<(), Box<dyn Error + Send + Sync>>(())
    };

    let server_to_browser = async {
        let mut buffer = [0u8; 4096];

        loop {
            let n = tcp_read.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            // Preserve partial TCP chunks.
           let message = String::from_utf8_lossy(&buffer[..n]).into_owned();
            ws_tx.send(Message::Text(message.into())).await?;
        }

        Ok::<(), Box<dyn Error + Send + Sync>>(())
    };

    tokio::select! {
        result = browser_to_server => result?,
        result = server_to_browser => result?,
    }

    Ok(())
}
