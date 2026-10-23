use std::net::{TcpStream, TcpListener};
use std::io::Write;

fn write_message(stream: &mut TcpStream, message: &str) -> std::io::Result<()> {
    let format_message = format!("{}\n", message);
    println!("Client send: {}", message);
    stream.write_all(format_message.as_bytes())
}

fn main() -> std::io::Result<()> {
    // connect to tcp server
    let mut stream = TcpStream::connect("127.0.0.1:5000")?;

    // writedata to stream
    write_message(&mut stream, "Hello server!");
    stream.flush()?;

    Ok(())
}