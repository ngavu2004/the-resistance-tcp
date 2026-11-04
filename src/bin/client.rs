use std::env;
use std::io::Error;
use std::net::{TcpStream, TcpListener};
use std::io::{self, Read, Write};
use std::thread;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("0.0.0.0:50000")?;

    println!("Connected to game server!");

    // Get players name
    let player_name = match env::args().nth(1) {
        Some(name) => name,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Usage: cargo run --bin client -- <player-name>",
            ));
        }
    };

    // Send the name to server
    stream.write_all(player_name.as_bytes())?;

    // Spawn a thread to listen to the server
    let listener_stream = stream.try_clone()?;

    let server_listener_handler = thread::spawn( move || {
        server_listener(listener_stream);
    });

    // start a thread to send input to the server
    let server_input_handler = thread::spawn( move || {
        server_transmitter(stream);
    });

    server_listener_handler.join().unwrap();
    Ok(())
}

fn server_transmitter(mut stream: TcpStream) {
    let mut input = String::new();
    loop {
        input.clear();

        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        stream.write_all(input.as_bytes()).unwrap();
    }
    
}

fn server_listener(mut stream: TcpStream) {
    let mut buffer = [0u8; 1024];

    loop {
        let n = stream.read(&mut buffer).unwrap();
        if n == 0 {
            println!("Server disconnected");
            break;
        }
        if n > 0 {
            print!(
                "{}",
                String::from_utf8_lossy(&buffer[..n])
            );
        }
    };
}