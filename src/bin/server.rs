use std::net::{TcpStream, TcpListener};
use std::io::{self, BufRead, BufReader};
use std::io::Write;

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:5000")?;
    let (mut stream, _) = listener.accept()?;
    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    
    // read_line blocks until it encounters a newline (\n) or EOF
    while reader.read_line(&mut line)? > 0 {
        print!("Server received: {}", line);
        line.clear(); // Clear the string buffer for the next iteration
    }
    Ok(())
}