#![allow(unused_imports)]
use std::io::Write;
use std::net::TcpStream;
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                handle_connection(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

pub fn handle_connection(mut stream: TcpStream) {
    let response: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 7];
    let _ = stream.write(&response);
}

#[derive(Debug)]
pub struct KResponse {
    message_size: i32,
    header: Header,
}

impl KResponse {
    pub fn new() -> Self {
        let header = Header { correlation_id: 7 };
        Self { message_size: 0, header }
    }
}

#[derive(Debug)]
struct Header {
    correlation_id: i32
}
