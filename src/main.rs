#![allow(unused_imports)]
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

pub fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    match stream.read(&mut buf[..]) {
        Ok(n) => println!("Read {n} bytes."),
        Err(err) => println!("Error reading stream: {}", err),
    }
    let buf = BytesMut::from(&buf[..]);
    let request = KRequest::parse(buf.freeze());
    let response = KResponse::new(request.size, request.correlation_id);
    let _ = stream.write(&response.compact());
}

#[derive(Debug)]
struct KRequest {
    size: Bytes,
    _api_key: Bytes,
    _api_version: Bytes,
    correlation_id: Bytes,
}

impl KRequest {
    pub fn parse(buf: Bytes) -> KRequest {
        let size = buf.slice(..4);
        let api_key = buf.slice(4..6);
        let api_version = buf.slice(6..8);
        let correlation_id = buf.slice(8..12);
        KRequest {
            size,
            _api_key: api_key,
            _api_version: api_version,
            correlation_id,
        }
    }
}

#[derive(Debug)]
pub struct KResponse {
    msg_size: Bytes,
    header: Bytes,
}

impl KResponse {
    pub fn new(msg_size: Bytes, header: Bytes) -> Self {
        Self { msg_size, header }
    }

    pub fn compact(self) -> Bytes {
        let mut result = BytesMut::new();
        result.put(self.msg_size);
        result.put(self.header);
        result.freeze()
    }
}
