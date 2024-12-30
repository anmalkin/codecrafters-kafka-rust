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

fn handle_connection(mut stream: TcpStream) {
    let mut buf = BytesMut::zeroed(512);
    match stream.read(&mut buf[..]) {
        Ok(n) => println!("Read {n} bytes."),
        Err(err) => println!("Error reading stream: {}", err),
    }
    let req = KRequest::parse(&buf.freeze());
    let res = gen_res(&req);
    let _ = stream.write(&res.as_be_bytes());
}

fn gen_res(req: &KRequest) -> KResponse {
    match req.api_key {
        18 => {
            let msg_size = 0;
            let correlation_id = req.correlation_id;
            let err_code = match req.api_version {
                0..=4 => 0,
                _ => 35,
            };
            KResponse::new(msg_size, correlation_id, err_code)
        }
        key => panic!("API key {} not yet supported.", key),
    }
}

fn be_bytes_to_i32(bytes: &[u8]) -> i32 {
    if bytes.len() != std::mem::size_of::<i32>() {
        panic!("Cannot convert {} bytes into an i32.", bytes.len());
    }
    let mut arr: [u8; 4] = [0; 4];
    arr.copy_from_slice(bytes);
    i32::from_be_bytes(arr)
}

fn be_bytes_to_i16(bytes: &[u8]) -> i16 {
    if bytes.len() != std::mem::size_of::<i16>() {
        panic!("Cannot convert {} bytes into an i16.", bytes.len());
    }
    let mut arr: [u8; 2] = [0; 2];
    arr.copy_from_slice(bytes);
    i16::from_be_bytes(arr)
}

#[derive(Debug)]
struct KRequest {
    msg_size: i32,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
}

impl KRequest {
    fn parse(buf: &[u8]) -> KRequest {
        let msg_size = be_bytes_to_i32(&buf[..4]);
        let api_key = be_bytes_to_i16(&buf[4..6]);
        let api_version = be_bytes_to_i16(&buf[6..8]);
        let correlation_id = be_bytes_to_i32(&buf[8..12]);
        KRequest {
            msg_size,
            api_key,
            api_version,
            correlation_id,
        }
    }
}

#[derive(Debug)]
struct KResponse {
    msg_size: i32,
    correlation_id: i32,
    err_code: i16,
}

impl KResponse {
    fn new(msg_size: i32, correlation_id: i32, err_code: i16) -> Self {
        Self {
            msg_size,
            correlation_id,
            err_code,
        }
    }

    fn as_be_bytes(&self) -> Bytes {
        let mut result = BytesMut::new();
        result.put(self.msg_size.to_be_bytes().as_slice());
        result.put(self.correlation_id.to_be_bytes().as_slice());
        result.put(self.err_code.to_be_bytes().as_slice());
        result.freeze()
    }
}
