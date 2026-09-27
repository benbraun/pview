//! Read complete HTTP requests in loopback tests so connection teardown cannot
//! race an unread request body. Tests use socket deadlines rather than hanging.
use std::io::Read;
use std::net::TcpStream;
use std::time::Duration;

pub fn read_http_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 65536, "oversized request headers");
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
    }
    let headers = String::from_utf8(request.clone()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(length < 65536, "oversized request body");
    let header_length = request.len();
    request.resize(header_length + length, 0);
    stream.read_exact(&mut request[header_length..]).unwrap();
    String::from_utf8(request).unwrap()
}
