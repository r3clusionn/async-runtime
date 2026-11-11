//! TCP and UDP on the reactor.

use std::sync::Arc;
use std::time::Duration;

use strand::net::{TcpListener, TcpStream, UdpSocket};
use strand::time::{sleep, timeout};
use strand::{spawn, Builder, Runtime};

fn rt() -> Runtime {
    Builder::new().workers(4).build().unwrap()
}

async fn echo_server() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    spawn(async move {
        loop {
            let (s, _) = l.accept().await.unwrap();
            spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                loop {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if s.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

#[test]
fn echo() {
    rt().block_on(async {
        let addr = echo_server().await;
        let c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"hello reactor").await.unwrap();
        let mut buf = [0u8; 13];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello reactor");
    });
}

#[test]
fn many_clients_at_once() {
    rt().block_on(async {
        let addr = echo_server().await;
        let clients: Vec<_> = (0..200u32)
            .map(|id| {
                spawn(async move {
                    let c = TcpStream::connect(addr).await.unwrap();
                    c.set_nodelay(true).unwrap();
                    for i in 0..50u32 {
                        let msg = format!("{id}:{i};");
                        c.write_all(msg.as_bytes()).await.unwrap();
                        let mut back = vec![0u8; msg.len()];
                        c.read_exact(&mut back).await.unwrap();
                        assert_eq!(back, msg.as_bytes());
                    }
                })
            })
            .collect();
        for c in clients {
            c.await.unwrap();
        }
    });
}

#[test]
fn large_transfer_arrives_intact() {
    rt().block_on(async {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        const N: usize = 64 << 20;
        let data: Arc<Vec<u8>> = Arc::new((0..N).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect());
        let sent = data.clone();
        let tx = spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            s.write_all(&sent).await.unwrap();
            s.shutdown_write().unwrap();
        });
        let c = TcpStream::connect(addr).await.unwrap();
        let mut got = Vec::with_capacity(N);
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = c.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        tx.await.unwrap();
        assert_eq!(got.len(), N);
        assert!(got == *data, "bytes differ");
    });
}

#[test]
fn connection_refused_is_an_error() {
    rt().block_on(async {
        // Bind then drop, so the port is very likely closed.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let r = timeout(Duration::from_secs(10), TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("refusal is reported, not hung");
        let e = r.err().expect("nothing listens there");
        assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused, "{e}");
    });
}

#[test]
fn reading_after_the_peer_closes_its_side_gives_zero() {
    rt().block_on(async {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let server = spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            s.write_all(b"bye").await.unwrap();
            s.shutdown_write().unwrap();
            // Keep the socket open; only its writing side is closed.
            sleep(Duration::from_millis(200)).await;
        });
        let c = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 8];
        let n = c.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"bye");
        assert_eq!(c.read(&mut buf).await.unwrap(), 0);
        assert_eq!(c.read(&mut buf).await.unwrap(), 0, "and again");
        server.await.unwrap();
    });
}

#[test]
fn udp_round_trip() {
    rt().block_on(async {
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").unwrap();
        let (aa, ba) = (a.local_addr().unwrap(), b.local_addr().unwrap());
        let echo = spawn(async move {
            let mut buf = [0u8; 1500];
            for _ in 0..100 {
                let (n, from) = b.recv_from(&mut buf).await.unwrap();
                b.send_to(&buf[..n], from).await.unwrap();
            }
        });
        let mut buf = [0u8; 1500];
        for i in 0..100u32 {
            let msg = i.to_le_bytes();
            a.send_to(&msg, ba).await.unwrap();
            let (n, from) = a.recv_from(&mut buf).await.unwrap();
            assert_eq!((&buf[..n], from), (&msg[..], ba));
        }
        echo.await.unwrap();
        let _ = aa;
    });
}

#[test]
fn aborting_a_task_blocked_in_a_read_closes_cleanly() {
    rt().block_on(async {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let reader = spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 16];
            let _ = s.read(&mut buf).await; // never gets data
        });
        let c = TcpStream::connect(addr).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        // The server side was dropped with its pending poll; the client sees the close.
        let mut buf = [0u8; 4];
        let n = timeout(Duration::from_secs(5), c.read(&mut buf)).await.expect("close seen");
        assert!(matches!(n, Ok(0) | Err(_)));
        // And the reactor still works for new sockets.
        let addr = echo_server().await;
        let c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"ok").await.unwrap();
        let mut b2 = [0u8; 2];
        c.read_exact(&mut b2).await.unwrap();
        assert_eq!(&b2, b"ok");
    });
}

/// One task writes while another reads the same stream: separate wakers per direction, and a
/// write that waits for room while a read is pending.
#[test]
fn read_and_write_one_stream_from_two_tasks() {
    rt().block_on(async {
        let addr = echo_server().await;
        let c = Arc::new(TcpStream::connect(addr).await.unwrap());
        const N: usize = 32 << 20;
        let w = c.clone();
        let writer = spawn(async move {
            let chunk = vec![0x5au8; 1 << 16];
            let mut sent = 0;
            while sent < N {
                w.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
        });
        let mut got = 0;
        let mut buf = vec![0u8; 1 << 16];
        while got < N {
            let n = c.read(&mut buf).await.unwrap();
            assert!(buf[..n].iter().all(|&b| b == 0x5a));
            got += n;
        }
        writer.await.unwrap();
        assert_eq!(got, N);
    });
}
