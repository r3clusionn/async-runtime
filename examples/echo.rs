//! A TCP echo server: one task per connection, each closed after 60 seconds of silence.
//!
//!     cargo run --release --example echo -- 127.0.0.1:7000

use std::time::Duration;

use strand::net::TcpListener;
use strand::time::timeout;

fn main() -> std::io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:7000".into());
    let rt = strand::Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind(&addr)?;
        println!("echoing on {}", listener.local_addr()?);
        loop {
            let (conn, peer) = listener.accept().await?;
            strand::spawn(async move {
                let mut buf = vec![0u8; 4096];
                loop {
                    match timeout(Duration::from_secs(60), conn.read(&mut buf)).await {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(n)) => {
                            if conn.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => {
                            println!("{peer}: idle for a minute, closing");
                            break;
                        }
                    }
                }
            });
        }
    })
}
