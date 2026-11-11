//! TCP and UDP sockets on the runtime's reactor.
//!
//! The sockets are the standard library's, switched to non-blocking mode; the reactor tells a
//! task when to try a call again. Methods take `&self`, so one task can read while another
//! writes the same stream (wrap it in an `Arc`).

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};

use crate::io::{Direction, Registration};

#[cfg(windows)]
fn raw<T: std::os::windows::io::AsRawSocket>(s: &T) -> crate::io::RawSource {
    s.as_raw_socket()
}

#[cfg(unix)]
fn raw<T: std::os::fd::AsRawFd>(s: &T) -> crate::io::RawSource {
    s.as_raw_fd()
}

fn one_addr(addr: impl ToSocketAddrs) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?.next().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address to use"))
}

/// A TCP socket listening for connections.
pub struct TcpListener {
    // Field order matters: the registration goes before the socket closes.
    reg: Registration,
    std: std::net::TcpListener,
}

impl TcpListener {
    /// Binds to `addr`. Address resolution, if any, blocks.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<TcpListener> {
        let std = std::net::TcpListener::bind(one_addr(addr)?)?;
        TcpListener::from_std(std)
    }

    pub fn from_std(std: std::net::TcpListener) -> io::Result<TcpListener> {
        std.set_nonblocking(true)?;
        Ok(TcpListener { reg: Registration::new(raw(&std))?, std })
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (s, addr) = std::future::poll_fn(|cx| self.reg.poll_io(cx, Direction::Read, || self.std.accept())).await?;
        Ok((TcpStream::from_std(s)?, addr))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.std.local_addr()
    }
}

/// A TCP connection.
pub struct TcpStream {
    reg: Registration,
    std: std::net::TcpStream,
}

impl TcpStream {
    /// Connects without blocking a worker: the connection is started, and the task waits for
    /// the socket to become writable (connected) or report an error.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<TcpStream> {
        let addr = one_addr(addr)?;
        let sock = socket2::Socket::new(socket2::Domain::for_address(addr), socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        sock.set_nonblocking(true)?;
        match sock.connect(&addr.into()) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.raw_os_error() == in_progress() => {}
            Err(e) => return Err(e),
        }
        let std: std::net::TcpStream = sock.into();
        let stream = TcpStream { reg: Registration::new(raw(&std))?, std };
        stream.reg.ready(Direction::Write).await;
        if let Some(e) = stream.std.take_error()? {
            return Err(e);
        }
        // Writable but not connected happens when the connection failed without an error code.
        stream.std.peer_addr()?;
        Ok(stream)
    }

    pub fn from_std(std: std::net::TcpStream) -> io::Result<TcpStream> {
        std.set_nonblocking(true)?;
        Ok(TcpStream { reg: Registration::new(raw(&std))?, std })
    }

    /// Reads what is available, at least one byte; 0 means the peer closed its side.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.reg.poll_io(cx, Direction::Read, || (&self.std).read(buf))).await
    }

    /// Reads until `buf` is full; an early end of stream is an `UnexpectedEof` error.
    pub async fn read_exact(&self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read(buf).await? {
                0 => return Err(io::ErrorKind::UnexpectedEof.into()),
                n => buf = &mut buf[n..],
            }
        }
        Ok(())
    }

    /// Writes some of `buf` and returns how much.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.reg.poll_io(cx, Direction::Write, || (&self.std).write(buf))).await
    }

    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write(buf).await? {
                0 => return Err(io::ErrorKind::WriteZero.into()),
                n => buf = &buf[n..],
            }
        }
        Ok(())
    }

    /// Closes the writing side; the peer then reads 0.
    pub fn shutdown_write(&self) -> io::Result<()> {
        self.std.shutdown(Shutdown::Write)
    }

    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        self.std.set_nodelay(on)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.std.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.std.peer_addr()
    }
}

#[cfg(windows)]
fn in_progress() -> Option<i32> {
    Some(10036) // WSAEINPROGRESS
}

#[cfg(unix)]
fn in_progress() -> Option<i32> {
    Some(libc::EINPROGRESS)
}

/// A UDP socket.
pub struct UdpSocket {
    reg: Registration,
    std: std::net::UdpSocket,
}

impl UdpSocket {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<UdpSocket> {
        let std = std::net::UdpSocket::bind(one_addr(addr)?)?;
        std.set_nonblocking(true)?;
        Ok(UdpSocket { reg: Registration::new(raw(&std))?, std })
    }

    pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.reg.poll_io(cx, Direction::Write, || self.std.send_to(buf, target))).await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::poll_fn(|cx| self.reg.poll_io(cx, Direction::Read, || self.std.recv_from(buf))).await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.std.local_addr()
    }
}
