//! Shared TCP listener setup for the HTTP and MCP servers.

use std::net::SocketAddr;

/// Bind a TCP listener with platform-appropriate socket options.
///
/// Unix: `SO_REUSEADDR` lets us rebind past `TIME_WAIT` sockets from a
/// previous clean shutdown. It doesn't let two live listeners share a port
/// (that's `SO_REUSEPORT`), so port-conflict detection still works.
///
/// Windows: a plain bind already rebinds past `TIME_WAIT`, but it can be
/// hijacked by another same-user process binding the port with
/// `SO_REUSEADDR`. `SO_EXCLUSIVEADDRUSE` closes that hole, at the cost
/// that MSDN warns the port may not be rebindable while connections
/// accepted by a previous instance are still terminating.
///
/// A failed bind or listen keeps its `io::ErrorKind`, so a caller can tell a
/// taken port from any other failure without reading the OS's wording.
pub(crate) fn bind_exclusive(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::io::Error;

    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };

    let context = |what: String| move |e: Error| Error::new(e.kind(), format!("{what}: {e}"));

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(context("failed to create socket".into()))?;

    #[cfg(unix)]
    socket
        .set_reuse_address(true)
        .map_err(context("failed to set SO_REUSEADDR".into()))?;

    #[cfg(windows)]
    set_exclusiveaddruse(&socket)?;

    socket
        .set_nonblocking(true)
        .map_err(context("failed to set nonblocking".into()))?;
    socket
        .bind(&addr.into())
        .map_err(context(format!("failed to bind {addr}")))?;
    socket
        .listen(1024)
        .map_err(context(format!("failed to listen on {addr}")))?;

    Ok(std::net::TcpListener::from(socket))
}

/// Resolve `addr` and bind the first address that accepts a listener,
/// mirroring `tokio::net::TcpListener::bind` over a resolved list.
#[cfg(feature = "mcp-server")]
pub(crate) fn bind_exclusive_to(addr: &str) -> Result<std::net::TcpListener, String> {
    use std::net::ToSocketAddrs;

    let addrs = addr
        .to_socket_addrs()
        .map_err(|e| format!("invalid address {addr}: {e}"))?;
    let mut last_err = format!("no addresses resolved for {addr}");
    for candidate in addrs {
        match bind_exclusive(candidate) {
            Ok(listener) => return Ok(listener),
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(last_err)
}

/// Set `SO_EXCLUSIVEADDRUSE` on a socket. socket2 does not expose this
/// option, so it goes through raw `setsockopt`.
#[cfg(windows)]
fn set_exclusiveaddruse(socket: &socket2::Socket) -> std::io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        SO_EXCLUSIVEADDRUSE, SOCKET, SOL_SOCKET, setsockopt,
    };

    let enable: i32 = 1;
    // SAFETY: the socket handle is valid for the lifetime of `socket`, and
    // optval/optlen describe a single i32 that outlives the call.
    let rc = unsafe {
        setsockopt(
            socket.as_raw_socket() as SOCKET,
            SOL_SOCKET,
            SO_EXCLUSIVEADDRUSE,
            std::ptr::from_ref(&enable).cast::<u8>(),
            size_of_val(&enable) as i32,
        )
    };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        return Err(std::io::Error::new(
            e.kind(),
            format!("failed to set SO_EXCLUSIVEADDRUSE: {e}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpStream;
    use std::time::Duration;

    /// A restart immediately after a clean shutdown must not fail on the old
    /// connection's `TIME_WAIT` socket. Unix relies on `SO_REUSEADDR` for
    /// this. On Windows, MSDN warns an `SO_EXCLUSIVEADDRUSE` bind may fail
    /// while the previous connection is still settling, so this test has to
    /// run on Windows to mean anything.
    #[test]
    fn rebind_succeeds_past_time_wait() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = bind_exclusive(addr).unwrap();
        listener.set_nonblocking(false).unwrap();
        let bound = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(bound).unwrap();
        // A lost FIN should fail the test in seconds, not hang the suite.
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (accepted, _) = listener.accept().unwrap();

        // Server initiates the close so the server side owns the TIME_WAIT.
        drop(accepted);

        // Wait for the server's FIN, then close our side to complete the
        // shutdown handshake.
        let mut buf = [0u8; 1];
        assert_eq!(client.read(&mut buf).unwrap(), 0);
        drop(client);
        drop(listener);

        // Let the final ACK land so the old socket settles into TIME_WAIT.
        std::thread::sleep(Duration::from_millis(50));

        bind_exclusive(bound).expect("rebind past TIME_WAIT failed");
    }
}
