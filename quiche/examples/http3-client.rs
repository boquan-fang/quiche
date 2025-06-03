// Copyright (C) 2019, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#[macro_use]
extern crate log;

use quiche::h3::NameValue;

const MAX_DATAGRAM_SIZE: usize = 1350;

fn main() {
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let mut args = std::env::args();

    let cmd = &args.next().unwrap();

    if args.len() != 1 {
        println!("Usage: {cmd} URL");
        println!("\nSee tools/apps/ for more complete implementations.");
        return;
    }

    let url = url::Url::parse(&args.next().unwrap()).unwrap();

    // Setup the event loop.
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);

    // Resolve server address.
    let peer_addr = url.socket_addrs(|| None).unwrap()[0];

    // Bind to INADDR_ANY or IN6ADDR_ANY depending on the IP family of the
    // server address. This is needed on macOS and BSD variants that don't
    // support binding to IN6ADDR_ANY for both v4 and v6.
    let bind_addr = match peer_addr {
        std::net::SocketAddr::V4(_) => "0.0.0.0:0",
        std::net::SocketAddr::V6(_) => "[::]:0",
    };

    // Create the UDP socket backing the QUIC connection, and register it with
    // the event loop.
    let mut socket =
        mio::net::UdpSocket::bind(bind_addr.parse().unwrap()).unwrap();
    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)
        .unwrap();

    // Create the configuration for the QUIC connection.
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    // *CAUTION*: this should not be set to `false` in production!!!
    config.verify_peer(false);

    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();

    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    // Enable active migration
    config.set_disable_active_migration(false);
    // Set the maximum number of active connection IDs
    config.set_active_connection_id_limit(2);

    let mut http3_conn = None;

    // Create a second socket for migration
    let mut migration_socket = None;
    let mut migration_local_addr = None;
    let mut migration_attempted = false;

    // Use a zero-length source connection ID as requested
    let scid = quiche::ConnectionId::from_ref(&[]);

    // Get local address.
    let local_addr = socket.local_addr().unwrap();

    // Create a QUIC connection and initiate handshake.
    let mut conn =
        quiche::connect(url.domain(), &scid, local_addr, peer_addr, &mut config)
            .unwrap();

    info!(
        "connecting to {:} from {:} with scid {}",
        peer_addr,
        socket.local_addr().unwrap(),
        hex_dump(&scid)
    );

    let (write, send_info) = conn.send(&mut out).expect("initial send failed");

    while let Err(e) = socket.send_to(&out[..write], send_info.to) {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            debug!("send() would block");
            continue;
        }

        panic!("send() failed: {:?}", e);
    }

    debug!("written {}", write);

    let h3_config = quiche::h3::Config::new().unwrap();

    // Prepare request.
    let mut path = String::from(url.path());

    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }

    let req = vec![
        quiche::h3::Header::new(b":method", b"GET"),
        quiche::h3::Header::new(b":scheme", url.scheme().as_bytes()),
        quiche::h3::Header::new(
            b":authority",
            url.host_str().unwrap().as_bytes(),
        ),
        quiche::h3::Header::new(b":path", path.as_bytes()),
        quiche::h3::Header::new(b"user-agent", b"quiche"),
    ];

    let req_start = std::time::Instant::now();

    let mut req_sent = false;

    loop {
        poll.poll(&mut events, conn.timeout()).unwrap();

        // Read incoming UDP packets from the sockets and feed them to quiche,
        // until there are no more packets to read.
        'read: loop {
            // If the event loop reported no events, it means that the timeout
            // has expired, so handle it without attempting to read packets. We
            // will then proceed with the send loop.
            if events.is_empty() {
                debug!("timed out");

                conn.on_timeout();

                break 'read;
            }

            // Process events for all sockets
            for event in &events {
                let socket_to_use = match event.token() {
                    mio::Token(0) => &socket,
                    mio::Token(1) => {
                        if let Some(ref sock) = migration_socket {
                            sock
                        } else {
                            continue;
                        }
                    },
                    _ => continue,
                };

                let socket_local_addr = socket_to_use.local_addr().unwrap();

                // Try to receive data from this socket
                let (len, from) = match socket_to_use.recv_from(&mut buf) {
                    Ok(v) => v,

                    Err(e) => {
                        // There are no more UDP packets to read, so try the next
                        // socket
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!(
                                "recv() would block on socket {}",
                                event.token().0
                            );
                            continue;
                        }

                        error!(
                            "recv() failed on socket {}: {:?}",
                            event.token().0,
                            e
                        );
                        continue;
                    },
                };

                debug!("got {} bytes on socket {}", len, event.token().0);

                let recv_info = quiche::RecvInfo {
                    to: socket_local_addr,
                    from,
                };

                // Process potentially coalesced packets.
                let read = match conn.recv(&mut buf[..len], recv_info) {
                    Ok(v) => v,

                    Err(e) => {
                        error!("recv failed: {:?}", e);
                        continue;
                    },
                };

                debug!(
                    "processed {} bytes from socket {}",
                    read,
                    event.token().0
                );
            }

            // Break out of the read loop after processing all events
            break 'read;
        }

        debug!("done reading");

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            break;
        }

        // Create a new HTTP/3 connection once the QUIC connection is established.
        if conn.is_established() && http3_conn.is_none() {
            http3_conn = Some(
                quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                .expect("Unable to create HTTP/3 connection, check the server's uni stream limit and window size"),
            );

            // Create a second socket for migration if not already created
            if migration_socket.is_none() {
                // Bind to a different port
                let migration_bind_addr = match peer_addr {
                    std::net::SocketAddr::V4(_) => "0.0.0.0:0",
                    std::net::SocketAddr::V6(_) => "[::]:0",
                };

                let mut second_socket = mio::net::UdpSocket::bind(
                    migration_bind_addr.parse().unwrap(),
                )
                .unwrap();
                poll.registry()
                    .register(
                        &mut second_socket,
                        mio::Token(1),
                        mio::Interest::READABLE,
                    )
                    .unwrap();

                migration_local_addr = Some(second_socket.local_addr().unwrap());
                migration_socket = Some(second_socket);

                info!(
                    "Created migration socket at {:?}",
                    migration_local_addr.unwrap()
                );
            }
        }

        // Send HTTP requests once the QUIC connection is established, and until
        // all requests have been sent.
        if let Some(h3_conn) = &mut http3_conn {
            if !req_sent {
                info!("sending HTTP request {:?}", req);

                h3_conn.send_request(&mut conn, &req, true).unwrap();

                req_sent = true;
            }
        }

        // Handle path events for connection migration
        while let Some(path_event) = conn.path_event_next() {
            match path_event {
                quiche::PathEvent::New(local_addr, peer_addr) => {
                    info!("New path ({}, {}) available", local_addr, peer_addr);
                },

                quiche::PathEvent::Validated(local_addr, peer_addr) => {
                    info!(
                        "Path ({}, {}) has been validated",
                        local_addr, peer_addr
                    );

                    // Migrate to the validated path
                    if let Err(e) = conn.migrate(local_addr, peer_addr) {
                        error!("Failed to migrate: {:?}", e);
                    } else {
                        info!(
                            "Successfully migrated to new path ({}, {})",
                            local_addr, peer_addr
                        );
                    }
                },

                quiche::PathEvent::FailedValidation(local_addr, peer_addr) => {
                    error!(
                        "Path validation failed for ({}, {})",
                        local_addr, peer_addr
                    );
                },

                quiche::PathEvent::Closed(local_addr, peer_addr) => {
                    info!("Path ({}, {}) is now closed", local_addr, peer_addr);
                },

                quiche::PathEvent::ReusedSourceConnectionId(
                    seq,
                    old_path,
                    new_path,
                ) => {
                    info!(
                        "Source CID with seq {} reused from {:?} on {:?}",
                        seq, old_path, new_path
                    );
                },

                quiche::PathEvent::PeerMigrated(local_addr, peer_addr) => {
                    info!("Peer migrated to ({}, {})", local_addr, peer_addr);
                },
            }
        }

        // Attempt connection migration after the connection is established
        // and we have a migration socket, but only once
        if conn.is_established()
            && migration_socket.is_some()
            && !migration_attempted
            && conn.available_dcids() > 0
        {
            let local_addr = migration_local_addr.unwrap();

            info!("Probing path from {} to {}", local_addr, peer_addr);

            if let Err(e) = conn.probe_path(local_addr, peer_addr) {
                error!("Failed to probe path: {:?}", e);
            } else {
                info!(
                    "Path probe initiated from {} to {}",
                    local_addr, peer_addr
                );
                migration_attempted = true;
            }
        }

        if let Some(http3_conn) = &mut http3_conn {
            // Process HTTP/3 events.
            loop {
                match http3_conn.poll(&mut conn) {
                    Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                        info!(
                            "got response headers {:?} on stream id {}",
                            hdrs_to_strings(&list),
                            stream_id
                        );
                    },

                    Ok((stream_id, quiche::h3::Event::Data)) => {
                        while let Ok(read) =
                            http3_conn.recv_body(&mut conn, stream_id, &mut buf)
                        {
                            debug!(
                                "got {} bytes of response data on stream {}",
                                read, stream_id
                            );

                            print!("{}", unsafe {
                                std::str::from_utf8_unchecked(&buf[..read])
                            });
                        }
                    },

                    Ok((_stream_id, quiche::h3::Event::Finished)) => {
                        info!(
                            "response received in {:?}, closing...",
                            req_start.elapsed()
                        );

                        conn.close(true, 0x100, b"kthxbye").unwrap();
                    },

                    Ok((_stream_id, quiche::h3::Event::Reset(e))) => {
                        error!(
                            "request was reset by peer with {}, closing...",
                            e
                        );

                        conn.close(true, 0x100, b"kthxbye").unwrap();
                    },

                    Ok((_, quiche::h3::Event::PriorityUpdate)) => unreachable!(),

                    Ok((goaway_id, quiche::h3::Event::GoAway)) => {
                        info!("GOAWAY id={}", goaway_id);
                    },

                    Err(quiche::h3::Error::Done) => {
                        break;
                    },

                    Err(e) => {
                        error!("HTTP/3 processing failed: {:?}", e);

                        break;
                    },
                }
            }
        }

        // Generate outgoing QUIC packets and send them on the appropriate socket
        // based on the local address, until quiche reports no more packets to be
        // sent.

        // First handle the main socket
        let local_addr = socket.local_addr().unwrap();
        for peer_addr in conn.paths_iter(local_addr) {
            loop {
                let (write, send_info) = match conn.send_on_path(
                    &mut out,
                    Some(local_addr),
                    Some(peer_addr),
                ) {
                    Ok(v) => v,

                    Err(quiche::Error::Done) => {
                        debug!(
                            "done writing on path {} -> {}",
                            local_addr, peer_addr
                        );
                        break;
                    },

                    Err(e) => {
                        error!(
                            "send failed on path {} -> {}: {:?}",
                            local_addr, peer_addr, e
                        );
                        break;
                    },
                };

                if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        debug!("send() would block");
                        break;
                    }

                    error!("send() failed: {:?}", e);
                    break;
                }

                debug!(
                    "written {} bytes from {} to {}",
                    write, local_addr, send_info.to
                );
            }
        }

        // Then handle the migration socket if it exists
        if let Some(ref sock) = migration_socket {
            let local_addr = sock.local_addr().unwrap();

            for peer_addr in conn.paths_iter(local_addr) {
                loop {
                    let (write, send_info) = match conn.send_on_path(
                        &mut out,
                        Some(local_addr),
                        Some(peer_addr),
                    ) {
                        Ok(v) => v,

                        Err(quiche::Error::Done) => {
                            debug!(
                                "done writing on path {} -> {}",
                                local_addr, peer_addr
                            );
                            break;
                        },

                        Err(e) => {
                            error!(
                                "send failed on path {} -> {}: {:?}",
                                local_addr, peer_addr, e
                            );
                            break;
                        },
                    };

                    if let Err(e) = sock.send_to(&out[..write], send_info.to) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("send() would block");
                            break;
                        }

                        error!("send() failed: {:?}", e);
                        break;
                    }

                    debug!(
                        "written {} bytes from {} to {}",
                        write, local_addr, send_info.to
                    );
                }
            }
        }

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            break;
        }
    }
}

fn hex_dump(buf: &[u8]) -> String {
    let vec: Vec<String> = buf.iter().map(|b| format!("{b:02x}")).collect();

    vec.join("")
}

pub fn hdrs_to_strings(hdrs: &[quiche::h3::Header]) -> Vec<(String, String)> {
    hdrs.iter()
        .map(|h| {
            let name = String::from_utf8_lossy(h.name()).to_string();
            let value = String::from_utf8_lossy(h.value()).to_string();

            (name, value)
        })
        .collect()
}
