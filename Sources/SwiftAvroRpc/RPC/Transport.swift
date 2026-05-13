//
//  RPC/Transport.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 11/05/2026.
//  Copyright © 2026 Yang Liu.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import NIO

// MARK: - Transport protocols

/// Abstracts how a server binds to a local address.
/// Implement this protocol to support custom transports (e.g. Unix domain sockets).
public protocol AvroIPCServerTransport: Sendable {
    func bind(using bootstrap: ServerBootstrap) async throws -> any Channel
}

/// Abstracts how a client connects to a remote address.
/// Implement this protocol to support custom transports (e.g. Unix domain sockets).
public protocol AvroIPCClientTransport: Sendable {
    func connect(using bootstrap: ClientBootstrap) async throws -> any Channel
}

// MARK: - Built-in TCP transport

/// Standard TCP transport. The default when no custom transport is required.
public struct TCPTransport: AvroIPCServerTransport, AvroIPCClientTransport {

    public let host: String
    public let port: Int

    public init(host: String = "0.0.0.0", port: Int) {
        self.host = host
        self.port = port
    }

    public func bind(using bootstrap: ServerBootstrap) async throws -> any Channel {
        try await bootstrap.bind(host: host, port: port).get()
    }

    public func connect(using bootstrap: ClientBootstrap) async throws -> any Channel {
        try await bootstrap.connect(host: host, port: port).get()
    }
}

// MARK: - Built-in Unit Domain transport
/// Avro IPC transport over a Unix domain socket.
///
/// Uses the identical NIO pipeline as ``TCPTransport`` — only the socket
/// address differs. Suitable for intra-node IPC on macOS and Linux.
///
/// **Not available on iOS** — the app sandbox restricts cross-process
/// Unix domain socket usage. Use ``TCPTransport`` for inter-node calls from iOS.
public struct UnixDomainTransport: AvroIPCServerTransport, AvroIPCClientTransport {

    /// Filesystem path for the Unix domain socket (e.g. `/var/run/myservice/rpc.sock`).
    public let path: String

    public init(path: String) {
        self.path = path
    }

    public func bind(using bootstrap: ServerBootstrap) async throws -> any Channel {
        try await bootstrap.bind(unixDomainSocketPath: path).get()
    }

    public func connect(using bootstrap: ClientBootstrap) async throws -> any Channel {
        try await bootstrap.connect(unixDomainSocketPath: path).get()
    }
}
