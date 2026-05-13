//
//  RPC/Transport.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 11/05/2026.
//

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
