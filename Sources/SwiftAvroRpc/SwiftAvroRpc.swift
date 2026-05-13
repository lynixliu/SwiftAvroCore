//
//  SwiftAvroRpc.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 06/04/2026.
//

#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif
import Foundation
import SwiftAvroCore
@preconcurrency import NIO
import NIOSSL

// MARK: - AvroTLSConfig

/// Wraps an `NIOSSLContext` for optional TLS on HTTP servers and clients.
///
/// Pass an `AvroTLSConfig` to ``AvroIPCHTTPServerConfig`` or ``AvroIPCHTTPClientConfig``
/// to upgrade a plain-HTTP transport to HTTPS. Omit it (leave `nil`) for plain HTTP.
public struct AvroTLSConfig: @unchecked Sendable {
    let sslContext: NIOSSLContext

    init(sslContext: NIOSSLContext) { self.sslContext = sslContext }

    /// TLS for an HTTPS **client** using the system trust store.
    public static func client() throws -> AvroTLSConfig {
        try .init(sslContext: NIOSSLContext(configuration: .makeClientConfiguration()))
    }

    /// TLS for an HTTPS **server** with a PEM certificate and private key.
    /// - Parameters:
    ///   - certificateFile: Path to the PEM certificate chain file.
    ///   - privateKeyFile:  Path to the PEM private key file.
    public static func server(
        certificateFile: String,
        privateKeyFile:  String
    ) throws -> AvroTLSConfig {
        let certs = try NIOSSLCertificate.fromPEMFile(certificateFile)
        let key   = try NIOSSLPrivateKey(file: privateKeyFile, format: .pem)
        let cfg   = TLSConfiguration.makeServerConfiguration(
            certificateChain: certs.map { .certificate($0) },
            privateKey:       .privateKey(key)
        )
        return try .init(sslContext: NIOSSLContext(configuration: cfg))
    }
}

// MARK: - AvroIPCHTTPServerConfig

/// Configuration for an Avro IPC server that speaks HTTP (or HTTPS).
public struct AvroIPCHTTPServerConfig: Sendable {
    public let host:           String
    public let port:           Int
    /// URL path the server listens on. Defaults to `"/"`.
    public let path:           String
    public let context:        AvroIPCContext
    public let serverHash:     MD5Hash
    public let serverProtocol: String
    public let handler:        any AvroIPCHandler
    /// Non-nil enables TLS (HTTPS). `nil` runs plain HTTP.
    public let tls:            AvroTLSConfig?

    public init(
        host:           String = "0.0.0.0",
        port:           Int,
        path:           String = "/",
        context:        AvroIPCContext,
        serverHash:     MD5Hash,
        serverProtocol: String,
        handler:        any AvroIPCHandler,
        tls:            AvroTLSConfig? = nil
    ) {
        self.host           = host
        self.port           = port
        self.path           = path
        self.context        = context
        self.serverHash     = serverHash
        self.serverProtocol = serverProtocol
        self.handler        = handler
        self.tls            = tls
    }
}

// MARK: - AvroIPCHTTPClientConfig

/// Configuration for an Avro IPC client that speaks HTTP (or HTTPS).
public struct AvroIPCHTTPClientConfig: Sendable {
    public let host:           String
    public let port:           Int
    /// URL path to POST to. Defaults to `"/"`.
    public let path:           String
    public let context:        AvroIPCContext
    public let clientHash:     MD5Hash
    public let clientProtocol: String
    public let serverHash:     MD5Hash
    /// Non-nil enables TLS (HTTPS). `nil` runs plain HTTP.
    public let tls:            AvroTLSConfig?

    public init(
        host:           String,
        port:           Int,
        path:           String = "/",
        context:        AvroIPCContext,
        clientHash:     MD5Hash,
        clientProtocol: String,
        serverHash:     MD5Hash,
        tls:            AvroTLSConfig? = nil
    ) {
        self.host           = host
        self.port           = port
        self.path           = path
        self.context        = context
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
        self.serverHash     = serverHash
        self.tls            = tls
    }
}

// MARK: - AvroIPCServerConfig

/// Configuration for an Avro IPC server.
///
/// Pass any ``AvroIPCServerTransport`` as `transport` — use ``TCPTransport`` for
/// standard TCP or supply a custom implementation (e.g. Unix domain socket) from
/// the call site.
public struct AvroIPCServerConfig: Sendable {

    /// Determines how the server binds to a local address.
    public let transport: any AvroIPCServerTransport

    /// The shared IPC context holding pre-parsed handshake schemas and metadata.
    public let context: AvroIPCContext

    /// The 16-byte MD5 hash identifying this server's protocol.
    public let serverHash: MD5Hash

    /// The Avro protocol JSON string this server implements.
    public let serverProtocol: String

    /// The application-level handler that processes incoming RPC calls.
    public let handler: any AvroIPCHandler

    public init(
        transport: any AvroIPCServerTransport,
        context: AvroIPCContext,
        serverHash: MD5Hash,
        serverProtocol: String,
        handler: any AvroIPCHandler
    ) {
        self.transport      = transport
        self.context        = context
        self.serverHash     = serverHash
        self.serverProtocol = serverProtocol
        self.handler        = handler
    }
}

// MARK: - AvroIPCClientConfig

/// Configuration for an Avro IPC client.
///
/// Pass any ``AvroIPCClientTransport`` as `transport` — use ``TCPTransport`` for
/// standard TCP or supply a custom implementation (e.g. Unix domain socket) from
/// the call site.
public struct AvroIPCClientConfig: Sendable {

    /// Determines how the client connects to the remote address.
    public let transport: any AvroIPCClientTransport

    /// The shared IPC context holding pre-parsed handshake schemas and metadata.
    public let context: AvroIPCContext

    /// The 16-byte MD5 hash identifying this client's protocol.
    public let clientHash: MD5Hash

    /// The Avro protocol JSON string this client speaks.
    public let clientProtocol: String

    /// The expected 16-byte MD5 hash of the server's protocol.
    public let serverHash: MD5Hash

    public init(
        transport: any AvroIPCClientTransport,
        context: AvroIPCContext,
        clientHash: MD5Hash,
        clientProtocol: String,
        serverHash: MD5Hash
    ) {
        self.transport      = transport
        self.context        = context
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
        self.serverHash     = serverHash
    }
}

// MARK: - SwiftAvroRpc

/// The main entry point for the SwiftAvroRpc package.
///
/// `SwiftAvroRpc` is an actor that manages the NIO event loop group and acts as
/// a factory for object container files, IPC servers, and IPC clients. Its actor
/// isolation ensures that the event loop group lifecycle — start and stop — is
/// never accessed concurrently.
///
/// Typical lifecycle:
///
///     let rpc    = SwiftAvroRpc()
///     let server = try await rpc.makeServer(serverConfig)
///     let client = try await rpc.makeClient(clientConfig)
///     // ... use client and server ...
///     try await client.disconnect()
///     try await server.close()
///     await rpc.stop()
public actor SwiftAvroRpc {

    // MARK: - State

    private enum State {
        case running
        case stopped
    }

    private let eventLoopGroup: MultiThreadedEventLoopGroup
    private var state: State = .running

    // MARK: - Init

    /// Creates a `SwiftAvroRpc` actor with a managed NIO event loop group.
    /// The group starts immediately and runs until ``stop()`` is called.
    /// - Parameter threads: Number of event loop threads. Defaults to the number of CPU cores.
    public init(threads: Int = System.coreCount) {
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
    }

    // MARK: - Object Container File

    /// Creates an ``AvroFileObjectContainer`` actor for reading and writing Avro container files.
    ///
    /// The container is safe to use from concurrent Swift code — all read and write
    /// operations are actor-isolated. Supported codecs are `null`, `deflate`, `xz`,
    /// `lz4`, and `lzfse`.
    ///
    /// - Parameters:
    ///   - schema: A valid Avro JSON schema string describing the record type.
    ///   - codecName: The compression codec to use. Defaults to `null` (no compression).
    ///   - syncMarker: 16-byte sync marker for the container. Required for writing.
    /// - Throws: ``AvroContainerError`` if the schema is invalid or the codec is unsupported.
    /// - Returns: A ready-to-use container actor.
    public func makeContainer(
        schema: String,
        codecName: String = AvroReservedConstants.nullCodec,
        syncMarker: [UInt8] = (0..<16).map { _ in UInt8.random(in: 0...255) }
    ) throws -> AvroFileObjectContainer {
        try guardRunning()
        let codec = try CodecFactory.make(named: codecName)
        return try AvroFileObjectContainer(schema: schema, codec: codec, syncMarker: syncMarker)
    }

    // MARK: - IPC Context

    /// Creates an ``AvroIPCContext`` that holds pre-parsed handshake schemas and metadata.
    ///
    /// The context is immutable and shared across the client and server for the lifetime
    /// of the application. Pass it into ``AvroIPCServerConfig`` or ``AvroIPCClientConfig``.
    ///
    /// - Parameters:
    ///   - requestMeta: Metadata to include in outgoing handshake requests.
    ///   - responseMeta: Metadata to include in outgoing handshake responses.
    ///   - knownProtocols: Optional set of recognised protocol JSON strings. When non-nil,
    ///     ``AvroIPCRequest`` and ``AvroIPCResponse`` validate against this set and throw
    ///     on unknown protocols. Useful for closed deployments.
    public func makeIPCContext(
        requestMeta:    [String: [UInt8]] = [:],
        responseMeta:   [String: [UInt8]] = [:],
        knownProtocols: Set<String>?      = nil
    ) throws -> AvroIPCContext {
        try guardRunning()
        return try AvroIPCContext.standard(
            avro:           Avro(),
            requestMeta:    requestMeta,
            responseMeta:   responseMeta,
            knownProtocols: knownProtocols
        )
    }

    // MARK: - Utilities

    /// Returns the 16-byte MD5 hash of the given Avro protocol JSON string.
    ///
    /// Use this to produce the `clientHash` / `serverHash` values required by
    /// ``AvroIPCClientConfig`` and ``AvroIPCServerConfig`` without depending on
    /// an external MD5 library.
    ///
    ///     let hash = SwiftAvroRpc.md5Hash(of: myProtocolJSON)
    public static func md5Hash(of protocolJSON: String) -> MD5Hash {
        var hasher = Insecure.MD5()
        hasher.update(data: Data(protocolJSON.utf8))
        return Array(hasher.finalize())
    }

    // MARK: - IPC Server

    /// Binds an Avro IPC server using the given configuration and returns
    /// an ``AvroServerChannel`` representing the bound port.
    ///
    /// Each accepted TCP connection receives its own ``AvroIPCServer`` actor instance,
    /// so handshake state and session cache are fully isolated per connection.
    ///
    /// - Parameter config: Server configuration including host, port, context, and handler.
    /// - Returns: An ``AvroServerChannel`` that can be closed when the server should stop.
    @discardableResult
    public func makeServer(_ config: AvroIPCServerConfig) async throws -> AvroServerChannel {
        try guardRunning()
        let channel = try await AvroIPCServerBootstrap(
            eventLoopGroup: eventLoopGroup,
            context: config.context,
            serverHash: config.serverHash,
            serverProtocol: config.serverProtocol,
            handler: config.handler
        ).bind(using: config.transport)
        return AvroServerChannel(channel: channel)
    }

    // MARK: - IPC Client

    /// Creates and returns a connected ``AvroIPCClient`` using the given configuration.
    ///
    /// The client connects to the server immediately. The Avro IPC handshake is performed
    /// transparently on the first `call` — no separate handshake step is required.
    /// Session state is tracked in a per-client ``ClientSessionCache`` actor.
    ///
    /// - Parameter config: Client configuration including host, port, context, and protocol hashes.
    /// - Returns: A connected ``AvroIPCClient`` ready to send RPC calls.
    public func makeClient(_ config: AvroIPCClientConfig) async throws -> AvroIPCClient {
        try guardRunning()
        let client = try AvroIPCClient(
            context: config.context,
            clientHash: config.clientHash,
            clientProtocol: config.clientProtocol,
            serverHash: config.serverHash
        )
        try await client.connect(using: config.transport, eventLoopGroup: eventLoopGroup)
        return client
    }

    // MARK: - HTTP IPC Server

    /// Binds an Avro IPC server that accepts requests over HTTP (or HTTPS when `config.tls`
    /// is set). All connections share a single ``AvroIPCServer`` actor so the handshake
    /// session cache persists across requests without per-request re-negotiation.
    ///
    /// - Parameter config: HTTP server configuration including host, port, handler, and optional TLS.
    /// - Returns: An ``AvroServerChannel`` that can be closed when the server should stop.
    @discardableResult
    public func makeHTTPServer(_ config: AvroIPCHTTPServerConfig) async throws -> AvroServerChannel {
        try guardRunning()
        let channel = try await AvroIPCHTTPServerBootstrap(
            eventLoopGroup: eventLoopGroup,
            context:        config.context,
            serverHash:     config.serverHash,
            serverProtocol: config.serverProtocol,
            handler:        config.handler,
            tlsContext:     config.tls?.sslContext
        ).bind(host: config.host, port: config.port)
        return AvroServerChannel(channel: channel)
    }

    // MARK: - HTTP IPC Client

    /// Creates an Avro IPC client that sends each RPC call as an HTTP POST (or HTTPS when
    /// `config.tls` is set). The handshake is performed transparently on the first call.
    ///
    /// Unlike the TCP client, the HTTP client opens a new connection per call, so no
    /// explicit `disconnect()` is required.
    ///
    /// - Parameter config: HTTP client configuration including host, port, and optional TLS.
    /// - Returns: A ready-to-use ``AvroIPCHTTPClient``.
    public func makeHTTPClient(_ config: AvroIPCHTTPClientConfig) throws -> AvroIPCHTTPClient {
        try guardRunning()
        return try AvroIPCHTTPClient(
            context:        config.context,
            clientHash:     config.clientHash,
            clientProtocol: config.clientProtocol,
            serverHash:     config.serverHash,
            host:           config.host,
            port:           config.port,
            path:           config.path,
            eventLoopGroup: eventLoopGroup,
            tlsContext:     config.tls?.sslContext
        )
    }

    // MARK: - Lifecycle

    /// Stops the managed NIO event loop group and releases all network resources.
    ///
    /// Call this once all clients have disconnected and all server channels have been closed.
    /// After `stop()` returns, no further factory methods may be called on this instance.
    /// Calling `stop()` more than once is safe — subsequent calls are no-ops.
    public func stop() async throws {
        guard case .running = state else { return }
        state = .stopped
        try await eventLoopGroup.shutdownGracefully()
    }

    // MARK: - Private helpers

    private func guardRunning() throws {
        guard case .running = state else {
            throw AvroIPCError.connectionClosed
        }
    }
}
