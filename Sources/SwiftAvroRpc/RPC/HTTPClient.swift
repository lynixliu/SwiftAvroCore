//
//  RPC/HTTPClient.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 09/05/2026.
//

import Foundation
import NIO
import NIOHTTP1
import NIOSSL
import SwiftAvroCore

// MARK: - AvroIPCHTTPClient

/// An Avro IPC client that sends each call as an HTTP POST.
///
/// Unlike the TCP client, each call opens a fresh connection, so no explicit
/// `disconnect()` is needed. The Avro IPC handshake is performed transparently
/// on the first call; subsequent calls reuse the accumulated session state.
///
/// Obtain an instance via ``SwiftAvroRpc/makeHTTPClient(_:)``.
public actor AvroIPCHTTPClient {

    private let request:          AvroIPCRequest
    private var session:          AvroIPCSession
    private var handshakeComplete = false
    private var serverHash:       MD5Hash

    private let host:           String
    private let port:           Int
    private let path:           String
    private let eventLoopGroup: EventLoopGroup
    private let tlsContext:     NIOSSLContext?

    // MARK: - Init

    init(
        context:        AvroIPCContext,
        clientHash:     MD5Hash,
        clientProtocol: String,
        serverHash:     MD5Hash,
        host:           String,
        port:           Int,
        path:           String,
        eventLoopGroup: EventLoopGroup,
        tlsContext:     NIOSSLContext? = nil
    ) throws {
        self.session    = AvroIPCSession(context: context)
        self.request    = try Avro().makeIPCRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            session:        session
        )
        self.serverHash     = serverHash
        self.host           = host
        self.port           = port
        self.path           = path
        self.eventLoopGroup = eventLoopGroup
        self.tlsContext     = tlsContext
    }

    // MARK: - HTTP POST

    /// Opens a new HTTP(S) connection, POSTs `body`, returns the complete response body.
    private func post(body: Data) async throws -> Data {
        let host = self.host
        let port = self.port
        let path = self.path
        let tls  = self.tlsContext
        let elg  = self.eventLoopGroup

        return try await withCheckedThrowingContinuation { continuation in
            let handler = AvroHTTPClientHandler(continuation: continuation)

            ClientBootstrap(group: elg)
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelInitializer { channel in
                    do {
                        if let tls {
                            let ssl = try NIOSSLClientHandler(context: tls, serverHostname: host)
                            try channel.pipeline.syncOperations.addHandler(ssl)
                        }
                        try channel.pipeline.syncOperations.addHandlers(
                            HTTPRequestEncoder(),
                            ByteToMessageHandler(HTTPResponseDecoder()),
                            handler
                        )
                        return channel.eventLoop.makeSucceededVoidFuture()
                    } catch {
                        return channel.eventLoop.makeFailedFuture(error)
                    }
                }
                .connect(host: host, port: port)
                .whenComplete { result in
                    switch result {
                    case .failure(let error):
                        // Pipeline was never set up; resume the continuation directly.
                        handler.fail(error)
                    case .success(let channel):
                        var headers = HTTPHeaders()
                        headers.add(name: "Host",           value: host)
                        headers.add(name: "Content-Type",   value: "avro/binary")
                        headers.add(name: "Content-Length", value: "\(body.count)")
                        headers.add(name: "Connection",     value: "close")
                        let head = HTTPRequestHead(
                            version: .http1_1,
                            method:  .POST,
                            uri:     path,
                            headers: headers
                        )
                        channel.write(HTTPClientRequestPart.head(head), promise: nil)
                        var buf = channel.allocator.buffer(capacity: body.count)
                        buf.writeBytes(body)
                        channel.write(HTTPClientRequestPart.body(.byteBuffer(buf)), promise: nil)
                        channel.writeAndFlush(HTTPClientRequestPart.end(nil), promise: nil)
                    }
                }
        }
    }

    // MARK: - Payload builder

    /// Encodes the handshake prefix + call body into one `Data` ready for framing.
    private func buildPayload<Req: Codable & Sendable>(
        messageName: String,
        parameters:  [Req]
    ) async throws -> Data {
        let avro = Avro()
        let handshakeData = try request.encodeHandshake(
            avro: avro, serverHash: serverHash, session: session
        )
        let callData = try await request.encodeCall(
            avro:        avro,
            messageName: messageName,
            parameters:  parameters,
            serverHash:  serverHash,
            session:     session
        )
        return handshakeData + callData
    }

    // MARK: - Handshake

    /// Two-round-trip IPC handshake over HTTP POST.
    private func performHandshake() async throws {
        let avro = Avro()

        // Round 1 — initial probe.
        let initialBytes = try request.encodeInitialHandshake(avro: avro, session: session)
        let resp1Msg = try await post(body: initialBytes.framing()).deFramed()

        let (resp1, _) = try request.decodeHandshakeResponse(
            avro: avro, from: resp1Msg, session: session
        )
        if let newHash = resp1.serverHash { serverHash = newHash }

        guard let retryBytes = try await request.resolveHandshakeResponse(
            resp1, avro: avro, session: session
        ) else {
            handshakeComplete = true
            return
        }

        // Round 2 — retry with full client protocol.
        let resp2Msg = try await post(body: retryBytes.framing()).deFramed()

        let (resp2, _) = try request.decodeHandshakeResponse(
            avro: avro, from: resp2Msg, session: session
        )
        if let newHash = resp2.serverHash { serverHash = newHash }
        _ = try await request.resolveHandshakeResponse(resp2, avro: avro, session: session)
        handshakeComplete = true
    }

    // MARK: - Two-way call

    /// Sends a typed request and waits for the response.
    ///
    /// The IPC handshake is performed automatically on the first call.
    public func call<Req: Codable & Sendable, Res: Codable & Sendable>(
        messageName: String,
        parameters:  [Req],
        as responseType: Res.Type
    ) async throws -> Res {
        if !handshakeComplete {
            try await performHandshake()
        }

        let payload      = try await buildPayload(messageName: messageName, parameters: parameters)
        let avro         = Avro()
        let responseMsg  = try await post(body: payload.framing()).deFramed()

        let (_, remainingSlice) = try request.decodeHandshakeResponse(
            avro: avro, from: responseMsg, session: session
        )
        let remaining = Data(remainingSlice)
        let (header, responseArray): (ResponseHeader, [Res]) = try await request.decodeResponse(
            avro:        avro,
            messageName: messageName,
            from:        remaining,
            serverHash:  serverHash,
            session:     session
        )

        if header.flag {
            let msg = (responseArray.first.map { "\($0)" }) ?? "server error"
            throw AvroIPCError.serverError(msg)
        }
        guard let result = responseArray.first else {
            throw AvroIPCError.decodingFailed("No response data for '\(messageName)'")
        }
        return result
    }

    // MARK: - One-way call

    /// Sends a one-way (fire-and-forget) request over HTTP POST.
    ///
    /// The HTTP response is awaited so the caller can detect transport errors,
    /// but the Avro response body (which contains only a HandshakeResponse) is discarded.
    public func onewayCall<Req: Codable & Sendable>(
        messageName: String,
        parameters:  [Req]
    ) async throws {
        if !handshakeComplete {
            try await performHandshake()
        }
        let payload = try await buildPayload(messageName: messageName, parameters: parameters)
        _ = try await post(body: payload.framing())
    }
}

// MARK: - AvroHTTPClientHandler

/// NIO channel handler that accumulates the HTTP response body and resumes
/// the `CheckedContinuation` when the response is complete.
///
/// All mutable state is accessed only on the channel's event loop — safe with `@unchecked Sendable`.
private final class AvroHTTPClientHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn   = HTTPClientResponsePart
    typealias OutboundOut = Never

    private static let maxBodyBytes = 64 * 1024 * 1024   // 64 MB

    private let continuation:   CheckedContinuation<Data, Error>
    private var bodyAccumulator = Data()
    private var resumed         = false

    init(continuation: CheckedContinuation<Data, Error>) {
        self.continuation = continuation
    }

    /// Called when the connection fails before any pipeline events fire.
    func fail(_ error: Error) {
        guard !resumed else { return }
        resumed = true
        continuation.resume(throwing: error)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head:
            bodyAccumulator = Data()
        case .body(var buffer):
            guard bodyAccumulator.count + buffer.readableBytes <= Self.maxBodyBytes else {
                context.close(promise: nil)
                return
            }
            if let bytes = buffer.readBytes(length: buffer.readableBytes) {
                bodyAccumulator.append(contentsOf: bytes)
            }
        case .end:
            guard !resumed else { return }
            resumed = true
            continuation.resume(returning: bodyAccumulator)
            context.close(promise: nil)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard !resumed else { return }
        resumed = true
        continuation.resume(throwing: error)
        context.close(promise: nil)
    }
}
