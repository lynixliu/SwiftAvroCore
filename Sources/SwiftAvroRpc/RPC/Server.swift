//
//  RPC/Server.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 04/04/2026.
//  Copyright © 2026 Yang Liu.
//

import Foundation
import NIO
import SwiftAvroCore

// MARK: - AvroIPCServer

/// Actor that owns the server-side handshake state and dispatches decoded
/// requests to the application-level ``AvroIPCHandler``.
///
/// One instance is created per accepted TCP connection; state is never shared
/// between connections.
public actor AvroIPCServer {

    private let response:       AvroIPCResponse
    private var session:        AvroIPCSession
    private let handler:        any AvroIPCHandler
    private var avroProtocol:   AvroProtocol?   // lazily parsed from response.serverProtocol

    /// Creates a server instance for a single connection.
    init(
        context:        AvroIPCContext,
        serverHash:     MD5Hash,
        serverProtocol: String,
        handler:        any AvroIPCHandler
    ) {
        self.response = Avro().makeIPCResponse(
            serverHash:     serverHash,
            serverProtocol: serverProtocol
        )
        self.session  = AvroIPCSession(context: context)
        self.handler  = handler
    }

    /// Returns true if the named message is declared one-way in the server protocol.
    private func isOneway(_ messageName: String) -> Bool {
        if avroProtocol == nil {
            avroProtocol = try? JSONDecoder().decode(
                AvroProtocol.self,
                from: Data(response.serverProtocol.utf8)
            )
        }
        return avroProtocol?.messages?[messageName]?.oneway == true
    }

    // MARK: - Inbound

    /// Processes one complete inbound Avro IPC framed message and returns the
    /// framed response to write back to the channel.
    func receive(data: Data) async throws -> Data {
        let avro = Avro()

        // AvroFrameDecoder fires the raw framed bytes (WITH length prefixes).
        // Deframe them and join all frame payloads into one message buffer.
        let frames = data.deFraming()
        guard !frames.isEmpty else {
            throw AvroIPCError.decodingFailed("Empty frame")
        }
        let messageData = frames.reduce(Data(), +)

        // Resolve handshake: updates session.serverCache, returns the
        // serialised HandshakeResponse and the remaining call payload.
        let (handshakeReq, handshakeResponseData, callPayload) =
            try await response.resolveHandshake(avro: avro, from: messageData, session: session)

        // Pure handshake exchange — no call payload (e.g. the initial probe or retry).
        guard !callPayload.isEmpty else {
            return handshakeResponseData.framing()
        }

        // Read meta + message name from the call payload to build RequestHeader.
        // We use AvroDataReader directly so we never pass pre-encoded Data into
        // the typed encodeResponse<T:Codable> path (which would double-encode it).
        let stringSchema = avro.newSchema(schema: "\"string\"")!
        let reader       = avro.makeDataReader(data: callPayload)
        let meta: [String: [UInt8]] = try reader.decode(schema: session.context.metaSchema)
        let messageName: String     = try reader.decode(schema: stringSchema)
        let paramsOffset = callPayload.count - reader.bytesRemaining
        let paramsData   = Data(callPayload.dropFirst(paramsOffset))

        // Fetch writer (client) and reader (server) request schemas for evolution.
        // isOneway() also lazily populates avroProtocol, so call it first.
        let oneway = isOneway(messageName)
        let writerSchemas = await session.serverCache.requestSchemas(
            hash: handshakeReq.clientHash, messageName: messageName
        ) ?? []
        let readerSchemas = avroProtocol?.getRequest(messageName: messageName) ?? []

        // One-way messages: dispatch handler but do not send a call response.
        // The framed HandshakeResponse is still sent so the peer's session stays in sync.
        if oneway {
            _ = try? await handler.handle(
                messageName:   messageName,
                requestData:   paramsData,
                writerSchemas: writerSchemas,
                readerSchemas: readerSchemas
            )
            return handshakeResponseData.framing()
        }

        // Two-way call: dispatch handler, encode result (or error) as response payload:
        // meta + flag(bool) + body.
        let boolSchema = avro.newSchema(schema: "\"boolean\"")!

        do {
            let responseBody = try await handler.handle(
                messageName:   messageName,
                requestData:   paramsData,
                writerSchemas: writerSchemas,
                readerSchemas: readerSchemas
            )
            var payload = Data()
            payload.append(try avro.encodeFrom(meta, schema: session.context.metaSchema))
            payload.append(try avro.encodeFrom(false, schema: boolSchema))
            payload.append(responseBody)
            return (handshakeResponseData + payload).framing()

        } catch {
            // Avro IPC error response: flag=true + union-index 0 (undeclared string error).
            var payload = Data()
            payload.append(try avro.encodeFrom(meta, schema: session.context.metaSchema))
            payload.append(try avro.encodeFrom(true, schema: boolSchema))
            payload.append(contentsOf: [UInt8(0)])   // zig-zag 0 = union branch 0 (string)
            payload.append(try avro.encodeFrom(
                error.localizedDescription, schema: stringSchema
            ))
            return (handshakeResponseData + payload).framing()
        }
    }
}

// MARK: - AvroIPCServerBootstrap

/// Starts the NIO server and creates an ``AvroIPCServer`` per accepted channel.
final class AvroIPCServerBootstrap: Sendable {

    private let eventLoopGroup: EventLoopGroup
    private let context:        AvroIPCContext
    private let serverHash:     MD5Hash
    private let serverProtocol: String
    private let handler:        any AvroIPCHandler

    init(
        eventLoopGroup: EventLoopGroup,
        context:        AvroIPCContext,
        serverHash:     MD5Hash,
        serverProtocol: String,
        handler:        any AvroIPCHandler
    ) {
        self.eventLoopGroup  = eventLoopGroup
        self.context         = context
        self.serverHash      = serverHash
        self.serverProtocol  = serverProtocol
        self.handler         = handler
    }

    /// Binds using the given transport (TCP, Unix domain socket, or custom).
    func bind(using transport: any AvroIPCServerTransport) async throws -> any Channel {
        try await transport.bind(using: makeBootstrap())
    }

    private func makeBootstrap() -> ServerBootstrap {
        let context        = self.context
        let serverHash     = self.serverHash
        let serverProtocol = self.serverProtocol
        let handler        = self.handler

        return ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let server = AvroIPCServer(
                    context:        context,
                    serverHash:     serverHash,
                    serverProtocol: serverProtocol,
                    handler:        handler
                )
                do {
                    try channel.pipeline.syncOperations.addHandler(
                        ByteToMessageHandler(AvroFrameDecoder())
                    )
                    try channel.pipeline.syncOperations.addHandler(
                        AvroServerChannelHandler(server: server)
                    )
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }
    }
}

// MARK: - AvroServerChannelHandler

private final class AvroServerChannelHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn   = Data
    typealias OutboundOut = ByteBuffer

    private let server: AvroIPCServer

    init(server: AvroIPCServer) {
        self.server = server
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let inData  = unwrapInboundIn(data)
        let channel = context.channel
        Task {
            do {
                let responseData = try await server.receive(data: inData)
                var buffer = channel.allocator.buffer(capacity: responseData.count)
                buffer.writeBytes(responseData)
                channel.writeAndFlush(buffer, promise: nil)
            } catch {
                // Protocol-level decode failure: close the connection.
                channel.close(promise: nil)
            }
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}
