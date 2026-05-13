//
//  RPC/Client.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 04/04/2026.
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

import Foundation
import NIO
import SwiftAvroCore

// MARK: - AvroIPCClient

public actor AvroIPCClient {

    private let request:          AvroIPCRequest
    private var session:          AvroIPCSession
    private var channel:          (any Channel)?
    private var pendingCalls:     [PendingCall] = []
    private var handshakeComplete = false
    private var serverHash:       MD5Hash

    init(
        context:        AvroIPCContext,
        clientHash:     MD5Hash,
        clientProtocol: String,
        serverHash:     MD5Hash
    ) throws {
        self.session    = AvroIPCSession(context: context)
        self.request    = try Avro().makeIPCRequest(
            clientHash:     clientHash,
            clientProtocol: clientProtocol,
            session:        session
        )
        self.serverHash = serverHash
    }

    // MARK: - Connection

    func connect(using transport: any AvroIPCClientTransport, eventLoopGroup: EventLoopGroup) async throws {
        let client = self
        let bootstrap = ClientBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                do {
                    try channel.pipeline.syncOperations.addHandler(
                        ByteToMessageHandler(AvroFrameDecoder())
                    )
                    try channel.pipeline.syncOperations.addHandler(
                        AvroClientChannelHandler(client: client)
                    )
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }
        channel = try await transport.connect(using: bootstrap)
    }

    public func disconnect() async throws {
        try await channel?.close()
        channel = nil
    }

    // MARK: - Handshake

    /// Two-round-trip handshake: probe → NONE (learn server schema) → retry → BOTH.
    ///
    /// After this returns `session.clientCache` holds the server protocol schemas
    /// and `self.serverHash` is set to the authoritative server hash.
    private func performHandshake(channel: Channel) async throws {
        let avro = Avro()

        // Round 1 — initial probe: nil clientProtocol, zero-filled serverHash (per spec).
        let initialBytes = try request.encodeInitialHandshake(avro: avro, session: session)
        let resp1Raw: Data = try await withCheckedThrowingContinuation { continuation in
            pendingCalls.append(PendingCall(messageName: "", continuation: continuation))
            var buf = channel.allocator.buffer(capacity: initialBytes.count + 8)
            buf.writeBytes(initialBytes.framing())
            channel.writeAndFlush(buf, promise: nil)
        }

        // receive(data:) already deframes; resp1Raw is the raw message bytes.
        let (resp1, _) = try request.decodeHandshakeResponse(
            avro: avro, from: resp1Raw, session: session
        )

        // Update serverHash from any response that carries one.
        if let newHash = resp1.serverHash { serverHash = newHash }

        // resolveHandshakeResponse updates clientCache and returns retry bytes for NONE,
        // nil for BOTH or CLIENT (handshake already complete after one round trip).
        guard let retryBytes = try await request.resolveHandshakeResponse(
            resp1, avro: avro, session: session
        ) else {
            handshakeComplete = true
            return
        }

        // Round 2 — retry with full clientProtocol so the server can cache our schema.
        let resp2Raw: Data = try await withCheckedThrowingContinuation { continuation in
            pendingCalls.append(PendingCall(messageName: "", continuation: continuation))
            var buf = channel.allocator.buffer(capacity: retryBytes.count + 8)
            buf.writeBytes(retryBytes.framing())
            channel.writeAndFlush(buf, promise: nil)
        }

        let (resp2, _) = try request.decodeHandshakeResponse(
            avro: avro, from: resp2Raw, session: session
        )
        if let newHash = resp2.serverHash { serverHash = newHash }
        _ = try await request.resolveHandshakeResponse(resp2, avro: avro, session: session)
        handshakeComplete = true
    }

    // MARK: - Two-way RPC call

    /// Sends a typed request and waits for the response.
    ///
    /// The Avro IPC handshake is performed automatically on the first call.
    /// Subsequent calls on the same connection skip the handshake round-trips.
    public func call<Req: Codable & Sendable, Res: Codable & Sendable>(
        messageName: String,
        parameters:  [Req],
        as responseType: Res.Type
    ) async throws -> Res {
        guard let channel else { throw AvroIPCError.connectionClosed }

        if !handshakeComplete {
            try await performHandshake(channel: channel)
        }

        let avro = Avro()

        // Every Avro IPC message begins with a HandshakeRequest.
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

        let payload = handshakeData + callData
        let responseRaw: Data = try await withCheckedThrowingContinuation { continuation in
            pendingCalls.append(
                PendingCall(messageName: messageName, continuation: continuation)
            )
            var buffer = channel.allocator.buffer(capacity: payload.count + 8)
            buffer.writeBytes(payload.framing())
            channel.writeAndFlush(buffer, promise: nil)
        }

        // responseRaw is already deframed by receive(data:).
        // Strip the leading HandshakeResponse, then decode the call response.
        // decodeHandshakeResponse returns data.suffix(...) which is a slice with
        // non-zero startIndex; copy it so AvroDataReader sees startIndex == 0.
        let (_, remainingSlice) = try request.decodeHandshakeResponse(
            avro: avro, from: responseRaw, session: session
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
        guard let response = responseArray.first else {
            throw AvroIPCError.decodingFailed("No response data for '\(messageName)'")
        }
        return response
    }

    // MARK: - One-way RPC call

    /// Sends a one-way (fire-and-forget) request. No response is awaited.
    ///
    /// Use for messages declared with `"oneway": true` in the Avro protocol.
    public func onewayCall<Req: Codable & Sendable>(
        messageName: String,
        parameters:  [Req]
    ) async throws {
        guard let channel else { throw AvroIPCError.connectionClosed }

        if !handshakeComplete {
            try await performHandshake(channel: channel)
        }

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
        let payload = handshakeData + callData
        var buffer = channel.allocator.buffer(capacity: payload.count + 8)
        buffer.writeBytes(payload.framing())
        channel.writeAndFlush(buffer, promise: nil)
    }

    // MARK: - Inbound (NIO handler → actor)

    /// Called by ``AvroClientChannelHandler`` for each complete inbound framed message.
    ///
    /// `data` arrives WITH length prefixes from ``AvroFrameDecoder``; deframe here so
    /// every pending-call continuation receives clean raw message bytes.
    func receive(data: Data) {
        guard let pending = pendingCalls.first else { return }
        pendingCalls.removeFirst()
        let rawData = data.deFraming().reduce(Data(), +)
        pending.continuation.resume(returning: rawData)
    }

    func failAllPending(error: Error) {
        for pending in pendingCalls {
            pending.continuation.resume(throwing: error)
        }
        pendingCalls.removeAll()
    }
}

// MARK: - AvroClientChannelHandler

private final class AvroClientChannelHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn   = Data
    typealias OutboundOut = ByteBuffer

    private let client: AvroIPCClient

    init(client: AvroIPCClient) {
        self.client = client
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let inData = unwrapInboundIn(data)
        Task { await client.receive(data: inData) }
    }

    func channelInactive(context: ChannelHandlerContext) {
        Task { await client.failAllPending(error: AvroIPCError.connectionClosed) }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        Task { await client.failAllPending(error: error) }
        context.close(promise: nil)
    }
}
