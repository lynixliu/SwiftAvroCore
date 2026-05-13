//
//  RPC/HTTPServer.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 09/05/2026.
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
import NIOHTTP1
import NIOSSL
import SwiftAvroCore

// MARK: - AvroIPCHTTPServerBootstrap

/// Binds an HTTP (or HTTPS) server and routes each POST body through a shared
/// ``AvroIPCServer`` actor. Sharing a single actor across all connections means
/// the server-side session cache (client protocol registry) persists for the
/// lifetime of the server — enabling BOTH-match handshakes on subsequent calls.
final class AvroIPCHTTPServerBootstrap: Sendable {

    private let eventLoopGroup: EventLoopGroup
    private let context:        AvroIPCContext
    private let serverHash:     MD5Hash
    private let serverProtocol: String
    private let handler:        any AvroIPCHandler
    private let tlsContext:     NIOSSLContext?

    init(
        eventLoopGroup: EventLoopGroup,
        context:        AvroIPCContext,
        serverHash:     MD5Hash,
        serverProtocol: String,
        handler:        any AvroIPCHandler,
        tlsContext:     NIOSSLContext? = nil
    ) {
        self.eventLoopGroup  = eventLoopGroup
        self.context         = context
        self.serverHash      = serverHash
        self.serverProtocol  = serverProtocol
        self.handler         = handler
        self.tlsContext      = tlsContext
    }

    func bind(host: String, port: Int) async throws -> Channel {
        // One shared IPC server for all HTTP connections: the session cache accumulates
        // client protocols across requests so re-negotiation is avoided after first contact.
        let ipcServer = AvroIPCServer(
            context:        context,
            serverHash:     serverHash,
            serverProtocol: serverProtocol,
            handler:        handler
        )
        let tls = self.tlsContext

        return try await ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                do {
                    if let tls {
                        let sslHandler = NIOSSLServerHandler(context: tls)
                        try channel.pipeline.syncOperations.addHandler(sslHandler)
                    }
                    try channel.pipeline.syncOperations.addHandlers(
                        HTTPResponseEncoder(),
                        ByteToMessageHandler(HTTPRequestDecoder()),
                        AvroHTTPServerHandler(ipcServer: ipcServer)
                    )
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }
            .bind(host: host, port: port)
            .get()
    }
}

// MARK: - AvroHTTPServerHandler

/// NIO channel handler that accumulates the HTTP request body and dispatches it to
/// ``AvroIPCServer/receive(data:)``. The Avro IPC framing (length-prefixed frames)
/// is preserved inside the HTTP body, matching the Avro HTTP transport specification.
private final class AvroHTTPServerHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn   = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private static let maxBodyBytes = 64 * 1024 * 1024   // 64 MB

    private let ipcServer: AvroIPCServer
    // Accessed only on the channel's event loop — safe without additional locking.
    private var bodyAccumulator = Data()
    private var keepAlive       = true

    init(ipcServer: AvroIPCServer) {
        self.ipcServer = ipcServer
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch unwrapInboundIn(data) {
        case .head(let head):
            bodyAccumulator = Data()
            keepAlive       = head.isKeepAlive
        case .body(var buffer):
            guard bodyAccumulator.count + buffer.readableBytes <= Self.maxBodyBytes else {
                context.close(promise: nil)
                return
            }
            if let bytes = buffer.readBytes(length: buffer.readableBytes) {
                bodyAccumulator.append(contentsOf: bytes)
            }
        case .end:
            let requestData = bodyAccumulator
            let keepAlive   = self.keepAlive
            let channel     = context.channel
            let server      = ipcServer
            Task {
                do {
                    let responseData = try await server.receive(data: requestData)
                    Self.write(responseData, status: .ok, to: channel, keepAlive: keepAlive)
                } catch {
                    // Send the zero-length frame terminator so the client's deframing
                    // layer gets a valid (empty) Avro message instead of raw empty bytes.
                    Self.write(Data([0, 0, 0, 0]), status: .internalServerError,
                               to: channel, keepAlive: false)
                }
            }
        }
    }

    private static func write(
        _ body:      Data,
        status:      HTTPResponseStatus,
        to channel:  Channel,
        keepAlive:   Bool
    ) {
        var headers = HTTPHeaders()
        headers.add(name: "Content-Type",   value: "avro/binary")
        headers.add(name: "Content-Length", value: "\(body.count)")
        if !keepAlive { headers.add(name: "Connection", value: "close") }

        channel.write(HTTPServerResponsePart.head(
            HTTPResponseHead(version: .http1_1, status: status, headers: headers)
        ), promise: nil)

        if !body.isEmpty {
            var buf = channel.allocator.buffer(capacity: body.count)
            buf.writeBytes(body)
            channel.write(HTTPServerResponsePart.body(.byteBuffer(buf)), promise: nil)
        }
        channel.writeAndFlush(HTTPServerResponsePart.end(nil), promise: nil)
        if !keepAlive { channel.close(promise: nil) }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}
