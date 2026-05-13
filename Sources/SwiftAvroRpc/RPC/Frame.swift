//
//  RPC/Frame.swift
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

// MARK: - Errors

/// Errors thrown by the Avro IPC client and server.
enum AvroIPCError: Error, Sendable {
    /// The handshake exchange failed with the given reason.
    case handshakeFailed(String)
    /// Avro encoding of a request or response failed.
    case encodingFailed(String)
    /// Avro decoding of a request or response failed.
    case decodingFailed(String)
    /// The TCP connection was closed or not established.
    case connectionClosed
    /// The operation exceeded the allowed time.
    case timeout
    /// No handler was registered for the given message name.
    case noHandler(String)
    /// The server returned an error response (flag = true in ResponseHeader).
    case serverError(String)
}

// MARK: - AvroIPCHandler

/// Implement this on the server to handle incoming RPC calls.
public protocol AvroIPCHandler: Sendable {
    /// Called for each decoded request. Return encoded response data.
    func handle(messageName: String, requestData: Data) async throws -> Data

    /// Schema-evolution-aware variant. `writerSchemas` are the client's (writer) schemas;
    /// `readerSchemas` are the server's (reader) schemas for the same message parameters.
    /// The default implementation ignores schema info and delegates to the basic overload.
    func handle(
        messageName:   String,
        requestData:   Data,
        writerSchemas: [AvroSchema],
        readerSchemas: [AvroSchema]
    ) async throws -> Data
}

extension AvroIPCHandler {
    public func handle(
        messageName:   String,
        requestData:   Data,
        writerSchemas: [AvroSchema],
        readerSchemas: [AvroSchema]
    ) async throws -> Data {
        try await handle(messageName: messageName, requestData: requestData)
    }
}

// MARK: - Pending call (client side)

/// A suspended RPC call waiting for the server's response.
struct PendingCall: Sendable {
    let messageName: String
    let continuation: CheckedContinuation<Data, Error>
}

// MARK: - AvroFrameDecoder

/// NIO decoder that accumulates length-prefixed Avro frames until the
/// zero-length terminator, then fires a single channel read event with
/// the complete reassembled framed message (including all length prefixes
/// and the terminator). This ensures each handler receives exactly one
/// event per complete Avro IPC message, so deFraming() works correctly
/// downstream and pendingCalls is consumed exactly once per round-trip.
final class AvroFrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = Data

    func decode(
        context: ChannelHandlerContext,
        buffer: inout ByteBuffer
    ) throws -> DecodingState {
        // Scan ahead to find the zero-length terminator without consuming bytes.
        // Only read once the full message including terminator is in the buffer.
        var scanIndex = buffer.readerIndex
        var totalSize = 0

        while scanIndex + 4 <= buffer.writerIndex {
            guard let length = buffer.getInteger(
                at: scanIndex, as: UInt32.self
            ) else { return .needMoreData }

            totalSize += 4
            scanIndex += 4

            if length == 0 {
                // Terminator found — read the entire message as one Data.
                guard let bytes = buffer.readBytes(length: totalSize) else {
                    return .needMoreData
                }
                context.fireChannelRead(wrapInboundOut(Data(bytes)))
                return .continue
            }

            let payloadSize = Int(length)
            guard scanIndex + payloadSize <= buffer.writerIndex else {
                return .needMoreData
            }
            totalSize += payloadSize
            scanIndex += payloadSize
        }
        return .needMoreData
    }

    func decodeLast(
        context: ChannelHandlerContext,
        buffer: inout ByteBuffer,
        seenEOF: Bool
    ) throws -> DecodingState {
        try decode(context: context, buffer: &buffer)
    }
}
