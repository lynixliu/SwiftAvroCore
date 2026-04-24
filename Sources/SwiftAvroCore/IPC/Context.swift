//
//  Context.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 20/04/22.
//  Refactored: Context → AvroIPCContext (public struct, Sendable, throwing init).
//

import Foundation

// MARK: - AvroIPCContext

/// Shared, immutable configuration for both the client and server sides of an
/// Avro IPC session.
///
/// Holds pre-parsed handshake schemas and metadata maps so that
/// ``AvroIPCRequest`` and ``AvroIPCResponse`` never need to re-parse them.
///
/// `AvroIPCContext` is `Sendable` — it is safe to share across actor boundaries.
public struct AvroIPCContext: Sendable {

    public let requestMeta:  [String: [UInt8]]
    public let responseMeta: [String: [UInt8]]

    /// The finite set of protocol names this endpoint recognises.
    ///
    /// When non-nil, ``AvroIPCRequest.encodeHandshake`` validates that the
    /// `clientProtocol` string supplied at init time is a member of this set
    /// and throws ``AvroHandshakeError.unknownProtocol`` if it is not.
    /// Pass `nil` (the default) to skip validation in open/dynamic deployments.
    public let knownProtocols: Set<String>?

    /// Pre-parsed schema for ``HandshakeRequest``.
    public let requestSchema:  AvroSchema

    /// Pre-parsed schema for ``HandshakeResponse``.
    public let responseSchema: AvroSchema

    /// Pre-parsed schema for the IPC metadata map (`map<bytes>`).
    public let metaSchema:     AvroSchema

    /// Creates a context, eagerly parsing all required Avro IPC schemas.
    ///
    /// - Parameters:
    ///   - requestMeta:    Metadata attached to every outgoing request (default `[:]`).
    ///   - responseMeta:   Metadata attached to every outgoing response (default `[:]`).
    ///   - knownProtocols: Optional closed set of valid protocol names.  When supplied,
    ///     ``AvroIPCRequest`` validates the `clientProtocol` string on every handshake.
    public init(
        requestMeta:    [String: [UInt8]] = [:],
        responseMeta:   [String: [UInt8]] = [:],
        knownProtocols: Set<String>?       = nil
    ) {
        self.requestMeta    = requestMeta
        self.responseMeta   = responseMeta
        self.knownProtocols = knownProtocols
        // Force-unwrap is acceptable here: these are compile-time constant schemas
        // that match the canonical Avro spec verbatim — a decode failure is an
        // unrecoverable programmer error, not a runtime condition.
        let avro = Avro()
        self.requestSchema  = avro.decodeSchema(schema: MessageConstant.requestSchema)!
        self.responseSchema = avro.decodeSchema(schema: MessageConstant.responseSchema)!
        self.metaSchema     = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
    }
}

// MARK: - Avro IPC Framing

extension Data {

    /// Parses Avro IPC frames from raw data.
    ///
    /// Each frame is prefixed with a big-endian `UInt32` byte count.
    /// A zero-length prefix signals the end of the frame sequence.
    func deFraming() -> [Data] {
        var frames: [Data] = []
        var offset = 0

        while offset + 4 <= count {
            let length: UInt32 = (UInt32(self[offset])     << 24) |
                                 (UInt32(self[offset + 1]) << 16) |
                                 (UInt32(self[offset + 2]) <<  8) |
                                  UInt32(self[offset + 3])
            offset += 4

            if length == 0 { break }  // mandatory Avro IPC terminator

            // `length` is a UInt32 (max ~4 GB). Converting UInt32(Int.max) crashes on
            // 64-bit because Int.max > UInt32.max. Use Int32.max (2 GB) as the safe
            // upper bound — Avro IPC frames are never that large in practice.
            guard length <= UInt32(Int32.max),
                  case let payloadEnd = offset + Int(length),
                  payloadEnd <= count else { break }  // malformed / truncated
            frames.append(self[offset..<payloadEnd])
            offset = payloadEnd
        }
        return frames
    }

    /// Wraps the receiver in Avro IPC framing, splitting into chunks of at most
    /// `maxFrameLength` bytes each, and appending the mandatory zero-length terminator.
    func framing(maxFrameLength: Int = 16 * 1024) -> Data {
        guard maxFrameLength > 0 else { return Data([0, 0, 0, 0]) }
        var result = Data()
        result.reserveCapacity(count + (count / maxFrameLength + 2) * 4)
        var offset = 0
        while offset < count {
            let chunkSize = Swift.min(count - offset, maxFrameLength)
            // Write the 4-byte big-endian length as UInt32 — matching the Avro IPC
            // wire format and avoiding sign-extension from Int32 on large chunks.
            result.append(contentsOf: UInt32(chunkSize).bigEndianBytes)
            let end = offset + chunkSize
            result.append(self[offset..<end])
            offset = end
        }
        result.append(contentsOf: UInt32(0).bigEndianBytes)  // Avro IPC end-of-frames terminator
        return result
    }

    /// Mutating convenience wrapper for ``framing(maxFrameLength:)``.
    mutating func frame(maxFrameLength: Int = 16 * 1024) {
        self = framing(maxFrameLength: maxFrameLength)
    }
}

private extension FixedWidthInteger {
    var bigEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
}
