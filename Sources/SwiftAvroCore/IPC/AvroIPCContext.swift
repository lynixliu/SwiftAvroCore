//
//  AvroIPCContext.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 20/04/22.
//

import Foundation

// MARK: - AvroIPCContext

/// Shared, immutable configuration for Avro IPC — holds pre-parsed handshake
/// schemas and request/response metadata. Passed explicitly to every method
/// that needs it; never stored inside request or response handlers.
public final class AvroIPCContext: Sendable {
    public let requestMeta:  [String: [UInt8]]
    public let responseMeta: [String: [UInt8]]
    public let requestSchema:  AvroSchema
    public let responseSchema: AvroSchema
    public let metaSchema:     AvroSchema

    public init(
        requestMeta:  [String: [UInt8]] = [:],
        responseMeta: [String: [UInt8]] = [:]
    ) {
        self.requestMeta  = requestMeta
        self.responseMeta = responseMeta
        let avro = Avro()
        self.requestSchema  = avro.decodeSchema(schema: MessageConstant.requestSchema)!
        self.responseSchema = avro.decodeSchema(schema: MessageConstant.responseSchema)!
        self.metaSchema     = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
    }
}

// MARK: - Message constants

public enum MessageConstant {
    public static let requestSchema: String = """
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {"name": "clientHash",     "type": {"type": "fixed", "name": "MD5", "size": 16}},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash",     "type": "MD5"},
        {"name": "meta",           "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    """

    public static let responseSchema: String = """
    {
      "type": "record",
      "name": "HandshakeResponse",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {"name": "match",
         "type": {"type": "enum", "name": "HandshakeMatch",
                  "symbols": ["BOTH", "CLIENT", "NONE"]}},
        {"name": "serverProtocol", "type": ["null", "string"]},
        {"name": "serverHash",
         "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
        {"name": "meta",
         "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    """

    public static let metadataSchema: String = """
    {"type": "map", "values": "bytes"}
    """
}

// MARK: - Avro IPC types

public enum HandshakeMatch: String, Codable, Sendable {
    case BOTH, CLIENT, NONE
}

public typealias MD5Hash = [UInt8]

public struct HandshakeRequest: Codable, Sendable {
    public let clientHash:     MD5Hash
    public let clientProtocol: String?
    public let serverHash:     MD5Hash
    public var meta:           [String: [UInt8]]?

    public init(
        clientHash: MD5Hash,
        clientProtocol: String?,
        serverHash: MD5Hash,
        meta: [String: [UInt8]]? = nil
    ) {
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
        self.serverHash     = serverHash
        self.meta           = meta
    }
}

public struct HandshakeResponse: Codable, Sendable {
    public let match:          HandshakeMatch
    public let serverProtocol: String?
    public let serverHash:     MD5Hash?
    public var meta:           [String: [UInt8]]?

    public init(
        match: HandshakeMatch,
        serverProtocol: String?,
        serverHash: MD5Hash?,
        meta: [String: [UInt8]]? = nil
    ) {
        self.match          = match
        self.serverProtocol = serverProtocol
        self.serverHash     = serverHash
        self.meta           = meta
    }
}

public struct RequestHeader: Codable, Sendable {
    public let meta: [String: [UInt8]]?
    public let name: String

    public init(meta: [String: [UInt8]]?, name: String) {
        self.meta = meta
        self.name = name
    }
}

public struct ResponseHeader: Codable, Sendable {
    public let meta: [String: [UInt8]]?
    /// `false` = normal response, `true` = error response (per Avro IPC spec).
    public let flag: Bool

    public init(meta: [String: [UInt8]]?, flag: Bool) {
        self.meta = meta
        self.flag = flag
    }
}

// MARK: - Framing

extension Data {

    /// Extracts Avro IPC frames. Stops at the mandatory zero-length terminator.
    public func deFraming() -> [Data] {
        var frames: [Data] = []
        var offset = 0
        while offset + 4 <= count {
            let length: UInt32 =
                (UInt32(self[offset])     << 24) |
                (UInt32(self[offset + 1]) << 16) |
                (UInt32(self[offset + 2]) <<  8) |
                 UInt32(self[offset + 3])
            offset += 4
            if length == 0 { break }
            let payloadEnd = offset + Int(length)
            guard payloadEnd <= count else { break }
            frames.append(self[offset ..< payloadEnd])
            offset = payloadEnd
        }
        return frames
    }

    /// Frames data into Avro IPC format, terminated by a zero-length buffer.
    public func framing(maxFrameLength: Int = 16 * 1024) -> Data {
        guard maxFrameLength > 0 else { return Data([0, 0, 0, 0]) }
        var result = Data()
        result.reserveCapacity(count + (count / maxFrameLength + 2) * 4)
        var offset = 0
        while offset < count {
            let chunkSize = Swift.min(count - offset, maxFrameLength)
            result.append(contentsOf: Int32(chunkSize).bigEndianBytes)
            result.append(self[offset ..< offset + chunkSize])
            offset += chunkSize
        }
        result.append(contentsOf: Int32(0).bigEndianBytes)
        return result
    }

    public mutating func frame(maxFrameLength: Int = 16 * 1024) {
        self = framing(maxFrameLength: maxFrameLength)
    }
}

private extension FixedWidthInteger {
    var bigEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
}
