//
//  Context.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 20/04/22.
//

import Foundation
// MARK: - Avro IPC Handshake Schemas (compliant with Apache Avro spec, all versions through 1.11.x)
// The handshake schemas below are verbatim from the official Avro IPC specification.
// Verified against: https://avro.apache.org/docs/1.11.1/specification/

enum MessageConstant {
    static let requestSchema: String = """
    {
      "type": "record",
      "name": "HandshakeRequest",
      "namespace": "org.apache.avro.ipc",
      "fields": [
        {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
        {"name": "clientProtocol", "type": ["null", "string"]},
        {"name": "serverHash", "type": "MD5"},
        {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
      ]
    }
    """

    static let responseSchema: String = """
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

    /// Avro IPC metadata schema: map<bytes>
    static let metadataSchema: String = """
    {"type": "map", "values": "bytes"}
    """
}

// MARK: - Avro IPC Types

/// Matches the `HandshakeMatch` enum in the Avro IPC spec exactly.
enum HandshakeMatch: String, Codable, Sendable {
    case BOTH
    case CLIENT
    case NONE
}

/// MD5 hash type — 16-byte fixed, matching the `MD5` fixed type in the spec.
typealias MD5Hash = [UInt8]

struct HandshakeRequest: Codable, Sendable {
    let clientHash: MD5Hash       // fixed(16): MD5
    let clientProtocol: String?   // union [null, string]
    let serverHash: MD5Hash       // reference to MD5 fixed
    var meta: [String: [UInt8]]?  // union [null, map<bytes>]
}

struct HandshakeResponse: Codable, Sendable {
    let match: HandshakeMatch     // enum HandshakeMatch
    let serverProtocol: String?   // union [null, string]
    let serverHash: MD5Hash?      // union [null, MD5]
    var meta: [String: [UInt8]]?  // union [null, map<bytes>]
}

/// Call request header (not part of the handshake; precedes message parameters).
struct RequestHeader: Codable, Sendable {
    let meta: [String: [UInt8]]?
    let name: String
}

/// Call response header (not part of the handshake; precedes response/error data).
struct ResponseHeader: Codable, Sendable {
    let meta: [String: [UInt8]]?
    /// `false` = normal response, `true` = error response (per Avro IPC spec).
    let flag: Bool
}

// MARK: - Context

/// Holds pre-decoded schemas and metadata shared between request/response sides.
final class Context: Sendable {
    let requestMeta: [String: [UInt8]]
    let responseMeta: [String: [UInt8]]
    let requestSchema: AvroSchema
    let responseSchema: AvroSchema
    let metaSchema: AvroSchema

    init(requestMeta: [String: [UInt8]], responseMeta: [String: [UInt8]]) {
        self.requestMeta = requestMeta
        self.responseMeta = responseMeta
        let avro = Avro()
        // Force-unwrap is acceptable here: these are compile-time constants
        // matching the canonical Avro spec schemas — a failure is a programmer error.
        self.requestSchema = avro.decodeSchema(schema: MessageConstant.requestSchema)!
        self.responseSchema = avro.decodeSchema(schema: MessageConstant.responseSchema)!
        self.metaSchema = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
    }
}

// MARK: - Framing (Avro IPC message framing)

extension Data {
    /// Wraps the receiver in Avro IPC framing buffers.
    /// Each frame is prefixed with a 4-byte big-endian length, followed by
    /// a 4-byte zero terminator as required by the Avro IPC framing spec.
    mutating func framing(frameLength: Int32) {
        let zeroFrame = Int32(0).bigEndianBytes
        guard !isEmpty else {
            append(contentsOf: zeroFrame)
            return
        }
        guard count > frameLength else {
            insert(contentsOf: Int32(count).bigEndianBytes, at: 0)
            append(contentsOf: zeroFrame)
            return
        }
        let frames = Int32(count) / frameLength
        let frameLenBytes = frameLength.bigEndianBytes
        let frameStep = frameLength + 4
        for i in 0 ..< frames {
            insert(contentsOf: frameLenBytes, at: Int(i * frameStep))
        }
        let rest = Int32(count) % frameLength
        if rest > 0 {
            let lastBytes = rest.bigEndianBytes
            insert(contentsOf: lastBytes, at: count - Int(rest))
        }
        append(contentsOf: zeroFrame)
    }

    /// Extracts frames from Avro IPC framing, returning each frame as `Data`.
    func deFraming() -> [Data] {
        guard count > 4 else { return [] }
        let payloadLen = count - 4
        let frameLen = Int(UInt32(bigEndianBytes: [UInt8](self[0...3])) + 4)
        guard frameLen > 4 else { return [] }
        let frameCount = payloadLen / frameLen
        var frames: [Data] = []
        frames.reserveCapacity(frameCount + 1)
        for i in 0 ..< frameCount {
            let loc = i * frameLen + 4
            frames.append(subdata(in: loc ..< (loc + frameLen - 4)))
        }
        let rest = payloadLen % frameLen - 4
        if rest > 0 {
            frames.append(subdata(in: (count - 4 - rest) ..< (count - 4)))
        }
        return frames
    }
}

private extension FixedWidthInteger {
    var bigEndianBytes: [UInt8] {
        withUnsafeBytes(of: bigEndian, Array.init)
    }
}

private extension UInt32 {
    init(bigEndianBytes bytes: [UInt8]) {
        self = bytes.withUnsafeBufferPointer {
            $0.baseAddress!.withMemoryRebound(to: UInt32.self, capacity: 1) { $0.pointee }
        }.bigEndian
    }
}
