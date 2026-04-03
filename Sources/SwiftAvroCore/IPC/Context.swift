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
    
    /// Safely extracts Avro IPC frames according to the specification.
    /// Each non-zero length prefix is followed by that many bytes of payload.
    /// Stops at the mandatory zero-length terminator.
    func deFraming() -> [Data] {
        var frames: [Data] = []
        var offset = 0
        
        while offset + 4 <= count {
            // Read big-endian UInt32 length safely
            let length: UInt32 = (UInt32(self[offset]) << 24) |
                                 (UInt32(self[offset + 1]) << 16) |
                                 (UInt32(self[offset + 2]) << 8) |
                                 UInt32(self[offset + 3])
            
            offset += 4
            
            if length == 0 {
                break                     // correct Avro terminator
            }
            
            let payloadEnd = offset + Int(length)
            guard payloadEnd <= count else {
                break                     // malformed / truncated → stop
            }
            
            frames.append(self[offset..<payloadEnd])
            offset = payloadEnd
        }
        
        return frames
    }
    
    /// Frames the data into Avro IPC format (max payload size per frame).
    /// Always terminates with a zero-length buffer as required by the spec.
    func framing(maxFrameLength: Int = 16 * 1024) -> Data {
        guard maxFrameLength > 0 else {
            return Data([0, 0, 0, 0])
        }
        
        var result = Data()
        result.reserveCapacity(count + (count / maxFrameLength + 2) * 4)
        
        var offset = 0
        
        while offset < count {
            let chunkSize = Swift.min(count - offset, maxFrameLength)
            
            // Length prefix (big-endian Int32)
            result.append(contentsOf: Int32(chunkSize).bigEndianBytes)
            
            // Payload
            let end = offset + chunkSize
            result.append(self[offset..<end])
            
            offset = end
        }
        
        // Mandatory zero-length terminator
        result.append(contentsOf: Int32(0).bigEndianBytes)
        
        return result
    }
    
    /// Mutating convenience
    mutating func frame(maxFrameLength: Int = 16 * 1024) {
        self = framing(maxFrameLength: maxFrameLength)
    }
}

private extension FixedWidthInteger {
    var bigEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bigEndian) { Array($0) }
    }
}

private extension UInt32 {
    init(bigEndianBytes bytes: [UInt8]) {
        self = bytes.withUnsafeBufferPointer {
            $0.baseAddress!.withMemoryRebound(to: UInt32.self, capacity: 1) { $0.pointee }
        }.bigEndian
    }
}
