//
//  IPC_Definitions.swift
//  SwiftAvroCore
//
//  Centralised definitions for all IPC-related types and constants.
//  Previously scattered across Context.swift, Request.swift, and Response.swift
//  as internal types; promoted to public and made Sendable so they can cross
//  actor/task boundaries in Swift 6 strict-concurrency mode.
//

import Foundation

// MARK: - Type aliases

/// MD5 hash type — 16-byte fixed array matching the `MD5` fixed type in the Avro IPC spec.
public typealias MD5Hash = [UInt8]

// MARK: - Enums

/// Matches the `HandshakeMatch` enum in the Avro IPC specification exactly.
public enum HandshakeMatch: String, Codable, Sendable {
    case BOTH, CLIENT, NONE
}

// MARK: - Handshake structures

public struct HandshakeRequest: Codable, Sendable {
    public let clientHash:     MD5Hash       // fixed(16): MD5
    public let clientProtocol: String?       // union [null, string]
    public let serverHash:     MD5Hash       // reference to MD5 fixed
    public var meta:           [String: [UInt8]]?  // union [null, map<bytes>]

    public init(
        clientHash:     MD5Hash,
        clientProtocol: String?,
        serverHash:     MD5Hash,
        meta:           [String: [UInt8]]? = nil
    ) {
        self.clientHash     = clientHash
        self.clientProtocol = clientProtocol
        self.serverHash     = serverHash
        self.meta           = meta
    }
}

public struct HandshakeResponse: Codable, Sendable {
    public let match:          HandshakeMatch   // enum HandshakeMatch
    public let serverProtocol: String?          // union [null, string]
    public let serverHash:     MD5Hash?         // union [null, MD5]
    public var meta:           [String: [UInt8]]?  // union [null, map<bytes>]

    public init(
        match:          HandshakeMatch,
        serverProtocol: String?,
        serverHash:     MD5Hash?,
        meta:           [String: [UInt8]]? = nil
    ) {
        self.match          = match
        self.serverProtocol = serverProtocol
        self.serverHash     = serverHash
        self.meta           = meta
    }
}

// MARK: - Call message headers

/// Call request header — precedes message parameters in an IPC call frame.
public struct RequestHeader: Codable, Sendable {
    public let meta: [String: [UInt8]]?
    public let name: String
}

/// Call response header — precedes response/error data in an IPC response frame.
/// `false` = normal response; `true` = error response (per Avro IPC spec).
public struct ResponseHeader: Codable, Sendable {
    public let meta: [String: [UInt8]]?
    public let flag: Bool
}

// MARK: - Schema constants

/// Avro IPC handshake schemas — verbatim from the official Avro IPC specification.
/// Verified against: https://avro.apache.org/docs/1.11.1/specification/
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

    /// Avro IPC metadata schema: map<bytes>
    public static let metadataSchema: String = """
    {"type": "map", "values": "bytes"}
    """
}
