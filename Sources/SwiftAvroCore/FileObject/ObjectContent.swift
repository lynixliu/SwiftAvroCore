//
//  ObjectContent.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Value types shared by ObjectContainerWriter and ObjectContainerReader.

import Foundation

// MARK: - Header

/// The Avro container file header.
///
/// Binary layout (encoded as an Avro record):
///   magic (4 bytes) | meta (Avro map<bytes>) | sync (16 bytes)
public struct Header: Codable {
    private var magic: [UInt8]
    private var meta:  [String: [UInt8]]
    private var sync:  [UInt8]

    /// Builds a header with the given sync marker.
    /// Pass `[]` for read-only containers — the marker is replaced when
    /// the header is decoded from a file.
    init(syncMarker: [UInt8] = []) {
        magic = Array("Obj".utf8) + [1]            // "Obj" + version byte 0x01
        meta  = [:]
        sync  = syncMarker
    }

    // MARK: Accessors

    /// The raw magic bytes — should equal `["O","b","j", 0x01]`.
    public var magicValue: [UInt8] { magic }

    /// The 16-byte random sync marker written after the header and each block.
    public var marker: [UInt8] { sync }

    /// The codec name stored in the header metadata.
    public var codec: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataCodec] else {
                throw AvroCodingError.decodingFailed("Missing codec metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

    /// The schema JSON string stored in the header metadata.
    public var schema: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataSchema] else {
                throw AvroCodingError.decodingFailed("Missing schema metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

    // MARK: Mutators

    mutating func addMetaData(key: String, value: [UInt8]) {
        meta[key] = value
    }

    mutating func setSchema(jsonSchema: String) {
        addMetaData(key: AvroReservedConstants.metaDataSchema, value: Array(jsonSchema.utf8))
    }

    mutating func setCodec(codec: String) {
        addMetaData(key: AvroReservedConstants.metaDataCodec, value: Array(codec.utf8))
    }
}

// MARK: - Block

/// A single data block within an Avro container file.
///
/// Binary layout (after the header):
///   objectCount (long) | byteCount (long) | compressedData | syncMarker (16 bytes)
public struct Block {
    /// Number of serialised objects in this block.
    public var objectCount: UInt64

    /// Byte length of the uncompressed payload.
    public var size: UInt64

    /// Concatenation of all Avro-encoded object payloads (uncompressed).
    public var data: Data

    init() {
        objectCount = 0
        size        = 0
        data        = Data()
    }

    init(count: UInt64, data: Data) {
        self.objectCount = count
        self.size        = UInt64(data.count)
        self.data        = data
    }

    /// Appends the encoded bytes of one object to this block.
    mutating func addObject(_ other: Data) {
        objectCount += 1
        size        += UInt64(other.count)
        data.append(other)
    }
}
