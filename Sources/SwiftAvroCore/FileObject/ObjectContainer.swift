//
//  AvroClient/AvroObjectFile.swift
//
//  Created by Yang Liu on 21/09/18.
//  Copyright © 2018 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

// MARK: - AvroReservedConstants

public enum AvroReservedConstants {
    public static let metaDataSync    = "avro.sync"
    public static let metaDataCodec   = "avro.codec"
    public static let metaDataSchema  = "avro.schema"
    public static let metaDataReserved = "avro"

    public static let nullCodec   = "null"
    public static let deflateCodec = "deflate"
    public static let xzCodec     = "xz"
    public static let lzfseCodec  = "lzfse"
    public static let lz4Codec    = "lz4"

    public static let syncSize            = 16
    public static let defaultSyncInterval = 4000 * 16

    public static let longScheme = #"{"type":"long"}"#

    public static let markerScheme = #"{"type":"fixed","name":"Sync","size":16}"#

    public static let headerScheme = """
        {
          "type": "record",
          "name": "org.apache.avro.file.Header",
          "fields": [
            {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
            {"name": "meta",  "type": {"type": "map",   "values": "bytes"}},
            {"name": "sync",  "type": {"type": "fixed", "name": "Sync",  "size": 16}}
          ]
        }
        """

    /// blockScheme mirrors the on-disk block structure (count + size + data + sync).
    /// Only the header/meta portion is represented here as a record for schema parsing.
    public static let blockScheme = """
        {
          "type": "record",
          "name": "org.apache.avro.file.Block",
          "fields": [
            {"name": "count", "type": "long"},
            {"name": "data",  "type": "bytes"},
            {"name": "sync",  "type": {"type": "fixed", "name": "Sync", "size": 16}}
          ]
        }
        """

    public static let dummyRecordScheme = """
        {"type":"record","name":"xx","fields":[]}
        """
}

// MARK: - ObjectContainer

public struct ObjectContainer {
    public var header: Header
    public var blocks: [Block]

    private let core:         Avro
    private let headerSchema: AvroSchema
    private let longSchema:   AvroSchema
    private let markerSchema: AvroSchema

    // MARK: Init

    public init(schema: String? = nil, codec: any CodecProtocol) throws {
        core         = Avro()
        headerSchema = try Self.requiredSchema(core, AvroReservedConstants.headerScheme)
        longSchema   = try Self.requiredSchema(core, AvroReservedConstants.longScheme)
        markerSchema = try Self.requiredSchema(core, AvroReservedConstants.markerScheme)

        header = Header()
        header.setSchema(jsonSchema: schema ?? AvroReservedConstants.dummyRecordScheme)
        header.setCodec(codec: codec.name)
        blocks = []

        guard core.decodeSchema(schema: try header.schema) != nil else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    private static func requiredSchema(_ core: Avro, _ json: String) throws -> AvroSchema {
        guard let s = core.newSchema(schema: json) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        return s
    }

    // MARK: Header size

    var headerSize: Int {
        (try? AvroEncoder().sizeOf(header, schema: headerSchema)) ?? 0
    }

    // MARK: Metadata

    mutating func setMetaItem(key: String, value: [UInt8]) {
        header.addMetaData(key: key, value: value)
    }

    // MARK: Adding objects

    /// Appends a single object as its own block.
    public mutating func addObject<T: Codable>(_ value: T) throws {
        let data = try core.encode(value)
        var block = Block()
        block.addObject(data)
        blocks.append(block)
    }

    /// Appends all objects into a single block.
    public mutating func addObjects<T: Codable>(_ values: [T]) throws {
        var block = Block()
        for value in values {
            block.addObject(try core.encode(value))
        }
        blocks.append(block)
    }

    /// Appends objects into blocks of at most `objectsInBlock` entries each.
    public mutating func addObjectsToBlocks<T: Codable>(
        _ values: [T],
        objectsInBlock: Int
    ) throws {
        guard objectsInBlock > 0 else { return }
        var block = Block()
        for (index, value) in values.enumerated() {
            block.addObject(try core.encode(value))
            // Flush the block when it's full, but not on the very last object
            // (the final block is appended after the loop).
            let isFull = (index + 1).isMultiple(of: objectsInBlock)
            let isLast = index == values.indices.last
            if isFull && !isLast {
                blocks.append(block)
                block = Block()
            }
        }
        if block.objectCount > 0 {
            blocks.append(block)
        }
    }

    // MARK: Encoding

    public func encodeHeader() throws -> Data {
        try core.encodeFrom(header, schema: headerSchema)
    }

    public func encodeObject() throws -> Data {
        var data = try encodeHeader()
        for block in blocks {
            data.append(try core.encodeFrom(block.objectCount, schema: longSchema))
            data.append(try core.encodeFrom(block.size,        schema: longSchema))
            data.append(block.data)
            data.append(contentsOf: header.marker)
        }
        return data
    }

    // MARK: Decoding

    public mutating func decodeFromData(from: Data) throws {
        try decodeHeader(from: from)
        let start = findMarker(from: from)
        try decodeBlock(from: from.subdata(in: start..<from.count))
    }

    public mutating func decodeHeader(from: Data) throws {
        if let hdr = try core.decodeFrom(from: from, schema: headerSchema) as Header? {
            header = hdr
        }
    }

    /// Returns the byte offset immediately after the first occurrence of the
    /// sync marker, or 0 if not found.
    public func findMarker(from: Data) -> Int {
        let marker = header.marker
        guard !marker.isEmpty, from.count >= marker.count else { return 0 }
        let searchRange = from.startIndex ..< from.index(from.endIndex, offsetBy: -marker.count + 1)
        for loc in searchRange {
            let candidate = from[loc ..< loc + marker.count]
            if candidate.elementsEqual(marker) {
                return loc + marker.count
            }
        }
        return 0
    }

    public mutating func decodeBlock(from: Data) throws {
        try from.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) throws in
            guard let baseAddress = buffer.baseAddress else { return }
            let pointer = baseAddress.assumingMemoryBound(to: UInt8.self)
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: from.count)

            var block = Block()

            // Decode object count — throws on malformed varint instead of
            // silently producing a garbage value via try?.
            let objectCount: UInt64 = try decoder.decode()
            block.objectCount = objectCount

            // Decode the byte-length of the compressed payload.
            // IMPORTANT: validate before passing to UnsafeBufferPointer —
            // a corrupt negative Int64 reinterpreted as UInt64 / Int would
            // cause "Fatal error: UnsafeBufferPointer with negative count".
            let rawLength: Int64 = try decoder.decode()
            guard rawLength >= 0 else {
                throw BinaryDecodingError.malformedAvro
            }
            let length = Int(rawLength)
            guard decoder.available >= length else {
                throw BinaryDecodingError.outOfBufferBoundary
            }

            let value: [UInt8] = try decoder.decode(fixedSize: length)
            block.data.append(contentsOf: value)
            block.size = UInt64(block.data.count)
            blocks.append(block)
        }
    }

    // MARK: Typed / untyped decode

    public func decodeObjects<T: Decodable>() throws -> [T] {
        try decodeObjectsHelper { remainingData, objectSchema in
            try core.decodeFromContinue(from: remainingData, schema: objectSchema) as (T, Int)
        }
    }

    public func decodeObjects() throws -> [Any?] {
        try decodeObjectsHelper { remainingData, objectSchema in
            try core.decodeFromContinue(from: remainingData, schema: objectSchema)
        }
    }

    private func decodeObjectsHelper<T>(
        objectDecoder: (Data, AvroSchema) throws -> (T?, Int)
    ) throws -> [T] {
        guard let objectSchema = core.decodeSchema(schema: try header.schema) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        var result: [T] = []
        for block in blocks {
            var remaining     = block.data
            var objectsDecoded = 0
            let expected       = Int(block.objectCount)

            while !remaining.isEmpty && objectsDecoded < expected {
                let (obj, decodedBytes) = try objectDecoder(remaining, objectSchema)
                remaining = remaining.dropFirst(decodedBytes)
                if let decoded = obj {
                    result.append(decoded)
                    objectsDecoded += 1
                }
            }
            guard objectsDecoded == expected else {
                throw AvroCodingError.decodingFailed(
                    "Expected \(expected) objects in block, decoded \(objectsDecoded)"
                )
            }
        }
        return result
    }
}

// MARK: - Header

public struct Header: Codable {
    private var magic: [UInt8]
    private var meta:  [String: [UInt8]]
    private var sync:  [UInt8]

    init() {
        magic = Array("Obj".utf8) + [1]           // "Obj" + version byte
        meta  = [:]
        sync  = withUnsafeBytes(of: UUID().uuid) { Array($0) }
    }

    var magicValue: [UInt8] { magic }
    var marker:     [UInt8] { sync  }

    var codec: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataCodec] else {
                throw AvroCodingError.decodingFailed("Missing codec metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

    var schema: String {
        get throws {
            guard let raw = meta[AvroReservedConstants.metaDataSchema] else {
                throw AvroCodingError.decodingFailed("Missing schema metadata")
            }
            return String(decoding: raw, as: UTF8.self)
        }
    }

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

public struct Block {
    public var objectCount: UInt64
    public var size:        UInt64
    public var data:        Data

    init() {
        objectCount = 0
        size        = 0
        data        = Data()
    }

    mutating func addObject(_ other: Data) {
        objectCount += 1
        size        += UInt64(other.count)
        data.append(other)
    }
}
