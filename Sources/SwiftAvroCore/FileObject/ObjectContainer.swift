//
//  ObjectContainer.swift
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
    public static let metaDataSync     = "avro.sync"
    public static let metaDataCodec    = "avro.codec"
    public static let metaDataSchema   = "avro.schema"
    public static let metaDataReserved = "avro"

    public static let nullCodec    = "null"
    public static let deflateCodec = "deflate"
    public static let xzCodec      = "xz"
    public static let lzfseCodec   = "lzfse"
    public static let lz4Codec     = "lz4"

    public static let syncSize            = 16
    public static let defaultSyncInterval = 4000 * 16

    public static let longScheme   = #"{"type":"long"}"#
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

    public static let dummyRecordScheme = """
        {"type":"record","name":"xx","fields":[]}
        """
}

// MARK: - ObjectContainer

/// An Avro Object Container File — a header plus a sequence of data blocks.
///
/// `ObjectContainer` is a plain value type: it holds only a ``Header`` and
/// an array of ``Block`` values. All I/O concerns (schema parsing, compression,
/// binary framing) are handled by the methods that accept an `Avro` instance
/// and a `CodecProtocol`.
///
/// **Writing**
/// ```swift
/// var container = ObjectContainer(schema: mySchemaJson)
/// try container.addObject(record,  avro: avro)
/// try container.addObjects(records, avro: avro)
/// try container.addObjectsToBlocks(records, objectsInBlock: 100, avro: avro)
/// let data = try container.encode(avro: avro, codec: DeflateCodec())
/// ```
///
/// **Reading**
/// ```swift
/// var container = ObjectContainer()
/// try container.decode(from: data, avro: avro, codec: NullCodec())
/// let records: [MyRecord] = try container.decodeAll(avro: avro)
/// ```
public struct ObjectContainer {

    // MARK: - State

    /// The container file header (magic, meta, sync marker).
    public private(set) var header: Header

    /// All flushed data blocks.
    public private(set) var blocks: [Block]

    /// The block currently being filled (flushed on ``encode`` or explicit ``flush``).
    private var currentBlock: Block

    // MARK: - Initialisation

    /// Creates an empty container.
    ///
    /// - Parameters:
    ///   - schema: Avro schema JSON for the objects that will be written.
    ///     Pass `nil` when the container will only be used for reading — the
    ///     schema is read from the header during ``decode(from:avro:codec:)``.
    ///   - syncMarker: 16-byte sync marker written after the header and after
    ///     every data block. When `nil` (the default), a random marker is
    ///     generated automatically. Supply an explicit marker when you need
    ///     deterministic output (e.g. in tests). ``encode(avro:codec:)`` throws
    ///     ``AvroContainerError/invalidSyncMarkerLength(_:)`` if a supplied
    ///     marker is not exactly 16 bytes.
    public init(schema: String? = nil, syncMarker: [UInt8]? = nil) {
        // Read-only containers (no schema) leave sync empty; it's populated by decode.
        let marker = syncMarker ?? (schema != nil
            ? (0..<AvroReservedConstants.syncSize).map { _ in UInt8.random(in: 0...255) }
            : [])
        var hdr = Header(syncMarker: marker)
        if let schema {
            hdr.setSchema(jsonSchema: schema)
            hdr.setCodec(codec: AvroReservedConstants.nullCodec)
        }
        self.header       = hdr
        self.blocks       = []
        self.currentBlock = Block()
    }

    // MARK: - Write: adding objects

    /// Encodes `value` and appends it to the current block.
    ///
    /// Call ``flush()`` or ``encode(avro:codec:)`` to commit the block.
    public mutating func addObject<T: Encodable>(_ value: T, avro: Avro) throws {
        currentBlock.addObject(try avro.encode(value))
    }

    /// Encodes all `values` into the current block.
    public mutating func addObjects<T: Encodable>(_ values: [T], avro: Avro) throws {
        for value in values {
            currentBlock.addObject(try avro.encode(value))
        }
    }

    /// Encodes `values`, flushing to a new block every `objectsInBlock` objects.
    public mutating func addObjectsToBlocks<T: Encodable>(
        _ values:       [T],
        objectsInBlock: Int,
        avro:           Avro
    ) throws {
        guard objectsInBlock > 0 else { return }
        for (index, value) in values.enumerated() {
            currentBlock.addObject(try avro.encode(value))
            let isFull = (index + 1).isMultiple(of: objectsInBlock)
            let isLast = index == values.indices.last
            if isFull && !isLast { flush() }
        }
    }

    /// Moves the current block into ``blocks`` and starts a new one.
    ///
    /// Called automatically by ``encode(avro:codec:)``. Use explicitly when
    /// you want manual control over block boundaries.
    public mutating func flush() {
        guard currentBlock.objectCount > 0 else { return }
        blocks.append(currentBlock)
        currentBlock = Block()
    }

    // MARK: - Write: serialisation

    /// Serialises the complete container — header followed by all blocks —
    /// using `codec` to compress each block's payload.
    ///
    /// The current (unflushed) block is included automatically.
    public mutating func encode(avro: Avro, codec: any CodecProtocol) throws -> Data {
        flush()

        guard let headerSchema = avro.newSchema(schema: AvroReservedConstants.headerScheme),
              let longSchema   = avro.newSchema(schema: AvroReservedConstants.longScheme)
        else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }

        guard header.marker.count == AvroReservedConstants.syncSize else {
            throw AvroContainerError.invalidSyncMarkerLength(header.marker.count)
        }

        header.setCodec(codec: codec.name)

        var out = try avro.encodeFrom(header, schema: headerSchema)
        for block in blocks {
            let compressed = try codec.compress(data: block.data)
            out.append(try avro.encodeFrom(block.objectCount,        schema: longSchema))
            out.append(try avro.encodeFrom(UInt64(compressed.count), schema: longSchema))
            out.append(compressed)
            out.append(contentsOf: header.marker)
        }
        return out
    }

    // MARK: - Read: deserialisation

    /// Decodes a complete container from `data`, populating ``header`` and ``blocks``.
    ///
    /// - Parameters:
    ///   - data:  Raw bytes of the `.avro` file.
    ///   - avro:  `Avro` instance used for binary decoding.
    ///   - codec: Codec used to decompress block payloads. Must match the codec
    ///            named in the container header.
    public mutating func decode(from data: Data, avro: Avro, codec: any CodecProtocol) throws {
        guard let headerSchema = avro.newSchema(schema: AvroReservedConstants.headerScheme),
              let longSchema   = avro.newSchema(schema: AvroReservedConstants.longScheme)
        else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }

        let reader = avro.makeDataReader(data: data)

        header = try reader.decode(schema: headerSchema)

        blocks = []
        while !reader.isAtEnd {
            let objectCount: UInt64 = try reader.decode(schema: longSchema)
            let byteCount:   UInt64 = try reader.decode(schema: longSchema)
            let compressed          = try reader.readBytes(count: Int(byteCount))
            let blockMarker         = try reader.readBytes(count: AvroReservedConstants.syncSize)
            guard blockMarker.elementsEqual(header.marker) else {
                throw AvroContainerError.syncMarkerMismatch(blockIndex: blocks.count)
            }
            let decompressed = try codec.decompress(data: compressed)
            blocks.append(Block(count: objectCount, data: decompressed))
        }
    }

    // MARK: - Read: object decoding

    /// Decodes all objects of type `T` from ``blocks``.
    public func decodeAll<T: Decodable>(_ type: T.Type = T.self, avro: Avro) throws -> [T] {
        guard let schemaJson = try? header.schema,
              let avroSchema = avro.newSchema(schema: schemaJson)
        else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }

        return try blocks.flatMap { block -> [T] in
            let reader = avro.makeDataReader(data: block.data)
            var items: [T] = []
            while !reader.isAtEnd {
                items.append(try reader.decode(schema: avroSchema))
            }
            return items
        }
    }

    /// Decodes all objects as untyped `Any?` values from ``blocks``.
    public func decodeAll(avro: Avro) throws -> [Any?] {
        guard let schemaJson = try? header.schema,
              let avroSchema = avro.newSchema(schema: schemaJson)
        else { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }

        return try blocks.flatMap { block -> [Any?] in
            let reader = avro.makeDataReader(data: block.data)
            var items: [Any?] = []
            while !reader.isAtEnd {
                items.append(try reader.decode(schema: avroSchema))
            }
            return items
        }
    }
}
