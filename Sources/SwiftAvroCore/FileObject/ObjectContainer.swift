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
    public static let metaDataSync      = "avro.sync"
    public static let metaDataCodec     = "avro.codec"
    public static let metaDataSchema    = "avro.schema"
    public static let metaDataReserved  = "avro"

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

/// A façade that owns both write and read paths for Avro Object Container Files.
///
/// Write operations: ``addObject(_:)``, ``addObjects(_:)``, ``addObjectsToBlocks(_:objectsInBlock:)``
/// Read operations: ``decodeHeader(from:)``, ``findMarker(from:)``, ``decodeBlock(from:)``, ``decodeObjects()``
///
/// Write operations are delegated to an internal ``ObjectContainerWriter``.
/// After ``decodeHeader(from:)`` is called the container switches to the
/// decoded header for all subsequent operations, so ``header.marker`` and
/// ``header.schema`` reflect what was read from the wire.
public struct ObjectContainer {

    // MARK: - Public interface

    /// The active header.
    ///
    /// Returns the header decoded from binary data when ``decodeHeader(from:)``
    /// has been called; otherwise returns the writer's freshly generated header.
    public var header: Header { decodedHeader ?? writer.header }

    /// Fully decoded blocks populated by ``decodeBlock(from:)`` or the
    /// `addObject` family of methods.
    private var _blocks: [Block]

    /// Byte length of the encoded container header.
    public var headerSize: Int { (try? writer.headerSize(context: context)) ?? 0 }

    /// Returns all accumulated blocks.
    ///
    /// Swift's copy-on-write semantics apply, so this is efficient for read-only access.
    public var blocks: [Block] { _blocks }

    // MARK: - Private state

    private var writer:        ObjectContainerWriter
    private let context:       ObjectContainerContext
    /// Set once ``decodeHeader(from:)`` successfully parses incoming data.
    private var decodedHeader: Header?

    // MARK: - Initialisation

    /// Creates a container ready for writing.
    ///
    /// - Parameters:
    ///   - schema: Avro schema JSON. Pass `nil` when the container will only be
    ///     used for reading (schema is then filled in from ``decodeHeader(from:)``).
    ///   - codec: Compression codec to use when encoding blocks.
    public init(schema: String? = nil, codec: any CodecProtocol) throws {
        let resolvedSchema = schema ?? AvroReservedConstants.dummyRecordScheme
        self.context       = try ObjectContainerContext(schema: resolvedSchema, codec: codec)
        self.writer        = try ObjectContainerWriter(context: context)
        self._blocks       = []
        self.decodedHeader = nil
    }

    // MARK: - Write path

    /// Encodes `value` and appends it as its own block.
    public mutating func addObject<T: Codable>(_ value: T) throws {
        try writer.add(value)
        writer.flushBlock()
        _blocks = writer.blocks
    }

    /// Encodes all `values` into a single block.
    public mutating func addObjects<T: Codable>(_ values: [T]) throws {
        try writer.add(values)
        writer.flushBlock()
        _blocks = writer.blocks
    }

    /// Encodes `values`, splitting into blocks of at most `objectsInBlock` entries.
    public mutating func addObjectsToBlocks<T: Codable>(
        _ values: [T],
        objectsInBlock: Int
    ) throws {
        try writer.add(values, blockSize: objectsInBlock)
        writer.flushBlock()
        _blocks = writer.blocks
    }

    /// Serialises the complete container — header followed by all blocks.
    public func encodeObject() throws -> Data {
        try writer.encode(context: context)
    }

    // MARK: - Read path

    /// Decodes the Avro container header from `data` and stores it so that
    /// ``header``, ``findMarker(from:)``, and ``decodeObjects()`` all use the
    /// values read from the wire.
    public mutating func decodeHeader(from data: Data) throws {
        let avro = Avro()
        guard let headerSchema = avro.newSchema(schema: AvroReservedConstants.headerScheme) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        decodedHeader = try avro.decodeFrom(from: data, schema: headerSchema) as Header?
    }

    /// Convenience method that decodes both the header and the first block.
    public mutating func decodeFromData(from data: Data) throws {
        try decodeHeader(from: data)
        let start = findMarker(from: data)
        try decodeBlock(from: data.subdata(in: start..<data.count))
    }

    /// Returns the byte offset immediately after the sync marker in `data`,
    /// or `0` if the marker is not found.
    public func findMarker(from data: Data) -> Int {
        let marker = header.marker
        guard !marker.isEmpty, data.count >= marker.count else { return 0 }
        let limit = data.index(data.endIndex, offsetBy: -marker.count + 1)
        for loc in data.startIndex..<limit {
            if data[loc..<(loc + marker.count)].elementsEqual(marker) {
                return loc + marker.count
            }
        }
        return 0
    }

    /// Decodes one block from `data` (starting at offset 0) and appends it to
    /// ``blocks``.
    public mutating func decodeBlock(from data: Data) throws {
        try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) throws in
            guard let base = buffer.baseAddress else { return }
            let prim = AvroPrimitiveDecoder(
                pointer: base.assumingMemoryBound(to: UInt8.self),
                size: data.count)

            var block          = Block()
            block.objectCount  = try prim.decode() as UInt64

            let rawLength: Int64 = try prim.decode()
            guard rawLength >= 0 else { throw BinaryDecodingError.malformedAvro }
            let length = Int(rawLength)
            guard prim.available >= length else { throw BinaryDecodingError.outOfBufferBoundary }

            block.data.append(contentsOf: try prim.decode(fixedSize: length))
            block.size = UInt64(block.data.count)
            _blocks.append(block)
        }
    }
/*
    // MARK: - Object decoding

    /// Decodes all objects in ``blocks`` as `T`.
    public func decodeObjects<T: Decodable>() throws -> [T] {
        try decodeObjectsHelper { remaining, schema in
            try Avro().decodeFromContinue(from: remaining, schema: schema) as (T, Int)
        }
    }

    /// Decodes all objects in ``blocks`` as untyped `Any?` values.
    public func decodeObjects() throws -> [Any?] {
        try decodeObjectsHelper { remaining, schema in
            try Avro().decodeFromContinue(from: remaining, schema: schema)
        }
    }
*/
    // MARK: - Private helpers

    private func decodeObjectsHelper<T>(
        objectDecoder: (Data, AvroSchema) throws -> (T?, Int)
    ) throws -> [T] {
        let schemaString: String
        do    { schemaString = try header.schema }
        catch { throw AvroSchemaDecodingError.unknownSchemaJsonFormat }

        let avro = Avro()
        guard let objectSchema = avro.decodeSchema(schema: schemaString) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }

        var result: [T] = []
        for block in _blocks {
            var remaining      = block.data
            var objectsDecoded = 0
            let expected       = Int(block.objectCount)

            while !remaining.isEmpty, objectsDecoded < expected {
                let (obj, consumed) = try objectDecoder(remaining, objectSchema)
                remaining = remaining.dropFirst(consumed)
                if let decoded = obj {
                    result.append(decoded)
                    objectsDecoded += 1
                }
            }
            guard objectsDecoded == expected else {
                throw AvroCodingError.decodingFailed(
                    "Expected \(expected) objects in block, decoded \(objectsDecoded)")
            }
        }
        return result
    }
}
