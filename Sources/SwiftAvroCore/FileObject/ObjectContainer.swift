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
//
// Backward-compatible façade that preserves the existing API used by tests
// and callers while delegating write operations to ObjectContainerWriter and
// using ObjectContainerContext for schema/codec ownership.
//
// The read path (decodeHeader / findMarker / decodeBlock / decodeObjects) is
// kept here because it has no analogue in the new writer type.

public struct ObjectContainer {

    // MARK: - Public state

    public var header: Header { writer.header }
    public var blocks: [Block]

    // MARK: - Private

    private var writer:  ObjectContainerWriter
    private let context: ObjectContainerContext

    // MARK: - Init

    /// Creates a container with an optional schema and a codec.
    /// When `schema` is `nil` the dummy record schema is used; the real schema
    /// is filled in after ``decodeHeader(from:)`` is called on a reader.
    public init(schema: String? = nil, codec: any CodecProtocol) throws {
        let resolvedSchema = schema ?? AvroReservedConstants.dummyRecordScheme
        self.context = try ObjectContainerContext(schema: resolvedSchema, codec: codec)
        self.writer  = try ObjectContainerWriter(context: context)
        self.blocks  = []
    }

    // MARK: - Header size

    public var headerSize: Int {
        (try? writer.headerSize(context: context)) ?? 0
    }

    // MARK: - Adding objects (delegate to writer)

    /// Appends a single object as its own block.
    public mutating func addObject<T: Codable>(_ value: T) throws {
        try writer.add(value)
        // Flush immediately so each addObject call produces one block,
        // matching the original ObjectContainer behaviour.
        writer.flushBlock()
        blocks = writer.blocks
    }

    /// Appends all objects into a single block.
    public mutating func addObjects<T: Codable>(_ values: [T]) throws {
        try writer.add(values)
        writer.flushBlock()
        blocks = writer.blocks
    }

    /// Appends objects, splitting into blocks of at most `objectsInBlock` entries.
    public mutating func addObjectsToBlocks<T: Codable>(
        _ values: [T],
        objectsInBlock: Int
    ) throws {
        try writer.add(values, blockSize: objectsInBlock)
        writer.flushBlock()
        blocks = writer.blocks
    }

    // MARK: - Encoding (delegate to writer)

    public func encodeObject() throws -> Data {
        try writer.encode(context: context)
    }

    // MARK: - Decoding (read path — kept here, no writer equivalent)

    public mutating func decodeFromData(from: Data) throws {
        try decodeHeader(from: from)
        let start = findMarker(from: from)
        try decodeBlock(from: from.subdata(in: start..<from.count))
    }

    public mutating func decodeHeader(from: Data) throws {
        let avro = Avro()
        guard let headerSchema = avro.newSchema(schema: AvroReservedConstants.headerScheme) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        if let hdr = try avro.decodeFrom(from: from, schema: headerSchema) as Header? {
            // Store decoded header for the read path by keeping a local copy.
            self._decodedHeader = hdr
        }
    }

    /// Decoded header from a read; nil until ``decodeHeader(from:)`` is called.
    private var _decodedHeader: Header?

    /// The effective header: decoded header if available, otherwise the writer's header.
    private var effectiveHeader: Header {
        _decodedHeader ?? writer.header
    }

    /// Returns the byte offset immediately after the first occurrence of the sync marker,
    /// or 0 if not found.
    public func findMarker(from: Data) -> Int {
        let marker = effectiveHeader.marker
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

            let objectCount: UInt64 = try decoder.decode()
            block.objectCount = objectCount

            // Validate length before passing to UnsafeBufferPointer.
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

    // MARK: - Typed / untyped decode

    public func decodeObjects<T: Decodable>() throws -> [T] {
        try decodeObjectsHelper { remaining, objectSchema in
            try decodeAvro().decodeFromContinue(from: remaining, schema: objectSchema) as (T, Int)
        }
    }

    public func decodeObjects() throws -> [Any?] {
        try decodeObjectsHelper { remaining, objectSchema in
            try decodeAvro().decodeFromContinue(from: remaining, schema: objectSchema)
        }
    }

    // MARK: - Private helpers

    private func decodeAvro() -> Avro { Avro() }

    private func decodeObjectsHelper<T>(
        objectDecoder: (Data, AvroSchema) throws -> (T?, Int)
    ) throws -> [T] {
        let schemaString: String
        do {
            schemaString = try effectiveHeader.schema
        } catch {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        let avro = Avro()
        guard let objectSchema = avro.decodeSchema(schema: schemaString) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        var result: [T] = []
        for block in blocks {
            var remaining      = block.data
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
                    "Expected \(expected) objects in block, decoded \(objectsDecoded)")
            }
        }
        return result
    }
}
