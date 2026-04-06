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

// MARK: - ObjectContainerReader

/// Decodes Avro objects from a container file format.
/// Call ``decodeHeader(from:context:)`` first, then ``decodeBlock(from:context:)``
/// for each block, then ``decodeObjects(context:)`` to retrieve typed values.
public struct ObjectContainerReader {
    public private(set) var header: Header
    public private(set) var blocks: [Block]
    private let core: Avro

    /// Creates an empty reader. Call ``decodeHeader(from:context:)`` before
    /// attempting to decode any blocks or objects.
    public init() {
        self.core   = Avro()
        self.header = Header()
        self.blocks = []
    }

    // MARK: Decoding

    /// Decodes the container header from the given data.
    /// Must be called before ``findMarker(from:)`` or ``decodeBlock(from:context:)``.
    /// Throws if the Avro magic bytes are missing or the header is malformed.
    public mutating func decodeHeader(from data: Data, context: ObjectContainerContext) throws {
        let magic: [UInt8] = [0x4F, 0x62, 0x6A, 0x01]
        guard data.count >= magic.count,
              Array(data.prefix(magic.count)) == magic else {
            throw BinaryDecodingError.malformedAvro
        }
        if let hdr = try core.decodeFrom(from: data, schema: context.headerSchema) as Header? {
            header = hdr
        }
    }

    /// Returns the byte offset immediately after the first sync marker in `data`,
    /// or 0 if the marker is not found. Call after ``decodeHeader(from:context:)``.
    public func findMarker(from data: Data) -> Int {
        let marker = header.marker
        guard !marker.isEmpty, data.count >= marker.count else { return 0 }
        let searchRange = data.startIndex ..< data.index(data.endIndex, offsetBy: -marker.count + 1)
        for loc in searchRange {
            let candidate = data[loc ..< loc + marker.count]
            if candidate.elementsEqual(marker) {
                return loc + marker.count
            }
        }
        return 0
    }

    /// Decodes a single block from the given data slice.
    /// Block data is decompressed using the codec from `context` after reading.
    /// Call after ``decodeHeader(from:context:)`` with data starting at the
    /// sync marker offset returned by ``findMarker(from:)``.
    public mutating func decodeBlock(from data: Data, context: ObjectContainerContext) throws {
        try data.withUnsafeBytes { (buffer: UnsafeRawBufferPointer) throws in
            guard let baseAddress = buffer.baseAddress else { return }
            let pointer = baseAddress.assumingMemoryBound(to: UInt8.self)
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: data.count)

            var block = Block()

            let objectCount: UInt64 = try decoder.decode()
            block.objectCount = objectCount

            let rawLength: Int64 = try decoder.decode()
            guard rawLength >= 0 else {
                throw BinaryDecodingError.malformedAvro
            }
            let length = Int(rawLength)
            guard decoder.available >= length else {
                throw BinaryDecodingError.outOfBufferBoundary
            }

            let compressed: [UInt8] = try decoder.decode(fixedSize: length)
            let decompressed = try context.codec.decompress(data: Data(compressed))
            block.data        = decompressed
            block.size        = UInt64(decompressed.count)
            block.objectCount = objectCount
            blocks.append(block)
        }
    }

    /// Convenience: decodes the header and first block from a complete container file.
    public mutating func decodeFromData(from data: Data, context: ObjectContainerContext) throws {
        try decodeHeader(from: data, context: context)
        let start = findMarker(from: data)
        try decodeBlock(from: data.subdata(in: start ..< data.count), context: context)
    }

    // MARK: Typed / untyped decode

    /// Decodes all accumulated block data into typed objects.
    /// Must be called after one or more ``decodeBlock(from:context:)`` calls.
    public func decodeObjects<T: Decodable>(context: ObjectContainerContext) throws -> [T] {
        try decodeObjectsHelper(context: context) { remainingData, objectSchema in
            try core.decodeFromContinue(from: remainingData, schema: objectSchema) as (T, Int)
        }
    }

    /// Decodes all accumulated block data into untyped values.
    /// Must be called after one or more ``decodeBlock(from:context:)`` calls.
    public func decodeObjects(context: ObjectContainerContext) throws -> [Any?] {
        try decodeObjectsHelper(context: context) { remainingData, objectSchema in
            try core.decodeFromContinue(from: remainingData, schema: objectSchema)
        }
    }

    private func decodeObjectsHelper<T>(
        context: ObjectContainerContext,
        objectDecoder: (Data, AvroSchema) throws -> (T?, Int)
    ) throws -> [T] {
        guard let objectSchema = core.decodeSchema(schema: try header.schema) else {
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
                    "Expected \(expected) objects in block, decoded \(objectsDecoded)"
                )
            }
        }
        return result
    }
}
