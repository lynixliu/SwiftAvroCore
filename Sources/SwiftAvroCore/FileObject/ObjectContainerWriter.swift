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

// MARK: - ObjectContainerWriter

/// Encodes Avro objects into a container file format.
/// Call ``addObject(_:)`` or ``addObjects(_:)`` to accumulate records,
/// then ``encodeObject(context:)`` to serialise the complete container.
public struct ObjectContainerWriter {
    public private(set) var header: Header
    public private(set) var blocks: [Block]
    private var currentBlock: Block
    private let core: SwiftAvroCore

    /// Creates a writer for the given schema and codec.
    /// The schema must be a valid Avro JSON schema string.
    public init(context: ObjectContainerContext) throws {
        self.core         = SwiftAvroCore()
        self.blocks       = []
        self.currentBlock = Block()

        var hdr = Header()
        hdr.setSchema(jsonSchema: context.schema)
        hdr.setCodec(codec: context.codec.name)
        self.header = hdr

        guard core.decodeSchema(schema: context.schema) != nil else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    /// Returns the encoded size of the container header in bytes.
    public func headerSize(context: ObjectContainerContext) -> Int {
        (try? encodeHeader(context: context).count) ?? 0
    }

    // MARK: Adding objects

    /// Appends a single encodable object to the current pending block.
    public mutating func addObject<T: Codable>(_ value: T) throws {
        let data = try core.encode(value)
        currentBlock.addObject(data)
    }

    /// Appends all objects into the current pending block.
    public mutating func addObjects<T: Codable>(_ values: [T]) throws {
        for value in values {
            currentBlock.addObject(try core.encode(value))
        }
    }

    /// Appends objects into blocks of at most `objectsInBlock` entries each.
    /// Each full block is flushed and a new block started before the next batch.
    public mutating func addObjectsToBlocks<T: Codable>(
        _ values: [T],
        objectsInBlock: Int
    ) throws {
        guard objectsInBlock > 0 else { return }
        for (index, value) in values.enumerated() {
            currentBlock.addObject(try core.encode(value))
            let isFull = (index + 1).isMultiple(of: objectsInBlock)
            let isLast = index == values.indices.last
            if isFull && !isLast {
                blocks.append(currentBlock)
                currentBlock = Block()
            }
        }
    }

    // MARK: Encoding

    /// Serialises the container header to `Data`.
    /// The header is always written uncompressed per the Avro specification.
    func encodeHeader(context: ObjectContainerContext) throws -> Data {
        try core.encodeFrom(header, schema: context.headerSchema)
    }

    /// Serialises all accumulated objects into a complete Avro container file.
    /// Block data is compressed using the codec from `context` before writing.
    /// The header is always written uncompressed per the Avro specification.
    public func encodeObject(context: ObjectContainerContext) throws -> Data {
        var allBlocks = blocks
        if currentBlock.objectCount > 0 {
            allBlocks.append(currentBlock)
        }
        var data = try encodeHeader(context: context)
        for block in allBlocks {
            let compressed     = try context.codec.compress(data: block.data)
            let compressedSize = UInt64(compressed.count)
            data.append(try core.encodeFrom(block.objectCount, schema: context.longSchema))
            data.append(try core.encodeFrom(compressedSize,    schema: context.longSchema))
            data.append(compressed)
            data.append(contentsOf: header.marker)
        }
        return data
    }
}
