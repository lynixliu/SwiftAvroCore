//
//  ObjectContainerWriter.swift
//  SwiftAvroCore
//
//  Created by Yang Liu on 06/04/2026.
//
//  Encodes Avro objects into the Avro Object Container File format.
//  Separated from ObjectContainerReader so each type has a single responsibility.

import Foundation

// MARK: - ObjectContainerWriter

/// Encodes Avro objects into a container file.
///
/// Call ``add(_:)`` (or its variants) to accumulate records into the current
/// block, then call ``encode(context:)`` to serialise the complete container.
///
/// A writer is lightweight and cheap to create — all expensive schema work
/// is done once in ``ObjectContainerContext``.
///
/// ## Usage
/// ```swift
/// let context = try ObjectContainerContext(schema: mySchema, codec: NullCodec())
/// var writer  = try ObjectContainerWriter(context: context)
/// try writer.add(myRecords)
/// let fileData = try writer.encode(context: context)
/// ```
public struct ObjectContainerWriter {

    // MARK: - Stored state

    /// The file header written at the start of the container.
    public private(set) var header: Header

    /// Fully flushed blocks, ready for serialisation.
    public private(set) var blocks: [Block]

    /// The block currently being filled.
    private var currentBlock: Block

    /// Avro instance used to encode individual objects.
    /// A single instance is reused across all `add` calls so the schema is
    /// parsed only once (in the context) rather than on every encode.
    private let avro: Avro

    // MARK: - Init

    /// Creates a writer for the given context.
    ///
    /// Throws ``AvroSchemaDecodingError`` if the schema in `context` is malformed,
    /// though in practice the context validates the schema at its own init time,
    /// so this init should not throw under normal use.
    public init(context: ObjectContainerContext) throws {
        self.avro         = Avro()
        self.blocks       = []
        self.currentBlock = Block()

        // Activate the object schema on our dedicated Avro instance.
        guard avro.decodeSchema(schema: context.schema) != nil else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }

        var hdr = Header()
        hdr.setSchema(jsonSchema: context.schema)
        hdr.setCodec(codec: context.codecName)
        self.header = hdr
    }

    // MARK: - Querying

    /// Returns the encoded byte size of the container header.
    public func headerSize(context: ObjectContainerContext) throws -> Int {
        try encodeHeader(context: context).count
    }

    /// Total number of objects accumulated across all blocks including the current block.
    public var objectCount: Int {
        let flushed = blocks.reduce(0) { $0 + Int($1.objectCount) }
        return flushed + Int(currentBlock.objectCount)
    }

    // MARK: - Adding objects

    /// Appends a single object to the current block.
    public mutating func add<T: Encodable>(_ value: T) throws {
        currentBlock.addObject(try avro.encode(value))
    }

    /// Appends all values in `values` to the current block.
    public mutating func add<T: Encodable>(_ values: [T]) throws {
        for value in values {
            currentBlock.addObject(try avro.encode(value))
        }
    }

    /// Appends `values`, flushing to a new block every `blockSize` objects.
    ///
    /// This is useful when you want fine-grained control over block boundaries,
    /// e.g. to limit memory usage or allow partial reads on large files.
    ///
    /// - Parameters:
    ///   - values: Objects to encode.
    ///   - blockSize: Maximum number of objects per block. Must be > 0; silently
    ///     ignored (no-op) if it is not.
    public mutating func add<T: Encodable>(_ values: [T], blockSize: Int) throws {
        guard blockSize > 0 else { return }
        for (index, value) in values.enumerated() {
            currentBlock.addObject(try avro.encode(value))
            let isFull = (index + 1).isMultiple(of: blockSize)
            let isLast = index == values.indices.last
            if isFull && !isLast {
                flushCurrentBlock()
            }
        }
    }

    /// Explicitly flushes the current block into `blocks` and starts a new one.
    ///
    /// Normally you do not need to call this — ``encode(context:)`` flushes
    /// automatically. Use it when you want manual control over block boundaries.
    public mutating func flushBlock() {
        guard currentBlock.objectCount > 0 else { return }
        flushCurrentBlock()
    }

    // MARK: - Serialisation

    /// Serialises all accumulated objects into a complete Avro container file.
    ///
    /// - The file header is always written uncompressed (Avro spec §4).
    /// - Each block's payload is compressed using `context.codec`.
    /// - The current (unflushed) block is included automatically.
    ///
    /// - Parameter context: The shared context supplying the codec and schemas.
    /// - Returns: A `Data` value containing the complete `.avro` file.
    public func encode(context: ObjectContainerContext) throws -> Data {
        // Collect flushed blocks plus the current (possibly partial) block.
        var allBlocks = blocks
        if currentBlock.objectCount > 0 {
            allBlocks.append(currentBlock)
        }

        var out = try encodeHeader(context: context)
        for block in allBlocks {
            try out.append(encodedBlock(block, context: context))
        }
        return out
    }

    // MARK: - Private helpers

    /// Moves `currentBlock` into `blocks` and resets `currentBlock`.
    private mutating func flushCurrentBlock() {
        blocks.append(currentBlock)
        currentBlock = Block()
    }

    /// Encodes the container header using the Avro header schema.
    private func encodeHeader(context: ObjectContainerContext) throws -> Data {
        try avro.encodeFrom(header, schema: context.headerSchema)
    }

    /// Encodes a single block: objectCount | byteCount | compressedData | syncMarker
    private func encodedBlock(_ block: Block, context: ObjectContainerContext) throws -> Data {
        let compressed = try context.codec.compress(data: block.data)
        var out = Data()
        out.append(try avro.encodeFrom(block.objectCount,        schema: context.longSchema))
        out.append(try avro.encodeFrom(UInt64(compressed.count), schema: context.longSchema))
        out.append(compressed)
        out.append(contentsOf: header.marker)
        return out
    }
}
