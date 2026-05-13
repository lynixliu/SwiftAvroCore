//
//  FileObject/AvroFileObjectContainer.swift
//  SwiftAvroCore
//

import Foundation

// MARK: - AvroFileObjectContainer

/// Actor that reads and writes Avro Object Container Files.
///
/// Wraps ``ObjectContainer``, isolating all mutable state behind actor serialisation.
/// The codec is injected at construction time; use ``NullCodec`` for uncompressed files
/// or any ``CodecProtocol``-conforming type for compression.
///
/// Platform-specific codecs (deflate, lz4, lzfse via Apple's `Compression` framework)
/// are provided by `SwiftAvroRpc`.
public actor AvroFileObjectContainer {
    private var container: ObjectContainer
    private let codec: any CodecProtocol

    /// Creates an uncompressed container (``NullCodec``).
    ///
    /// - Parameters:
    ///   - schema: The Avro schema JSON string describing the record type.
    ///   - syncMarker: 16-byte sync marker. Must be exactly 16 bytes.
    /// - Throws: ``AvroContainerError/encodingFailed(_:)`` if the sync marker length is wrong.
    public init(schema: String, syncMarker: [UInt8]) throws {
        try self.init(schema: schema, codec: NullCodec(), syncMarker: syncMarker)
    }

    /// Creates a container with a directly injected codec.
    ///
    /// - Parameters:
    ///   - schema: The Avro schema JSON string describing the record type.
    ///   - codec: Any ``CodecProtocol``-conforming value. Use ``NullCodec`` for no compression.
    ///   - syncMarker: 16-byte sync marker. Must be exactly 16 bytes.
    /// - Throws: ``AvroContainerError/encodingFailed(_:)`` if the sync marker length is wrong.
    public init(
        schema: String,
        codec: any CodecProtocol,
        syncMarker: [UInt8]
    ) throws {
        guard syncMarker.count == AvroReservedConstants.syncSize else {
            throw AvroContainerError.encodingFailed(
                "syncMarker must be exactly \(AvroReservedConstants.syncSize) bytes"
            )
        }
        self.codec     = codec
        self.container = ObjectContainer(schema: schema, syncMarker: syncMarker)
    }

    // MARK: - Write

    /// Appends a single object to the current pending block.
    public func addObject<T: Codable & Sendable>(_ value: T) throws {
        do {
            try container.addObject(value, avro: Avro())
        } catch {
            throw AvroContainerError.encodingFailed(error.localizedDescription)
        }
    }

    /// Appends multiple objects, distributing them into blocks of `objectsPerBlock` each.
    public func addObjectsToBlocks<T: Codable & Sendable>(
        _ values: [T],
        objectsPerBlock: Int = AvroReservedConstants.defaultSyncInterval
    ) throws {
        do {
            try container.addObjectsToBlocks(
                values,
                objectsInBlock: objectsPerBlock,
                avro: Avro()
            )
        } catch {
            throw AvroContainerError.encodingFailed(error.localizedDescription)
        }
    }

    /// Encodes all objects and returns a complete Avro container file as `Data`.
    public func write<T: Codable & Sendable>(objects: [T]) throws -> Data {
        do {
            for obj in objects {
                try container.addObject(obj, avro: Avro())
            }
        } catch {
            throw AvroContainerError.encodingFailed(error.localizedDescription)
        }
        return try flush()
    }

    /// Serialises the current pending block to `Data`.
    public func flush() throws -> Data {
        do {
            return try container.encode(avro: Avro(), codec: codec)
        } catch {
            throw AvroContainerError.encodingFailed(error.localizedDescription)
        }
    }

    // MARK: - Read

    /// Parses a complete container file and returns all decoded objects.
    public func read<T: Codable & Sendable>(
        from data: Data,
        as type: T.Type
    ) throws -> [T] {
        do {
            let avro = Avro()
            try container.decode(from: data, avro: avro, codec: codec)
            return try container.decodeAll(T.self, avro: avro)
        } catch {
            throw AvroContainerError.decodingFailed(error.localizedDescription)
        }
    }

    /// Returns an `AsyncThrowingStream` that yields one decoded object at a time.
    public func stream<T: Codable & Sendable>(
        from data: Data,
        as type: T.Type
    ) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    let objects = try self.read(from: data, as: type)
                    for object in objects { continuation.yield(object) }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Parses a container file with schema evolution.
    ///
    /// Fields absent in the writer schema are filled from defaults in `readerSchemaJson`;
    /// extra writer fields are discarded.
    public func read<T: Codable & Sendable>(
        from data: Data,
        as type: T.Type,
        readerSchema readerSchemaJson: String
    ) throws -> [T] {
        let avro = Avro()
        guard let readerSchema = avro.newSchema(schema: readerSchemaJson) else {
            throw AvroContainerError.decodingFailed("Invalid reader schema")
        }
        do {
            try container.decode(from: data, avro: avro, codec: codec)
            return try container.decodeAll(T.self, avro: avro, readerSchema: readerSchema)
        } catch let e as AvroContainerError {
            throw e
        } catch {
            throw AvroContainerError.decodingFailed(error.localizedDescription)
        }
    }

    /// Returns an `AsyncThrowingStream` that yields one schema-evolved object at a time.
    public func stream<T: Codable & Sendable>(
        from data: Data,
        as type: T.Type,
        readerSchema readerSchemaJson: String
    ) -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    let objects = try self.read(from: data, as: type, readerSchema: readerSchemaJson)
                    for object in objects { continuation.yield(object) }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }
}
