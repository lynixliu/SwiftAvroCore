//
//  AvroFileObjectContainerTests.swift
//  SwiftAvroRpc
//
//  Created by Yang Liu on 05/04/2026.
//

import Testing
import Foundation
import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - Helper

func makeSyncMarker() -> [UInt8] {
    (0..<16).map { _ in UInt8.random(in: 0...255) }
}

@Suite("AvroFileObjectContainer")
struct AvroFileObjectContainerTests {

    // MARK: Init

    @Test("Init succeeds with null codec")
    func initNullCodec() throws {
        #expect(throws: Never.self) {
            try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        }
    }

    #if canImport(Compression)
    @Test("Init succeeds with deflate codec")
    func initDeflateCodec() throws {
        #expect(throws: Never.self) {
            try AvroFileObjectContainer(
                schema: sensorSchema,
                codec: try CodecFactory.make(named: AvroReservedConstants.deflateCodec),
                syncMarker: makeSyncMarker()
            )
        }
    }
    #endif

    // MARK: Write

    @Test("Write single object produces non-empty data")
    func writeSingleObject() async throws {
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let data = try await container.write(objects: [SimpleRecord(a: 42, b: "hello")])
        #expect(!data.isEmpty)
    }

    @Test("Write produces Avro magic bytes")
    func writeMagicBytes() async throws {
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let data = try await container.write(objects: [SimpleRecord(a: 1, b: "test")])
        #expect(Array(data.prefix(4)) == [0x4F, 0x62, 0x6A, 0x01])
    }

    @Test("Write multiple objects produces non-empty data")
    func writeMultipleObjects() async throws {
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let records = (0..<10).map { SimpleRecord(a: Int64($0), b: "record-\($0)") }
        let data = try await container.write(objects: records)
        #expect(!data.isEmpty)
    }

    #if canImport(Compression)
    @Test("Write with each codec produces non-empty data",
          arguments: [AvroReservedConstants.nullCodec,
                      AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func writeWithCodec(codecName: String) async throws {
        let original = SensorReading(id: 1, temperature: 22.5, location: "Auckland")
        let codec = try CodecFactory.make(named: codecName)
        let container = try AvroFileObjectContainer(schema: sensorSchema, codec: codec, syncMarker: makeSyncMarker())
        let encoded = try await container.write(objects: [original])

        #expect(!encoded.isEmpty)
        #expect(Array(encoded.prefix(4)) == [0x4F, 0x62, 0x6A, 0x01])

        let reader = try AvroFileObjectContainer(schema: sensorSchema, codec: codec, syncMarker: makeSyncMarker())
        let decoded: [SensorReading] = try await reader.read(from: encoded, as: SensorReading.self)
        #expect(decoded.count == 1)
        #expect(decoded[0] == original)
    }
    #endif

    // MARK: addObject + flush

    @Test("addObject then flush produces non-empty data")
    func addObjectThenFlush() async throws {
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        try await container.addObject(SimpleRecord(a: 1, b: "one"))
        try await container.addObject(SimpleRecord(a: 2, b: "two"))
        let data = try await container.flush()
        #expect(!data.isEmpty)
    }

    // MARK: Round-trips

    @Test("Round-trip single record with null codec")
    func roundTripSingleNull() async throws {
        let original = [SimpleRecord(a: 99, b: "avro")]
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let encoded = try await container.write(objects: original)

        let decoded: [SimpleRecord] = try await container.read(from: encoded, as: SimpleRecord.self)
        #expect(decoded == original)
    }

    @Test("Round-trip multiple records with null codec")
    func roundTripMultipleNull() async throws {
        let original = (0..<20).map { SimpleRecord(a: Int64($0), b: "item-\($0)") }
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let encoded = try await container.write(objects: original)

        let decoded: [SimpleRecord] = try await container.read(from: encoded, as: SimpleRecord.self)
        #expect(decoded == original)
    }

    #if canImport(Compression)
    @Test("Round-trip sensor records with each codec",
          arguments: [AvroReservedConstants.nullCodec,
                      AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func roundTripSensorRecords(codecName: String) async throws {
        let original = [
            SensorReading(id: 1, temperature: 22.5, location: "Auckland"),
            SensorReading(id: 2, temperature: 18.3, location: "Wellington"),
            SensorReading(id: 3, temperature: 14.1, location: "Christchurch"),
        ]
        let codec = try CodecFactory.make(named: codecName)
        let container = try AvroFileObjectContainer(schema: sensorSchema, codec: codec, syncMarker: makeSyncMarker())
        let encoded = try await container.write(objects: original)

        let decoded: [SensorReading] = try await container.read(from: encoded, as: SensorReading.self)
        #expect(decoded == original)
    }

    // MARK: Compression ratio

    @Test("Deflate output is smaller than null for repetitive data")
    func deflateOutputIsSmaller() async throws {
        let records = (0..<200).map {
            SensorReading(id: Int32($0), temperature: 20.0, location: "repeated-location")
        }
        let container = try AvroFileObjectContainer(schema: sensorSchema, syncMarker: makeSyncMarker())
        let nullData = try await container.write(objects: records)

        let deflateWriter = try AvroFileObjectContainer(
            schema: sensorSchema,
            codec: try CodecFactory.make(named: AvroReservedConstants.deflateCodec),
            syncMarker: makeSyncMarker()
        )
        let deflateData = try await deflateWriter.write(objects: records)
        #expect(deflateData.count < nullData.count)
    }
    #endif

    // MARK: Streaming

    @Test("Stream yields all objects in order")
    func streamYieldsAllObjects() async throws {
        let original = (0..<5).map { SimpleRecord(a: Int64($0), b: "stream-\($0)") }
        let writer = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let encoded = try await writer.write(objects: original)

        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        var streamed: [SimpleRecord] = []
        for try await record in await reader.stream(from: encoded, as: SimpleRecord.self) {
            streamed.append(record)
        }
        #expect(streamed == original)
    }

    // MARK: Error cases

    @Test("Read from corrupt data throws")
    func readCorruptDataThrows() async throws {
        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        await #expect(throws: (any Error).self) {
            try await reader.read(from: Data("this is not avro".utf8), as: SimpleRecord.self)
        }
    }

    @Test("Read from empty data throws")
    func readEmptyDataThrows() async throws {
        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        await #expect(throws: (any Error).self) {
            try await reader.read(from: Data(), as: SimpleRecord.self)
        }
    }

    @Test("read throws for corrupt data")
    func readCorruptTyped() async throws {
        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        await #expect(throws: (any Error).self) {
            _ = try await reader.read(from: Data("corrupt".utf8), as: SimpleRecord.self)
        }
    }

    @Test("addObject then read round-trips with typed throws")
    func addObjectRoundTrip() async throws {
        let container = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        try await container.addObject(SimpleRecord(a: 10, b: "typed"))
        try await container.addObject(SimpleRecord(a: 20, b: "throws"))
        let data = try await container.flush()

        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let results: [SimpleRecord] = try await reader.read(from: data, as: SimpleRecord.self)
        #expect(results == [SimpleRecord(a: 10, b: "typed"), SimpleRecord(a: 20, b: "throws")])
    }
}

// MARK: - Custom codec

/// A trivial codec whose compress and decompress are their own inverse —
/// used to verify the custom codec interface without any real compression library.
private struct ReverseCodec: CodecProtocol {
    let name = "reverse"
    func compress(data: Data) throws -> Data   { Data(data.reversed()) }
    func decompress(data: Data) throws -> Data { Data(data.reversed()) }
}

@Suite("Custom codec")
struct CustomCodecTests {

    @Test("Direct injection round-trips with custom codec")
    func directInjectionRoundTrip() async throws {
        let original = [SimpleRecord(a: 7, b: "custom")]
        let writer = try AvroFileObjectContainer(
            schema: simpleSchema, codec: ReverseCodec(), syncMarker: makeSyncMarker()
        )
        let encoded = try await writer.write(objects: original)
        #expect(!encoded.isEmpty)

        let reader = try AvroFileObjectContainer(
            schema: simpleSchema, codec: ReverseCodec(), syncMarker: makeSyncMarker()
        )
        let decoded: [SimpleRecord] = try await reader.read(from: encoded, as: SimpleRecord.self)
        #expect(decoded == original)
    }

    @Test("Name-based lookup via AvroCodecRegistry round-trips")
    func registryRoundTrip() async throws {
        AvroCodecRegistry.register(ReverseCodec())

        let original = [SimpleRecord(a: 99, b: "registry")]
        let codec = try CodecFactory.make(named: "reverse")
        let writer = try AvroFileObjectContainer(
            schema: simpleSchema, codec: codec, syncMarker: makeSyncMarker()
        )
        let encoded = try await writer.write(objects: original)

        let reader = try AvroFileObjectContainer(
            schema: simpleSchema, codec: codec, syncMarker: makeSyncMarker()
        )
        let decoded: [SimpleRecord] = try await reader.read(from: encoded, as: SimpleRecord.self)
        #expect(decoded == original)
    }

    @Test("Unknown codec name throws unsupportedCodec")
    func unknownCodecThrows() {
        #expect(throws: AvroCodecError.unsupportedCodec("nonexistent")) {
            try CodecFactory.make(named: "nonexistent")
        }
    }

    @Test("AvroCodecRegistry.codec(named:) returns nil for unregistered names")
    func registryReturnsNilForUnknown() {
        #expect(AvroCodecRegistry.codec(named: "no-such-codec") == nil)
    }

    @Test("AvroCodecRegistry.codec(named:) returns the registered codec")
    func registryReturnsRegisteredCodec() {
        AvroCodecRegistry.register(ReverseCodec())
        let codec = AvroCodecRegistry.codec(named: "reverse")
        #expect(codec != nil)
        #expect(codec?.name == "reverse")
    }
}

// MARK: - Schema evolution

/// Reader schema: SimpleRecord with an extra optional field `c` that has a default.
private let simpleSchemaV2 = """
{
    "type": "record",
    "name": "SimpleRecord",
    "fields": [
        { "name": "a", "type": "long"   },
        { "name": "b", "type": "string" },
        { "name": "c", "type": "string", "default": "default-value" }
    ]
}
"""

/// Reader struct corresponding to `simpleSchemaV2`.
private struct SimpleRecordV2: Codable, Sendable, Equatable {
    var a: Int64
    var b: String
    var c: String
}

/// Writer schema: SensorReading without the `location` field.
private let sensorSchemaV1 = """
{
    "type": "record",
    "name": "SensorReading",
    "fields": [
        { "name": "id",          "type": "int"    },
        { "name": "temperature", "type": "double" }
    ]
}
"""

/// Reader struct for the v1 sensor schema (no location field).
private struct SensorReadingV1: Codable, Sendable, Equatable {
    var id: Int32
    var temperature: Double
}

@Suite("Schema evolution")
struct SchemaEvolutionTests {

    @Test("Reader adds a field with default when writer schema lacks it")
    func readerAddsFieldWithDefault() async throws {
        let writer = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let encoded = try await writer.write(objects: [
            SimpleRecord(a: 1, b: "hello"),
            SimpleRecord(a: 2, b: "world"),
        ])

        let reader = try AvroFileObjectContainer(schema: simpleSchemaV2, syncMarker: makeSyncMarker())
        let results: [SimpleRecordV2] = try await reader.read(
            from: encoded, as: SimpleRecordV2.self, readerSchema: simpleSchemaV2
        )
        #expect(results == [
            SimpleRecordV2(a: 1, b: "hello", c: "default-value"),
            SimpleRecordV2(a: 2, b: "world", c: "default-value"),
        ])
    }

    @Test("Reader drops a field absent from the reader schema")
    func readerDropsExtraWriterField() async throws {
        let writer = try AvroFileObjectContainer(schema: sensorSchema, syncMarker: makeSyncMarker())
        let encoded = try await writer.write(objects: [
            SensorReading(id: 10, temperature: 21.0, location: "Auckland"),
            SensorReading(id: 11, temperature: 19.5, location: "Wellington"),
        ])

        let reader = try AvroFileObjectContainer(schema: sensorSchemaV1, syncMarker: makeSyncMarker())
        let results: [SensorReadingV1] = try await reader.read(
            from: encoded, as: SensorReadingV1.self, readerSchema: sensorSchemaV1
        )
        #expect(results == [
            SensorReadingV1(id: 10, temperature: 21.0),
            SensorReadingV1(id: 11, temperature: 19.5),
        ])
    }

    @Test("Evolved stream yields schema-resolved objects in order")
    func evolvedStreamYieldsObjects() async throws {
        let writer = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let original = (0..<4).map { SimpleRecord(a: Int64($0), b: "item-\($0)") }
        let encoded = try await writer.write(objects: original)

        let reader = try AvroFileObjectContainer(schema: simpleSchemaV2, syncMarker: makeSyncMarker())
        var streamed: [SimpleRecordV2] = []
        for try await record in await reader.stream(
            from: encoded, as: SimpleRecordV2.self, readerSchema: simpleSchemaV2
        ) {
            streamed.append(record)
        }
        let expected = original.map { SimpleRecordV2(a: $0.a, b: $0.b, c: "default-value") }
        #expect(streamed == expected)
    }

    @Test("Invalid reader schema JSON throws decodingFailed")
    func invalidReaderSchemaThrows() async throws {
        let writer = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        let encoded = try await writer.write(objects: [SimpleRecord(a: 1, b: "x")])

        let reader = try AvroFileObjectContainer(schema: simpleSchema, syncMarker: makeSyncMarker())
        await #expect(throws: AvroContainerError.decodingFailed("Invalid reader schema")) {
            _ = try await reader.read(from: encoded, as: SimpleRecord.self, readerSchema: "not-valid-json{")
        }
    }
}
