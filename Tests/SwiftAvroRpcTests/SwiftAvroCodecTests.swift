import Testing
import Foundation
import SwiftAvroCore
@testable import SwiftAvroRpc

// MARK: - Test Models

struct SensorReading: Codable, Sendable, Equatable {
    var id: Int32
    var temperature: Double
    var location: String
}

struct SimpleRecord: Codable, Sendable, Equatable {
    var a: Int64
    var b: String
}

// MARK: - Schemas

let sensorSchema = """
{
    "type": "record",
    "name": "SensorReading",
    "fields": [
        { "name": "id",          "type": "int"    },
        { "name": "temperature", "type": "double" },
        { "name": "location",    "type": "string" }
    ]
}
"""

let simpleSchema = """
{
    "type": "record",
    "name": "SimpleRecord",
    "fields": [
        { "name": "a", "type": "long"   },
        { "name": "b", "type": "string" }
    ]
}
"""

// MARK: - NullCodec

@Suite("NullCodec")
struct NullCodecTests {

    let codec = NullCodec()

    @Test("Name matches Avro constant")
    func name() {
        #expect(codec.name == AvroReservedConstants.nullCodec)
    }
}

// MARK: - BuiltinCodec (Apple platforms only)

#if canImport(Compression)
@Suite("BuiltinCodec")
struct BuiltinCodecTests {

    // MARK: Names

    @Test("Deflate name matches constant")
    func deflateName() {
        #expect(BuiltinCodec(codecName: AvroReservedConstants.deflateCodec).name
                == AvroReservedConstants.deflateCodec)
    }

    @Test("LZ4 name matches constant")
    func lz4Name() {
        #expect(BuiltinCodec(codecName: AvroReservedConstants.lz4Codec).name
                == AvroReservedConstants.lz4Codec)
    }

    @Test("LZMA name matches constant")
    func lzmaName() {
        #expect(BuiltinCodec(codecName: AvroReservedConstants.xzCodec).name
                == AvroReservedConstants.xzCodec)
    }

    // MARK: Round-trips

    @Test("Deflate round-trip on repetitive data",
          arguments: [AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func roundTrip(codecName: String) throws {
        let codec = BuiltinCodec(codecName: codecName)
        let input = Data(repeating: 0x42, count: 4096)
        let decompressed = try codec.decompress(data: try codec.compress(data: input))
        #expect(decompressed == input)
    }

    @Test("Deflate round-trip on string data")
    func deflateRoundTripString() throws {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        let input = Data(String(repeating: "SwiftAvroRpc ", count: 200).utf8)
        let decompressed = try codec.decompress(data: try codec.compress(data: input))
        #expect(decompressed == input)
    }

    @Test("Deflate compresses repetitive data to smaller size")
    func deflateReducesSize() throws {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        let input = Data(repeating: 0x42, count: 4096)
        let compressed = try codec.compress(data: input)
        #expect(compressed.count < input.count)
    }

    // MARK: Empty data

    @Test("Compress empty data throws emptySourceData")
    func compressEmptyThrows() {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        #expect(throws: AvroCodecError.emptySourceData) {
            try codec.compress(data: Data())
        }
    }

    @Test("Decompress empty data throws emptySourceData")
    func decompressEmptyThrows() {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        #expect(throws: AvroCodecError.emptySourceData) {
            try codec.decompress(data: Data())
        }
    }

    // MARK: Custom buffer size

    @Test("Custom decompress buffer size still round-trips correctly")
    func customBufferSize() throws {
        let codec = BuiltinCodec(
            codecName: AvroReservedConstants.deflateCodec,
            decompressBufferSize: 1024 * 128
        )
        let input = Data(repeating: 0xCD, count: 4096)
        let decompressed = try codec.decompress(data: try codec.compress(data: input))
        #expect(decompressed == input)
    }

    // MARK: BuiltinCodec typed throws

    @Test("compress throws AvroCodecError.emptySourceData for empty input")
    func compressEmptyTyped() {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        #expect(throws: AvroCodecError.emptySourceData) {
            try codec.compress(data: Data())
        }
    }

    @Test("decompress throws AvroCodecError.emptySourceData for empty input")
    func decompressEmptyTyped() {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        #expect(throws: AvroCodecError.emptySourceData) {
            try codec.decompress(data: Data())
        }
    }

    @Test("decompress throws AvroCodecError.decompressionFailed for garbage input")
    func decompressGarbageTyped() {
        let codec = BuiltinCodec(codecName: AvroReservedConstants.deflateCodec)
        #expect(throws: AvroCodecError.decompressionFailed) {
            try codec.decompress(data: Data([0xFF, 0xFE, 0xFD, 0xFC]))
        }
    }

    @Test("typed error is exhaustively switchable without default",
          arguments: [AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func exhaustiveSwitch(codecName: String) {
        let codec = BuiltinCodec(codecName: codecName)
        do {
            _ = try codec.compress(data: Data())
        } catch {
            switch error {
            case .emptySourceData:
                break // expected
            case .compressionFailed, .decompressionFailed, .unsupportedCodec:
                Issue.record("Unexpected error: \(error)")
            }
        }
    }
}
#endif

// MARK: - CodecFactory

@Suite("CodecFactory")
struct CodecFactoryTests {

    @Test("Makes NullCodec for null constant")
    func makesNull() throws {
        let codec = try CodecFactory.make(named: AvroReservedConstants.nullCodec)
        #expect(codec.name == AvroReservedConstants.nullCodec)
        #expect(codec is NullCodec)
    }

    @Test("Unknown codec name throws unsupportedCodec")
    func unknownCodecThrows() {
        #expect(throws: AvroCodecError.unsupportedCodec("snappy")) {
            try CodecFactory.make(named: "snappy")
        }
    }

    @Test("make throws AvroCodecError.unsupportedCodec for unknown codec")
    func unsupportedCodecTyped() {
        #expect(throws: AvroCodecError.unsupportedCodec("brotli")) {
            try CodecFactory.make(named: "brotli")
        }
    }

    @Test("make does not throw for null codec")
    func nullCodecSucceeds() throws {
        let codec = try CodecFactory.make(named: AvroReservedConstants.nullCodec)
        #expect(codec.name == AvroReservedConstants.nullCodec)
    }

#if canImport(Compression)
    @Test("Makes BuiltinCodec for known codec names",
          arguments: [AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func makesBuiltin(codecName: String) throws {
        let codec = try CodecFactory.make(named: codecName)
        #expect(codec.name == codecName)
        #expect(codec is BuiltinCodec)
    }

    @Test("make does not throw for compression codecs",
          arguments: [AvroReservedConstants.deflateCodec,
                      AvroReservedConstants.lz4Codec,
                      AvroReservedConstants.xzCodec])
    func compressionCodecSucceeds(codecName: String) throws {
        let codec = try CodecFactory.make(named: codecName)
        #expect(codec.name == codecName)
    }
#endif
}

// MARK: - AvroIPCError

@Suite("AvroIPCError")
struct AvroIPCErrorTests {

    @Test("error cases are Sendable")
    func sendable() {
        let errors: [any Error & Sendable] = [
            AvroIPCError.handshakeFailed("test"),
            AvroIPCError.encodingFailed("test"),
            AvroIPCError.decodingFailed("test"),
            AvroIPCError.connectionClosed,
            AvroIPCError.timeout,
            AvroIPCError.noHandler("test"),
        ]
        #expect(errors.count == 6)
    }

    @Test("error descriptions contain associated values")
    func descriptions() {
        let error = AvroIPCError.handshakeFailed("bad hash")
        #expect("\(error)".contains("bad hash"))
    }
}
