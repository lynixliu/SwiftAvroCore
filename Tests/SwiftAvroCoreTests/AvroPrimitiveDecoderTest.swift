//
//  AvroPrimitiveDecoderTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
@testable import SwiftAvroCore

@Suite("Avro Primitive Decoder")
struct AvroPrimitiveDecoderTests {

    // MARK: - Initialization and basic properties

    @Test("init with empty buffer")
    func initEmptyBuffer() {
        let data = Data()
        data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                return
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 0)
            #expect(decoder.available == 0)
            #expect(decoder.read == 0)
        }
    }

    @Test("init with data tracks available and read")
    func initWithData() {
        let data = Data([1, 2, 3, 4, 5])
        data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                return
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 5)
            #expect(decoder.available == 5)
            #expect(decoder.read == 0)
        }
    }

    // MARK: - advance

    @Test("advance reduces available and increases read")
    func advance() {
        let data = Data([1, 2, 3, 4, 5])
        data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                return
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 5)
            decoder.advance(2)
            #expect(decoder.available == 3)
            #expect(decoder.read == 2)
        }
    }

    // MARK: - decodeNull

    @Test("decodeNull does not consume bytes")
    func decodeNull() {
        let data = Data([1, 2, 3])
        data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                return
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            decoder.decodeNull()
            #expect(decoder.available == 3)
        }
    }

    // MARK: - decode Bool

    @Test("decode Bool false")
    func decodeBoolFalse() throws {
        let data = Data([0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Bool
            #expect(!result)
        }
    }

    @Test("decode Bool true")
    func decodeBoolTrue() throws {
        let data = Data([0x01])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Bool
            #expect(result)
        }
    }

    @Test("decode Bool throws on empty buffer")
    func decodeBoolEmptyBuffer() throws {
        let data = Data()
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 0)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Bool
            }
        }
    }

    // MARK: - decode Int32 (varint zigzag)

    @Test("decode Int32 zero")
    func decodeInt32Zero() throws {
        let data = Data([0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int32
            #expect(result == 0)
        }
    }

    @Test("decode Int32 positive value")
    func decodeInt32Positive() throws {
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int32
            #expect(result == 1)  // zigzag: 2 -> 1
        }
    }

    @Test("decode Int32 negative value")
    func decodeInt32Negative() throws {
        let data = Data([0x01])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int32
            #expect(result == -1)  // zigzag: 1 -> -1
        }
    }

    @Test("decode Int32 multi-byte varint")
    func decodeInt32MultiByte() throws {
        let data = Data([0x54])  // 42 zigzag encoded fits in Int32
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int32
            #expect(result == 42)
        }
    }

    // MARK: - decode Int64 (varint zigzag)

    @Test("decode Int64 zero")
    func decodeInt64Zero() throws {
        let data = Data([0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int64
            #expect(result == 0)
        }
    }

    @Test("decode Int64 large value")
    func decodeInt64Large() throws {
        // 3209099 zigzag encoded
        let data = Data([0x96, 0xDE, 0x87, 0x03])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode() as Int64
            #expect(result == 3_209_099)
        }
    }

    // MARK: - decode Int

    @Test("decode Int delegates to Int64")
    func decodeInt() throws {
        let data = Data([0x02])  // zigzag: 2 -> 1
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int
            #expect(result == 1)
        }
    }

    // MARK: - decode Int8

    @Test("decode Int8")
    func decodeInt8Value() throws {
        let data = Data([0x54])  // 42 zigzag
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int8
            #expect(result == 42)
        }
    }

    // MARK: - decode Int16

    @Test("decode Int16")
    func decodeInt16Value() throws {
        let data = Data([0xD0, 0x0F])  // 1000 zigzag
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 2)
            let result = try decoder.decode() as Int16
            #expect(result == 1000)
        }
    }

    // MARK: - decode UInt

    @Test("decode UInt")
    func decodeUInt() throws {
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as UInt
            #expect(result == 1)
        }
    }

    // MARK: - decode UInt8

    @Test("decode UInt8")
    func decodeUInt8Value() throws {
        let data = Data([0xFF])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as UInt8
            #expect(result == 255)
        }
    }

    @Test("decode UInt8 throws on empty buffer")
    func decodeUInt8EmptyBuffer() throws {
        let data = Data()
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 0)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as UInt8
            }
        }
    }

    // MARK: - decode UInt16

    @Test("decode UInt16")
    func decodeUInt16Value() throws {
        let data = Data([0xD0, 0x0F])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 2)
            let result = try decoder.decode() as UInt16
            #expect(result == 1000)
        }
    }

    // MARK: - decode UInt32 (fixed width, little-endian)

    @Test("decode UInt32")
    func decodeUInt32Value() throws {
        // 0x12345678 in little-endian
        let data = Data([0x78, 0x56, 0x34, 0x12])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode() as UInt32
            #expect(result == 0x12345678)
        }
    }

    @Test("decode UInt32 throws on insufficient bytes")
    func decodeUInt32InsufficientBytes() throws {
        let data = Data([0x78, 0x56, 0x34])  // Only 3 bytes
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as UInt32
            }
        }
    }

    // MARK: - decode UInt64

    @Test("decode UInt64")
    func decodeUInt64Value() throws {
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as UInt64
            #expect(result == 1)
        }
    }

    // MARK: - decode Float

    @Test("decode Float")
    func decodeFloatValue() throws {
        // 3.14 in IEEE 754 little-endian
        let data = Data([0xC3, 0xF5, 0x48, 0x40])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode() as Float
            #expect(abs(result - 3.14) < 0.001)
        }
    }

    @Test("decode Float throws on insufficient bytes")
    func decodeFloatInsufficientBytes() throws {
        let data = Data([0xC3, 0xF5, 0x48])  // Only 3 bytes
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Float
            }
        }
    }

    // MARK: - decode Double

    @Test("decode Double")
    func decodeDoubleValue() throws {
        // 3.14 in IEEE 754 little-endian
        let data = Data([0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 8)
            let result = try decoder.decode() as Double
            #expect(abs(result - 3.14) < 0.0001)
        }
    }

    @Test("decode Double throws on insufficient bytes")
    func decodeDoubleInsufficientBytes() throws {
        let data = Data([0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09])  // Only 7 bytes
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 7)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Double
            }
        }
    }

    // MARK: - decode String

    @Test("decode String ASCII")
    func decodeStringASCII() throws {
        // "foo" = length 3 (varint 0x06) + bytes
        let data = Data([0x06, 0x66, 0x6F, 0x6F])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode() as String
            #expect(result == "foo")
        }
    }

    @Test("decode String empty")
    func decodeStringEmpty() throws {
        let data = Data([0x00])  // length 0
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as String
            #expect(result == "")
        }
    }

    @Test("decode String throws on length beyond buffer")
    func decodeStringLengthBeyondBuffer() throws {
        let data = Data([0x0A, 0x66, 0x6F, 0x6F])  // Claims 5 bytes, only has 3
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as String
            }
        }
    }

    @Test("decode String throws on invalid UTF8")
    func decodeStringInvalidUTF8() throws {
        let data = Data([0x02, 0xFF, 0xFE])  // Invalid UTF-8 sequence
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode() as String
            }
        }
    }

    // MARK: - decode [UInt8] (bytes)

    @Test("decode bytes")
    func decodeBytes() throws {
        let data = Data([0x06, 0x66, 0x6F, 0x6F])  // length 3 + "foo"
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode() as [UInt8]
            #expect(result == [0x66, 0x6F, 0x6F])
        }
    }

    @Test("decode empty bytes")
    func decodeEmptyBytes() throws {
        let data = Data([0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as [UInt8]
            #expect(result.isEmpty)
        }
    }

    @Test("decode bytes throws on length beyond buffer")
    func decodeBytesLengthBeyondBuffer() throws {
        let data = Data([0x0A, 0x66, 0x6F, 0x6F])  // Claims 5 bytes, only has 3
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as [UInt8]
            }
        }
    }

    // MARK: - decode fixed

    @Test("decode fixed size bytes")
    func decodeFixedSizeBytes() throws {
        let data = Data([0x01, 0x02, 0x03, 0x04])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            let result = try decoder.decode(fixedSize: 4) as [UInt8]
            #expect(result == [0x01, 0x02, 0x03, 0x04])
        }
    }

    @Test("decode fixed size throws on insufficient bytes")
    func decodeFixedSizeInsufficientBytes() throws {
        let data = Data([0x01, 0x02, 0x03])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode(fixedSize: 4) as [UInt8]
            }
        }
    }

    // MARK: - decode fixed [UInt32] (duration)

    @Test("decode fixed UInt32 array for duration")
    func decodeFixedUInt32Duration() throws {
        // 12 bytes = 3 UInt32s in little-endian
        let data = Data([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xB2, 0x07, 0x00, 0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 12)
            let result = try decoder.decode(fixedSize: 12) as [UInt32]
            #expect(result == [1, 1, 1970])
        }
    }

    @Test("decode fixed UInt32 throws on insufficient bytes")
    func decodeFixedUInt32InsufficientBytes() throws {
        let data = Data([0x01, 0x00, 0x00])  // Only 3 bytes, need 4 for one UInt32
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode(fixedSize: 4) as [UInt32]
            }
        }
    }

    // MARK: - decodeVarint edge cases

    @Test("decode varint single byte")
    func decodeVarintSingleByteMax() throws {
        let data = Data([0x7E])  // zigzag: 63 -> 126
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Int64
            #expect(result == 63)
        }
    }

    @Test("decode varint max length 10 bytes")
    func decodeVarintMaxLength() throws {
        // Int64.max zigzag encoded (10 bytes)
        let data = Data([0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 10)
            let result = try decoder.decode() as Int64
            #expect(result == Int64.max)
        }
    }

    @Test("decode varint throws on malformed continuation")
    func decodeVarintMalformedContinuation() throws {
        // Varint that claims to continue but buffer ends
        let data = Data([0x80])  // Continuation bit set, but no more bytes
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode() as Int64
            }
        }
    }
}
