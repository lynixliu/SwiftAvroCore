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

    // MARK: - Bool decode

    @Test("decode Bool false and true")
    func decodeBool() throws {
        let dataFalse = Data([0x00])
        try dataFalse.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            let result = try decoder.decode() as Bool
            #expect(!result)
        }
        let dataTrue = Data([0x01])
        try dataTrue.withUnsafeBytes { buffer in
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

    // MARK: - Integer types (varint zigzag)

    @Test("decode Int32 zero, positive, negative, and multi-byte")
    func decodeInt32() throws {
        let dataZero = Data([0x00])
        try dataZero.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int32 == 0)
        }
        let dataPos = Data([0x02])
        try dataPos.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int32 == 1)
        }
        let dataNeg = Data([0x01])
        try dataNeg.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int32 == -1)
        }
        let dataMulti = Data([0x54])
        try dataMulti.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int32 == 42)
        }
    }

    @Test("decode Int64 zero and large value")
    func decodeInt64() throws {
        let dataZero = Data([0x00])
        try dataZero.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int64 == 0)
        }
        let dataLarge = Data([0x96, 0xDE, 0x87, 0x03])
        try dataLarge.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(try decoder.decode() as Int64 == 3_209_099)
        }
    }

    @Test("decode Int, Int8, Int16")
    func decodeSmallInts() throws {
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int == 1)
        }
        let data8 = Data([0x54])
        try data8.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int8 == 42)
        }
        let data16 = Data([0xD0, 0x0F])
        try data16.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 2)
            #expect(try decoder.decode() as Int16 == 1000)
        }
    }

    @Test("decode UInt, UInt8, UInt16, UInt32, UInt64")
    func decodeUInts() throws {
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as UInt == 1)
        }
        let data8 = Data([0xFF])
        try data8.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as UInt8 == 255)
        }
        let data16 = Data([0xD0, 0x0F])
        try data16.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 2)
            #expect(try decoder.decode() as UInt16 == 1000)
        }
        let data32 = Data([0x78, 0x56, 0x34, 0x12])
        try data32.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(try decoder.decode() as UInt32 == 0x12345678)
        }
        let data64 = Data([0x02])
        try data64.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as UInt64 == 1)
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

    // MARK: - Floating point types

    @Test("decode Float value and insufficient bytes")
    func decodeFloat() throws {
        let data = Data([0xC3, 0xF5, 0x48, 0x40])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(abs(try decoder.decode() as Float - 3.14) < 0.001)
        }
        let dataShort = Data([0xC3, 0xF5, 0x48])
        try dataShort.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Float
            }
        }
    }

    @Test("decode Double value and insufficient bytes")
    func decodeDouble() throws {
        let data = Data([0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 8)
            #expect(abs(try decoder.decode() as Double - 3.14) < 0.0001)
        }
        let dataShort = Data([0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09])
        try dataShort.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 7)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Double
            }
        }
    }

    // MARK: - String decode

    @Test("decode String ASCII, empty, invalid UTF8, and length beyond buffer")
    func decodeString() throws {
        let data = Data([0x06, 0x66, 0x6F, 0x6F])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(try decoder.decode() as String == "foo")
        }
        let dataEmpty = Data([0x00])
        try dataEmpty.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as String == "")
        }
        let dataInvalid = Data([0x02, 0xFF, 0xFE])
        try dataInvalid.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode() as String
            }
        }
        let dataBeyond = Data([0x0A, 0x66, 0x6F, 0x6F])
        try dataBeyond.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as String
            }
        }
    }

    // MARK: - Bytes decode

    @Test("decode bytes, empty bytes, and length beyond buffer")
    func decodeBytes() throws {
        let data = Data([0x06, 0x66, 0x6F, 0x6F])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(try decoder.decode() as [UInt8] == [0x66, 0x6F, 0x6F])
        }
        let dataEmpty = Data([0x00])
        try dataEmpty.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect((try decoder.decode() as [UInt8]).isEmpty)
        }
        let dataBeyond = Data([0x0A, 0x66, 0x6F, 0x6F])
        try dataBeyond.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as [UInt8]
            }
        }
    }

    // MARK: - Fixed size decode

    @Test("decode fixed size bytes and insufficient bytes")
    func decodeFixedSize() throws {
        let data = Data([0x01, 0x02, 0x03, 0x04])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 4)
            #expect(try decoder.decode(fixedSize: 4) as [UInt8] == [0x01, 0x02, 0x03, 0x04])
        }
        let dataShort = Data([0x01, 0x02, 0x03])
        try dataShort.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 3)
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode(fixedSize: 4) as [UInt8]
            }
        }
    }

    @Test("decode fixed UInt32 array for duration and insufficient bytes")
    func decodeFixedUInt32() throws {
        let data = Data([0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xB2, 0x07, 0x00, 0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 12)
            #expect(try decoder.decode(fixedSize: 12) as [UInt32] == [1, 1, 1970])
        }
        let dataShort = Data([0x01, 0x00, 0x00])
        try dataShort.withUnsafeBytes { buffer in
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

    @Test("decode varint single byte max, max length 10 bytes, and malformed continuation")
    func decodeVarint() throws {
        let data1 = Data([0x7E])
        try data1.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(try decoder.decode() as Int64 == 63)
        }
        let data1b = Data([0x80, 0x01])  // 2-byte varint: value 64
        try data1b.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 2)
            #expect(try decoder.decode() as Int64 == 64)
        }
        let data2 = Data([0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01])
        try data2.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 10)
            #expect(try decoder.decode() as Int64 == Int64.max)
        }
        let data3 = Data([0x80])
        try data3.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode() as Int64
            }
        }
    }

    // MARK: - Negative-length error paths

    @Test("decodeVarint throws outOfBufferBoundary when buffer is exhausted")
    func decodeVarintExhaustedBuffer() throws {
        // After consuming the only byte, a second varint read hits the
        // `guard available >= 1` check with available == 0 (line 180).
        let data = Data([0x02])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            _ = try decoder.decode() as Int64  // consumes the single byte
            #expect(throws: BinaryDecodingError.outOfBufferBoundary) {
                try decoder.decode() as Int64  // available == 0, throws line 180
            }
        }
    }

    @Test("decode [UInt8] throws malformed when zig-zag length is negative")
    func decodeBytesNegativeLength() throws {
        // Varint 0x01 → UInt64(1) → zig-zag → Int64(-1) → negative-length guard
        let data = Data([0x01])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode() as [UInt8]
            }
        }
    }

    @Test("decode(fixedSize:) throws malformed when fixedSize is negative")
    func decodeFixedNegativeSize() throws {
        let data = Data([0x00])
        try data.withUnsafeBytes { buffer in
            guard let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                throw BinaryDecodingError.outOfBufferBoundary
            }
            let decoder = AvroPrimitiveDecoder(pointer: pointer, size: 1)
            #expect(throws: BinaryDecodingError.malformedAvro) {
                try decoder.decode(fixedSize: -1) as [UInt8]
            }
        }
    }
}
