import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Primitive Sizer")
struct AvroPrimitiveSizerTests {

    // MARK: - Null

    @Test("encodeNull adds zero size")
    func encodeNull() {
        let sizer = AvroPrimitiveSizer()
        sizer.encodeNull()
        #expect(sizer.size == 0)
    }

    // MARK: - Bool

    @Test("encode Bool adds 1 byte")
    func encodeBool() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(true)
        #expect(sizer.size == 1)
        sizer.encode(false)
        #expect(sizer.size == 2)
    }

    // MARK: - UInt8

    @Test("encode UInt8 adds 1 byte")
    func encodeUInt8() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(UInt8(255))
        #expect(sizer.size == 1)
    }

    // MARK: - Float/Double

    @Test("encode Float adds 4 bytes")
    func encodeFloat() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Float(3.14))
        #expect(sizer.size == 4)
    }

    @Test("encode Double adds 8 bytes")
    func encodeDouble() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Double(2.71828))
        #expect(sizer.size == 8)
    }

    // MARK: - Int types

    @Test("encode Int adds varint size")
    func encodeInt() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int(0))
        #expect(sizer.size == 1)
    }

    @Test("encode Int8 adds varint size")
    func encodeInt8() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int8(42))
        #expect(sizer.size == 1)
    }

    @Test("encode Int16 adds varint size")
    func encodeInt16() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int16(1000))
        #expect(sizer.size == 2)
    }

    @Test("encode Int32 adds varint size")
    func encodeInt32() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int32(42))
        #expect(sizer.size == 1)
    }

    @Test("encode Int64 adds varint size")
    func encodeInt64() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int64(42))
        #expect(sizer.size == 1)
    }

    // MARK: - UInt types

    @Test("encode UInt16 adds varint size")
    func encodeUInt16() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(UInt16(1000))
        #expect(sizer.size == 2)
    }

    @Test("encode UInt32 adds 4 bytes")
    func encodeUInt32() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(UInt32(100))
        #expect(sizer.size == 4)
    }

    // MARK: - String

    @Test("encode String adds length prefix plus bytes")
    func encodeString() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode("hello")
        #expect(sizer.size == 6)
    }

    @Test("encode empty string")
    func encodeEmptyString() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode("")
        #expect(sizer.size == 1)
    }

    // MARK: - Bytes

    @Test("encode bytes adds length prefix plus bytes")
    func encodeBytes() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode([72, 101, 108, 108, 111] as [UInt8])
        #expect(sizer.size == 6)
    }

    @Test("encode empty bytes")
    func encodeEmptyBytes() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode([] as [UInt8])
        #expect(sizer.size == 1)
    }

    // MARK: - Fixed

    @Test("encode fixed adds byte count")
    func encodeFixed() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(fixed: [1, 2, 3, 4] as [UInt8])
        #expect(sizer.size == 4)
    }

    @Test("encode fixed UInt32 array adds size")
    func encodeFixedUInt32() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(fixed: [1, 2, 3] as [UInt32])
        #expect(sizer.size == 12)
    }

    // MARK: - Append

    @Test("append another sizer increases size")
    func append() throws {
        let sizer1 = AvroPrimitiveSizer()
        sizer1.encode(Int32(42))
        let sizer2 = AvroPrimitiveSizer()
        sizer2.encode(Int64(100))
        sizer1.append(sizer2)
    }

    // MARK: - Buffer property

    @Test("buffer returns empty array")
    func buffer() {
        let sizer = AvroPrimitiveSizer()
        #expect(sizer.buffer.isEmpty)
    }

    // MARK: - Edge cases

    @Test("Int64 max value needs 10 bytes")
    func int64Max() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int64(Int64.max))
        #expect(sizer.size == 10)
    }

    @Test("negative Int64 needs 1 byte for -1")
    func int64Negative() {
        let sizer = AvroPrimitiveSizer()
        sizer.encode(Int64(-1))
        #expect(sizer.size == 1)
    }

    @Test("large string encoding")
    func largeString() {
        let sizer = AvroPrimitiveSizer()
        let longString = String(repeating: "a", count: 1000)
        sizer.encode(longString)
        #expect(sizer.size == 1002)  // 1000 + 2 (varint length prefix)
    }
}