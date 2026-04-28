//
//  CodecTest.swift
//  SwiftAvroCoreTests
//
//  Tests for NullCodec and Codec type-erasing wrapper.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Codec Tests")
struct CodecTests {

    // MARK: - NullCodec Tests

    @Test("NullCodec name defaults to 'null'")
    func nullCodecName() {
        let codec = NullCodec()
        #expect(codec.name == AvroReservedConstants.nullCodec)
    }

    @Test("NullCodec name can be customized")
    func nullCodecCustomName() {
        let codec = NullCodec(name: "custom")
        #expect(codec.name == "custom")
    }

    @Test("NullCodec compress passes data through unchanged")
    func nullCodecCompress() throws {
        let codec = NullCodec()
        let data = Data([1, 2, 3, 4, 5])
        let compressed = try codec.compress(data: data)
        #expect(compressed == data)
    }

    @Test("NullCodec decompress passes data through unchanged")
    func nullCodecDecompress() throws {
        let codec = NullCodec()
        let data = Data([10, 20, 30, 40, 50])
        let decompressed = try codec.decompress(data: data)
        #expect(decompressed == data)
    }

    @Test("NullCodec round-trip compress then decompress")
    func nullCodecRoundTrip() throws {
        let codec = NullCodec()
        let original = Data("Hello, Avro!".utf8)
        let compressed = try codec.compress(data: original)
        let decompressed = try codec.decompress(data: compressed)
        #expect(decompressed == original)
    }

    @Test("NullCodec handles empty data")
    func nullCodecEmptyData() throws {
        let codec = NullCodec()
        let empty = Data()
        let compressed = try codec.compress(data: empty)
        let decompressed = try codec.decompress(data: compressed)
        #expect(compressed.isEmpty)
        #expect(decompressed.isEmpty)
    }

    @Test("NullCodec handles large data")
    func nullCodecLargeData() throws {
        let codec = NullCodec()
        let large = Data(repeating: 0xAB, count: 1_000_000)
        let compressed = try codec.compress(data: large)
        let decompressed = try codec.decompress(data: compressed)
        #expect(compressed.count == 1_000_000)
        #expect(decompressed == large)
    }

    // MARK: - Codec (Type-Erased Wrapper) Tests

    @Test("Codec default initializer uses NullCodec")
    func codecDefaultInit() {
        let codec = Codec()
        #expect(codec.name == AvroReservedConstants.nullCodec)
    }

    @Test("Codec wraps another codec")
    func codecWrap() {
        let nullCodec = NullCodec(name: "wrapped")
        let codec = Codec(nullCodec)
        #expect(codec.name == "wrapped")
    }

    @Test("Codec compress delegates to wrapped codec")
    func codecCompress() throws {
        let nullCodec = NullCodec()
        let codec = Codec(nullCodec)
        let data = Data([1, 2, 3])
        let compressed = try codec.compress(data: data)
        #expect(compressed == data)
    }

    @Test("Codec decompress delegates to wrapped codec")
    func codecDecompress() throws {
        let nullCodec = NullCodec()
        let codec = Codec(nullCodec)
        let data = Data([4, 5, 6])
        let decompressed = try codec.decompress(data: data)
        #expect(decompressed == data)
    }

    @Test("Codec round-trip with wrapped codec")
    func codecRoundTrip() throws {
        let nullCodec = NullCodec()
        let codec = Codec(nullCodec)
        let original = Data("Test data".utf8)
        let compressed = try codec.compress(data: original)
        let decompressed = try codec.decompress(data: compressed)
        #expect(decompressed == original)
    }
}
