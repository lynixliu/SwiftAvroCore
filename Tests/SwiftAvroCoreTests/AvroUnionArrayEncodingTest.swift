//
//  AvroUnionArrayEncodingTest.swift
//  SwiftAvroCoreTests
//
//  Regression tests for encoding non-nil values whose Avro item schema is a
//  union — e.g. `array<union{null,long}>` (the shape of `JsmaEvent.intValues`).
//
//  The unkeyed / single-value encoders resolved union branches only for
//  `String`; the numeric/bool scalar encoders did not. So a non-nil numeric
//  element in a union-typed array threw `BinaryEncodingError` (e.g.
//  `.typeMismatchWithSchemaInt64`) and the value was silently dropped. These
//  tests fail against the broken encoder and pass once the numeric encoders
//  learn union resolution.
//
//  Originally IOS-22148 (XCTest); converted to Swift Testing.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Union Array Encoding")
struct AvroUnionArrayEncodingTests {

    // MARK: - Demonstrates the bug (fails until the numeric encoders resolve unions)

    /// `array<union{null,long}>` with a non-nil element — the exact production shape.
    /// Previously threw `typeMismatchWithSchemaInt64`.
    @Test("array<union{null,long}> encodes a non-nil element")
    func encodeArrayOfNullableLong_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","long"]}"#))
        let data = try AvroEncoder().encode([Int64(1)], schema: schema)
        // block count = 1 (0x02), union branch index = 1/long (0x02),
        // value = 1 (0x02), end-of-array block (0x00)
        #expect(data == Data([0x02, 0x02, 0x02, 0x00]))
    }

    /// `array<union{null,int}>` with a non-nil element.
    @Test("array<union{null,int}> encodes a non-nil element")
    func encodeArrayOfNullableInt_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","int"]}"#))
        _ = try AvroEncoder().encode([Int32(1)], schema: schema)
    }

    /// `array<union{null,boolean}>` with a non-nil element.
    @Test("array<union{null,boolean}> encodes a non-nil element")
    func encodeArrayOfNullableBoolean_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","boolean"]}"#))
        _ = try AvroEncoder().encode([true], schema: schema)
    }

    // MARK: - Controls that already passed (prove the defect was numeric-specific)

    /// String union arrays already encode — `encode(String)` resolved the union.
    @Test("array<union{null,string}> encodes a non-nil element")
    func encodeArrayOfNullableString_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","string"]}"#))
        _ = try AvroEncoder().encode(["x"], schema: schema)
    }

    /// Nil elements already encode — `encodeNil()` resolved the union.
    @Test("array<union{null,long}> encodes a nil element")
    func encodeArrayOfNullableLong_nilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","long"]}"#))
        _ = try AvroEncoder().encode([Int64?.none], schema: schema)
    }

    // MARK: - Float / Double union coverage

    @Test("array<union{null,float}> encodes a non-nil element")
    func encodeArrayOfNullableFloat_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","float"]}"#))
        _ = try AvroEncoder().encode([Float(1.5)], schema: schema)
    }

    @Test("array<union{null,double}> encodes a non-nil element")
    func encodeArrayOfNullableDouble_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","double"]}"#))
        _ = try AvroEncoder().encode([Double(2.5)], schema: schema)
    }

    // MARK: - Double via a logical-type (date) branch inside a union
    //
    // A Swift `Date` encodes as a Double, which the direct path maps onto an
    // `int` schema carrying `logicalType: "date"`. The union path must select
    // that branch too — not just a plain `double` branch.

    /// `array<union{null, int/date}>` with a non-nil `Date`.
    @Test("array<union{null,int/date}> encodes a non-nil Date")
    func encodeArrayOfNullableDate_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null",{"type":"int","logicalType":"date"}]}"#))
        _ = try AvroEncoder().encode([Date(timeIntervalSince1970: 0)], schema: schema)
    }

    // A Double must also resolve to the remaining Double-encodable logical-type
    // branches inside a union — int/time-millis and long/time-micros,
    // timestamp-millis, timestamp-micros — not just double and int/date.

    @Test("array<union{null,int/time-millis}> encodes a non-nil element")
    func encodeArrayOfNullableTimeMillis_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null",{"type":"int","logicalType":"time-millis"}]}"#))
        _ = try AvroEncoder().encode([Double(0)], schema: schema)
    }

    @Test("array<union{null,long/time-micros}> encodes a non-nil element")
    func encodeArrayOfNullableTimeMicros_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null",{"type":"long","logicalType":"time-micros"}]}"#))
        _ = try AvroEncoder().encode([Double(0)], schema: schema)
    }

    @Test("array<union{null,long/timestamp-millis}> encodes a non-nil element")
    func encodeArrayOfNullableTimestampMillis_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null",{"type":"long","logicalType":"timestamp-millis"}]}"#))
        _ = try AvroEncoder().encode([Double(0)], schema: schema)
    }

    @Test("array<union{null,long/timestamp-micros}> encodes a non-nil element")
    func encodeArrayOfNullableTimestampMicros_nonNilElement() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null",{"type":"long","logicalType":"timestamp-micros"}]}"#))
        _ = try AvroEncoder().encode([Double(0)], schema: schema)
    }

    // MARK: - String union: enum branch + no-matching-branch
    //
    // The String union path only matched a `string` branch and had no `else`,
    // so an enum-only union silently encoded NOTHING (worse than throwing).

    /// `union{null, enum}` with a valid symbol. Previously a silent no-op
    /// (encoded empty), so the byte assertion — not a no-throw check — exposes it.
    @Test("union{null,enum} encodes a non-nil symbol")
    func encodeNullableEnum_nonNil() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"["null",{"type":"enum","name":"Suit","symbols":["A","B","C"]}]"#))
        let data = try AvroEncoder().encode("B", schema: schema)
        // union branch index = 1/enum (0x02), enum symbol index for "B" = 1 (0x02)
        #expect(data == Data([0x02, 0x02]))
    }

    /// A String against a union with no string/enum branch must throw, not
    /// silently encode nothing.
    @Test("String against a non-string/enum union throws")
    func encodeString_againstNonStringUnion_throws() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"["null","long"]"#))
        #expect(throws: (any Error).self) {
            _ = try AvroEncoder().encode("x", schema: schema)
        }
    }
}
