import Testing
import Foundation
@testable import SwiftAvroCore

struct LogicalTypesTests {
    let avro = Avro()

    @Test("Date logical type round-trips")
    func testDateRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
        let date = Date(timeIntervalSince1970: 86400 * 10) // 10 days after epoch
        let encoded = try avro.encodeFrom(date, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Date
        #expect(decoded == date)
    }

    @Test("Date logical type decodes through typed API")
    func testDateTypedDecode() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
        let date = Date(timeIntervalSince1970: 86400 * 10)
        let encoded = try avro.encodeFrom(date, schema: schema)
        let decoded: Date = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(decoded == date)
    }

    @Test("Date logical type decodes in arrays")
    func testDateArrayDecode() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}"#))
        let values = [
            Date(timeIntervalSince1970: 1622548800.123),
            Date(timeIntervalSince1970: 1622548801.456)
        ]
        let encoded = try avro.encodeFrom(values, schema: schema)
        let decoded: [Date] = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(decoded == values)
    }

    @Test("Timestamp-millis logical type round-trips")
    func testTimestampMillisRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-millis"}"#))
        let date = Date(timeIntervalSince1970: 1622548800.123) // 2021-06-01T12:00:00.123Z
        let encoded = try avro.encodeFrom(date, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Date
        #expect(abs(decoded.timeIntervalSince1970 - date.timeIntervalSince1970) < 0.001)
    }

    @Test("Timestamp-micros logical type round-trips")
    func testTimestampMicrosRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-micros"}"#))
        let date = Date(timeIntervalSince1970: 1622548800.123456)
        let encoded = try avro.encodeFrom(date, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Date
        #expect(abs(decoded.timeIntervalSince1970 - date.timeIntervalSince1970) < 0.000001)
    }

    @Test("Time-millis logical type round-trips")
    func testTimeMillisRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int","logicalType":"time-millis"}"#))
        let date = Date(timeIntervalSince1970: 3600 * 1000) // 1000 seconds into the day
        let encoded = try avro.encodeFrom(date, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Date
        #expect(decoded.timeIntervalSince1970.truncatingRemainder(dividingBy: 86400) ==
                date.timeIntervalSince1970.truncatingRemainder(dividingBy: 86400))
    }

    @Test("Decimal bytes logical type round-trips")
    func testDecimalBytesRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#))
        let value: Decimal = 123.45
        let encoded = try avro.encodeFrom(value, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Decimal
        #expect(decoded == value)
    }

    @Test("Decimal bytes logical type supports values larger than Int64")
    func testLargeDecimalBytesRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"bytes","logicalType":"decimal","precision":24,"scale":2}"#))
        let value = try #require(Decimal(string: "12345678901234567890.12"))
        let encoded = try avro.encodeFrom(value, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Decimal
        #expect(decoded == value)
    }

    @Test("Decimal fixed logical type round-trips")
    func testDecimalFixedRoundTrip() throws {
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"fixed","size":4,"logicalType":"decimal","precision":10,"scale":2}"#))
        let value: Decimal = 123.45
        let encoded = try avro.encodeFrom(value, schema: schema)
        let decoded = try avro.decodeFrom(from: encoded, schema: schema) as! Decimal
        #expect(decoded == value)
    }
}
