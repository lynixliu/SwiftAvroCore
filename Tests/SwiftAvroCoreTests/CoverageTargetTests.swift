import Testing
import Foundation
@testable import SwiftAvroCore

// MARK: - Helpers

private func parse(_ json: String) throws -> AvroSchema {
    let avro = Avro()
    return try #require(avro.decodeSchema(schema: json))
}

// MARK: - JSONValue Tests

@Suite("JSONValue – Codable")
struct JSONValueCodableTests {

    @Test func nullRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.null)
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == .null)
    }

    @Test func boolTrueRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.bool(true))
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == .bool(true))
    }

    @Test func boolFalseRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.bool(false))
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == .bool(false))
    }

    @Test func intRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.int(9999))
        let decoded = try JSONDecoder().decode(JSONValue.self, from: data)
        switch decoded {
        case .int(let v): #expect(v == 9999)
        case .double(let v): #expect(Int64(v) == 9999)
        default: Issue.record("unexpected case")
        }
    }

    @Test func doubleRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.double(1.5))
        let decoded = try JSONDecoder().decode(JSONValue.self, from: data)
        if case .double(let v) = decoded { #expect(v == 1.5) }
        else { Issue.record("expected .double") }
    }

    @Test func stringRoundTrip() throws {
        let data = try JSONEncoder().encode(JSONValue.string("avro"))
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == .string("avro"))
    }

    @Test func arrayRoundTrip() throws {
        let val = JSONValue.array([.null, .bool(true), .int(1), .double(2.5), .string("s")])
        let data = try JSONEncoder().encode(val)
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == val)
    }

    @Test func objectRoundTrip() throws {
        let val = JSONValue.object(["a": .int(1), "b": .string("x")])
        let data = try JSONEncoder().encode(val)
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == val)
    }

    @Test func nestedRoundTrip() throws {
        let val = JSONValue.object(["arr": .array([.int(1)]), "obj": .object(["x": .null])])
        let data = try JSONEncoder().encode(val)
        #expect(try JSONDecoder().decode(JSONValue.self, from: data) == val)
    }

    @Test func decodeRawNull() throws {
        #expect(try JSONDecoder().decode(JSONValue.self, from: Data("null".utf8)) == .null)
    }

    @Test func decodeRawBool() throws {
        #expect(try JSONDecoder().decode(JSONValue.self, from: Data("true".utf8)) == .bool(true))
    }

    @Test func decodeRawString() throws {
        #expect(try JSONDecoder().decode(JSONValue.self, from: Data(#""hello""#.utf8)) == .string("hello"))
    }

    @Test func decodeRawDouble() throws {
        let decoded = try JSONDecoder().decode(JSONValue.self, from: Data("3.14".utf8))
        if case .double(let v) = decoded { #expect(abs(v - 3.14) < 0.001) }
        else { Issue.record("expected .double") }
    }
}

// MARK: - LogicalTypeConverter Tests

@Suite("LogicalTypeConverter")
struct LogicalTypeConverterTests {

    @Test func decodeDateZero() {
        #expect(LogicalTypeConverter.decodeDate(0).timeIntervalSince1970 == 0)
    }

    @Test func decodeDateOneDay() {
        #expect(LogicalTypeConverter.decodeDate(1).timeIntervalSince1970 == 86400)
    }

    @Test func encodeDateFloors() {
        let date = Date(timeIntervalSince1970: 86400 * 1.7)
        #expect(LogicalTypeConverter.encodeDate(date) == 1)
    }

    @Test func encodeDateRoundTrip() {
        let date = Date(timeIntervalSince1970: 86400 * 100)
        #expect(LogicalTypeConverter.decodeDate(LogicalTypeConverter.encodeDate(date)) == date)
    }

    @Test func timestampMillisRoundTrip() {
        let millis: Int64 = 1_622_548_800_123
        let date = LogicalTypeConverter.decodeTimestampMillis(millis)
        #expect(LogicalTypeConverter.encodeTimestampMillis(date) == millis)
    }

    @Test func timestampMicrosRoundTrip() {
        let micros: Int64 = 1_622_548_800_123_456
        let date = LogicalTypeConverter.decodeTimestampMicros(micros)
        #expect(abs(LogicalTypeConverter.encodeTimestampMicros(date) - micros) <= 1)
    }

    @Test func decodeTimestampMicrosZero() {
        #expect(LogicalTypeConverter.decodeTimestampMicros(0).timeIntervalSince1970 == 0)
    }

    @Test func timeMillisNoon() {
        let date = Date(timeIntervalSince1970: 12 * 3600)
        #expect(LogicalTypeConverter.encodeTimeMillis(date) == 12 * 3600 * 1000)
    }

    @Test func timeMillisNegativeAdjusted() {
        // -1 second from epoch → time-of-day = 86399s
        let date = Date(timeIntervalSince1970: -1)
        #expect(LogicalTypeConverter.encodeTimeMillis(date) == 86_399_000)
    }

    @Test func timeMicrosRoundTrip() {
        let micros: Int64 = 43_200_000_000
        let date = LogicalTypeConverter.decodeTimeMicros(micros)
        #expect(abs(LogicalTypeConverter.encodeTimeMicros(date) - micros) <= 1)
    }

    @Test func timeMicrosNegativeAdjusted() {
        let date = Date(timeIntervalSince1970: -1)
        #expect(LogicalTypeConverter.encodeTimeMicros(date) == 86_399_000_000)
    }

    @Test func decodeDurationShortBytes() {
        #expect(LogicalTypeConverter.decodeDuration(bytes: [1, 2]) == [0, 0, 0])
    }

    @Test func decodeDurationExactly12Bytes() {
        var b = [UInt8](repeating: 0, count: 12)
        b[0] = 1; b[4] = 2; b[8] = 3
        #expect(LogicalTypeConverter.decodeDuration(bytes: b) == [1, 2, 3])
    }

    @Test func decodeDurationMultiByte() {
        var b = [UInt8](repeating: 0, count: 12)
        b[0] = 0x00; b[1] = 0x01  // months = 256
        let result = LogicalTypeConverter.decodeDuration(bytes: b)
        #expect(result[0] == 256)
    }

    @Test func decodeDecimalEmptyBytes() {
        #expect(LogicalTypeConverter.decodeDecimal(bytes: [], scale: 2, precision: 10) == 0)
    }

    @Test func decimalPositiveRoundTrip() throws {
        let d = Decimal(string: "123.45")!
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 2, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 2, precision: 10) == d)
    }

    @Test func decimalNegativeRoundTrip() throws {
        let d = Decimal(string: "-123.45")!
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 2, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 2, precision: 10) == d)
    }

    @Test func decimalZeroRoundTrip() throws {
        let bytes = try LogicalTypeConverter.encodeDecimal(.zero, scale: 2, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 2, precision: 10) == 0)
    }

    @Test func decimalScaleZeroRoundTrip() throws {
        let d = Decimal(42)
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 0, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 0, precision: 10) == d)
    }

    @Test func decimalScaleGreaterThanDigits() throws {
        let d = Decimal(string: "0.00123")!
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 5, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 5, precision: 10) == d)
    }

    @Test func decimalLargeValue() throws {
        let d = Decimal(string: "12345678901234567890.12")!
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 2, precision: 24)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 2, precision: 24) == d)
    }

    @Test func decimalNegativeSmall() throws {
        let d = Decimal(string: "-0.01")!
        let bytes = try LogicalTypeConverter.encodeDecimal(d, scale: 2, precision: 10)
        #expect(LogicalTypeConverter.decodeDecimal(bytes: bytes, scale: 2, precision: 10) == d)
    }

    @Test func decimalPrecisionExceededThrows() {
        #expect(throws: BinaryEncodingError.invalidDecimal) {
            try LogicalTypeConverter.encodeDecimal(Decimal(string: "123456789012")!, scale: 0, precision: 5)
        }
    }

    @Test func decimalFixedSizeTooSmallThrows() {
        #expect(throws: BinaryEncodingError.invalidDecimal) {
            try LogicalTypeConverter.encodeDecimal(Decimal(string: "1234567890")!, scale: 0, precision: 15, fixedSize: 1)
        }
    }

    @Test func decimalFixedSizePadsPositive() throws {
        let bytes = try LogicalTypeConverter.encodeDecimal(Decimal(string: "1.23")!, scale: 2, precision: 10, fixedSize: 4)
        #expect(bytes.count == 4)
        #expect(bytes[0] == 0x00)
    }

    @Test func decimalFixedSizePadsNegative() throws {
        let bytes = try LogicalTypeConverter.encodeDecimal(Decimal(string: "-1.23")!, scale: 2, precision: 10, fixedSize: 4)
        #expect(bytes.count == 4)
        #expect(bytes[0] == 0xFF)
    }

    @Test func decimalNegativeFromBytes() throws {
        // [0xFF] = -1 in two's complement, scale=2 → -0.01
        let result = LogicalTypeConverter.decodeDecimal(bytes: [0xFF], scale: 2, precision: 10)
        #expect(result == Decimal(string: "-0.01")!)
    }
}

// MARK: - AvroSchema+Evolution: resolveValue

@Suite("AvroSchema+Evolution – resolveValue")
struct EvolutionResolveValueTests {

    // MARK: exactlyMatches → early return

    @Test func resolveNullExact() throws {
        let s = try parse(#""null""#)
        #expect(try s.resolveValue(nil, writtenBy: s) == nil)
    }

    @Test func resolveBoolExact() throws {
        let s = try parse(#""boolean""#)
        #expect(try s.resolveValue(true, writtenBy: s) as? Bool == true)
    }

    @Test func resolveFloatExact() throws {
        let s = try parse(#""float""#)
        #expect(try s.resolveValue(Float(1.5), writtenBy: s) as? Float == 1.5)
    }

    @Test func resolveDoubleExact() throws {
        let s = try parse(#""double""#)
        #expect(try s.resolveValue(Double(2.5), writtenBy: s) as? Double == 2.5)
    }

    @Test func resolveIntExact() throws {
        let s = try parse(#"{"type":"int"}"#)
        #expect(try s.resolveValue(Int32(7), writtenBy: s) as? Int32 == 7)
    }

    @Test func resolveLongExact() throws {
        let s = try parse(#"{"type":"long"}"#)
        #expect(try s.resolveValue(Int64(8), writtenBy: s) as? Int64 == 8)
    }

    @Test func resolveBytesExact() throws {
        let s = try parse(#"{"type":"bytes"}"#)
        let b: [UInt8] = [1, 2]
        #expect(try s.resolveValue(b, writtenBy: s) as? [UInt8] == b)
    }

    @Test func resolveStringExact() throws {
        let s = try parse(#"{"type":"string"}"#)
        #expect(try s.resolveValue("hi", writtenBy: s) as? String == "hi")
    }

    // MARK: int64Value all branches

    @Test func longFromInt64() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int64(1), writtenBy: w) as? Int64 == 1)
    }

    @Test func longFromInt32() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int32(2), writtenBy: w) as? Int64 == 2)
    }

    @Test func longFromInt() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int(3), writtenBy: w) as? Int64 == 3)
    }

    @Test func longFromDouble() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Double(4), writtenBy: w) as? Int64 == 4)
    }

    @Test func longFromFloat() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Float(5), writtenBy: w) as? Int64 == 5)
    }

    @Test func longFromInvalidThrows() throws {
        let r = try parse(#"{"type":"long"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("bad", writtenBy: w)
        }
    }

    // MARK: doubleValue all branches

    @Test func doubleFromDouble() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"float"}"#)
        #expect(try r.resolveValue(Double(1.0), writtenBy: w) as? Double == 1.0)
    }

    @Test func doubleFromFloat() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"float"}"#)
        #expect(try r.resolveValue(Float(2.0), writtenBy: w) as? Double == 2.0)
    }

    @Test func doubleFromInt64() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"long"}"#)
        #expect(try r.resolveValue(Int64(3), writtenBy: w) as? Double == 3.0)
    }

    @Test func doubleFromInt32() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int32(4), writtenBy: w) as? Double == 4.0)
    }

    @Test func doubleFromInt() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int(5), writtenBy: w) as? Double == 5.0)
    }

    @Test func doubleFromInvalidThrows() throws {
        let r = try parse(#"{"type":"double"}"#); let w = try parse(#"{"type":"float"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("bad", writtenBy: w)
        }
    }

    // MARK: float promotions

    @Test func floatFromInt() throws {
        let r = try parse(#"{"type":"float"}"#); let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int32(5), writtenBy: w) as? Float == 5.0)
    }

    @Test func floatFromLong() throws {
        let r = try parse(#"{"type":"float"}"#); let w = try parse(#"{"type":"long"}"#)
        #expect(try r.resolveValue(Int64(6), writtenBy: w) as? Float == 6.0)
    }

    // MARK: bytes ↔ string

    @Test func bytesFromString() throws {
        let r = try parse(#"{"type":"bytes"}"#); let w = try parse(#"{"type":"string"}"#)
        #expect(try r.resolveValue("hi", writtenBy: w) as? [UInt8] == Array("hi".utf8))
    }

    @Test func bytesFromStringInvalidTypeThrows() throws {
        let r = try parse(#"{"type":"bytes"}"#); let w = try parse(#"{"type":"string"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue(42, writtenBy: w)
        }
    }

    @Test func stringFromBytes() throws {
        let r = try parse(#"{"type":"string"}"#); let w = try parse(#"{"type":"bytes"}"#)
        #expect(try r.resolveValue(Array("hi".utf8), writtenBy: w) as? String == "hi")
    }

    @Test func stringFromBytesInvalidUTF8Throws() throws {
        let r = try parse(#"{"type":"string"}"#); let w = try parse(#"{"type":"bytes"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue([UInt8]([0xFF, 0xFE]), writtenBy: w)
        }
    }

    @Test func stringFromBytesNonArrayThrows() throws {
        let r = try parse(#"{"type":"string"}"#); let w = try parse(#"{"type":"bytes"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("notBytes", writtenBy: w)
        }
    }

    // MARK: array / map

    @Test func arrayResolvesElements() throws {
        let r = try parse(#"{"type":"array","items":"long"}"#)
        let w = try parse(#"{"type":"array","items":"int"}"#)
        let result = try r.resolveValue([Int32(1), Int32(2)], writtenBy: w) as? [Any]
        #expect(result?.first as? Int64 == 1)
    }

    @Test func arrayNonArrayThrows() throws {
        let r = try parse(#"{"type":"array","items":"long"}"#)
        let w = try parse(#"{"type":"array","items":"int"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("bad", writtenBy: w)
        }
    }

    @Test func mapResolvesValues() throws {
        let r = try parse(#"{"type":"map","values":"long"}"#)
        let w = try parse(#"{"type":"map","values":"int"}"#)
        let result = try r.resolveValue(["k": Int32(9)], writtenBy: w) as? [String: Any]
        #expect(result?["k"] as? Int64 == 9)
    }

    @Test func mapNonDictThrows() throws {
        let r = try parse(#"{"type":"map","values":"long"}"#)
        let w = try parse(#"{"type":"map","values":"int"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("bad", writtenBy: w)
        }
    }

    // MARK: enum

    @Test func enumSymbolInBoth() throws {
        let r = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        let w = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        #expect(try r.resolveValue("A", writtenBy: w) as? String == "A")
    }

    @Test func enumSymbolUsesReaderDefault() throws {
        let r = try parse(#"{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}"#)
        let w = try parse(#"{"type":"enum","name":"E","symbols":["A","B","C"]}"#)
        #expect(try r.resolveValue("C", writtenBy: w) as? String == "A")
    }

    @Test func enumSymbolNotInWriterThrows() throws {
        let r = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        let w = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("X", writtenBy: w)
        }
    }

    @Test func enumSymbolNotInReaderNoDefaultThrows() throws {
        let r = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        let w = try parse(#"{"type":"enum","name":"E","symbols":["A","B","C"]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue("C", writtenBy: w)
        }
    }

    @Test func enumNonStringThrows() throws {
        let r = try parse(#"{"type":"enum","name":"E","symbols":["A"]}"#)
        let w = try parse(#"{"type":"enum","name":"E","symbols":["A"]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue(0, writtenBy: w)
        }
    }

    // MARK: fixed

    @Test func fixedMatchingNames() throws {
        let r = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        let w = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        let b: [UInt8] = [1, 2, 3, 4]
        // When schemas are equal, exactlyMatches returns true → value returned
        #expect(try r.resolveValue(b, writtenBy: w) as? [UInt8] == b)
    }

    @Test func fixedSizeMismatchThrows() throws {
        let r = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        let w = try parse(#"{"type":"fixed","name":"F","size":8}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue([UInt8](repeating: 0, count: 8), writtenBy: w)
        }
    }

    @Test func fixedNameMismatchThrows() throws {
        let r = try parse(#"{"type":"fixed","name":"F1","size":4}"#)
        let w = try parse(#"{"type":"fixed","name":"F2","size":4}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue([UInt8](repeating: 0, count: 4), writtenBy: w)
        }
    }

    // MARK: bytes/fixed decimal interop

    @Test func bytesDecimalFromFixedDecimal() throws {
        let r = try parse(#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#)
        let w = try parse(#"{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}"#)
        let b: [UInt8] = [0x30, 0x39]
        #expect(try r.resolveValue(b, writtenBy: w) as? [UInt8] == b)
    }

    @Test func fixedDecimalFromBytesDecimal() throws {
        let r = try parse(#"{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}"#)
        let w = try parse(#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#)
        let b: [UInt8] = [0x30, 0x39]
        #expect(try r.resolveValue(b, writtenBy: w) as? [UInt8] == b)
    }

    // MARK: union

    @Test func readerUnionMatchesBranch() throws {
        let r = try parse(#"["null","int"]"#)
        let w = try parse(#"{"type":"int"}"#)
        #expect(try r.resolveValue(Int32(5), writtenBy: w) as? Int32 == 5)
    }

    @Test func writerUnionMatchesBranch() throws {
        let r = try parse(#"{"type":"int"}"#)
        let w = try parse(#"["int","string"]"#)
        #expect(try r.resolveValue(Int32(5), writtenBy: w) as? Int32 == 5)
    }

    @Test func writerUnionNoMatchThrows() throws {
        let r = try parse(#"{"type":"boolean"}"#)
        let w = try parse(#"["int","string"]"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue(true, writtenBy: w)
        }
    }

    @Test func incompatibleSchemasThrow() throws {
        let r = try parse(#"{"type":"int"}"#)
        let w = try parse(#"{"type":"boolean"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try r.resolveValue(true, writtenBy: w)
        }
    }

    // MARK: record

    @Test func recordResolvesByName() throws {
        let r = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}"#)
        let w = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#)
        let dict = try r.resolveValue(["x": Int32(99)], writtenBy: w) as? [String: Any]
        #expect(dict?["x"] as? Int64 == 99)
    }

    @Test func recordResolvesByAlias() throws {
        let r = try parse(#"{"type":"record","name":"R","fields":[{"name":"newName","type":"int","aliases":["oldName"]}]}"#)
        let w = try parse(#"{"type":"record","name":"R","fields":[{"name":"oldName","type":"int"}]}"#)
        let dict = try r.resolveValue(["oldName": Int32(7)], writtenBy: w) as? [String: Any]
        #expect(dict?["newName"] as? Int32 == 7)
    }

    @Test func recordUsesReaderDefaultForMissingField() throws {
        let r = try parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int","default":0}]}"#)
        let w = try parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#)
        let dict = try r.resolveValue(["a": Int32(1)], writtenBy: w) as? [String: Any]
        #expect(dict?["b"] as? Int32 == 0)
    }

    @Test func recordMissingFieldNoDefaultThrows() throws {
        let r = try parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}"#)
        let w = try parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#)
        #expect(throws: AvroSchemaResolutionError.WriterFieldMissingWithoutDefaultValue) {
            try r.resolveValue(["a": Int32(1)], writtenBy: w)
        }
    }

    @Test func recordNonDictValueThrows() throws {
        let s = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.resolveValue("bad", writtenBy: s)
        }
    }

    @Test func errorSchemaResolvesLikeRecord() throws {
        let r = try parse(#"{"type":"error","name":"E","fields":[{"name":"msg","type":"string"}]}"#)
        let w = try parse(#"{"type":"error","name":"E","fields":[{"name":"msg","type":"string"}]}"#)
        let dict = try r.resolveValue(["msg": "oops"], writtenBy: w) as? [String: Any]
        #expect(dict?["msg"] as? String == "oops")
    }
}

// MARK: - AvroSchema+Evolution: defaultValue(from:)

@Suite("AvroSchema+Evolution – defaultValue")
struct EvolutionDefaultValueTests {

    @Test func nullFromNull() throws {
        #expect(try parse(#""null""#).defaultValue(from: .null) == nil)
    }

    @Test func nullFromNonNullThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#""null""#).defaultValue(from: .bool(true))
        }
    }

    @Test func boolFromBool() throws {
        #expect(try parse(#""boolean""#).defaultValue(from: .bool(false)) as? Bool == false)
    }

    @Test func boolFromNonBoolThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#""boolean""#).defaultValue(from: .null)
        }
    }

    @Test func intFromInt() throws {
        #expect(try parse(#"{"type":"int"}"#).defaultValue(from: .int(42)) as? Int32 == 42)
    }

    @Test func intFromDouble() throws {
        // jsonInt double branch
        #expect(try parse(#"{"type":"int"}"#).defaultValue(from: .double(5.0)) as? Int32 == 5)
    }

    @Test func intFromStringThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#"{"type":"int"}"#).defaultValue(from: .string("x"))
        }
    }

    @Test func intLogicalDate() throws {
        #expect(try parse(#"{"type":"int","logicalType":"date"}"#).defaultValue(from: .int(1)) is Date)
    }

    @Test func intLogicalTimeMillis() throws {
        #expect(try parse(#"{"type":"int","logicalType":"time-millis"}"#).defaultValue(from: .int(0)) is Date)
    }

    @Test func longDefault() throws {
        #expect(try parse(#"{"type":"long"}"#).defaultValue(from: .int(99)) as? Int64 == 99)
    }

    @Test func longTimestampMillis() throws {
        #expect(try parse(#"{"type":"long","logicalType":"timestamp-millis"}"#).defaultValue(from: .int(0)) is Date)
    }

    @Test func longTimestampMicros() throws {
        #expect(try parse(#"{"type":"long","logicalType":"timestamp-micros"}"#).defaultValue(from: .int(0)) is Date)
    }

    @Test func longTimeMicros() throws {
        #expect(try parse(#"{"type":"long","logicalType":"time-micros"}"#).defaultValue(from: .int(0)) is Date)
    }

    @Test func floatFromDouble() throws {
        #expect(try parse(#"{"type":"float"}"#).defaultValue(from: .double(1.5)) as? Float == 1.5)
    }

    @Test func floatFromInt() throws {
        // jsonDouble int branch
        #expect(try parse(#"{"type":"float"}"#).defaultValue(from: .int(2)) as? Float == 2.0)
    }

    @Test func floatFromStringThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#"{"type":"float"}"#).defaultValue(from: .string("x"))
        }
    }

    @Test func doubleDefault() throws {
        #expect(try parse(#"{"type":"double"}"#).defaultValue(from: .double(2.5)) as? Double == 2.5)
    }

    @Test func stringDefault() throws {
        #expect(try parse(#"{"type":"string"}"#).defaultValue(from: .string("hi")) as? String == "hi")
    }

    @Test func stringFromNonStringThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#"{"type":"string"}"#).defaultValue(from: .int(1))
        }
    }

    @Test func bytesDefault() throws {
        #expect(try parse(#"{"type":"bytes"}"#).defaultValue(from: .string("ab")) as? [UInt8] == Array("ab".utf8))
    }

    @Test func bytesFromNonStringThrows() throws {
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try parse(#"{"type":"bytes"}"#).defaultValue(from: .int(1))
        }
    }

    @Test func fixedCorrectSize() throws {
        let s = try parse(#"{"type":"fixed","name":"F","size":2}"#)
        #expect(try s.defaultValue(from: .string("ab")) as? [UInt8] == Array("ab".utf8))
    }

    @Test func fixedWrongSizeThrows() throws {
        let s = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .string("ab"))
        }
    }

    @Test func fixedNonStringThrows() throws {
        let s = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .int(0))
        }
    }

    @Test func enumValidSymbol() throws {
        let s = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        #expect(try s.defaultValue(from: .string("B")) as? String == "B")
    }

    @Test func enumInvalidSymbolThrows() throws {
        let s = try parse(#"{"type":"enum","name":"E","symbols":["A","B"]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .string("Z"))
        }
    }

    @Test func enumNonStringThrows() throws {
        let s = try parse(#"{"type":"enum","name":"E","symbols":["A"]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .int(0))
        }
    }

    @Test func arrayDefault() throws {
        let s = try parse(#"{"type":"array","items":"int"}"#)
        let result = try s.defaultValue(from: .array([.int(1), .int(2)])) as? [Any]
        #expect(result?.count == 2)
        #expect(result?.first as? Int32 == 1)
    }

    @Test func arrayNonArrayThrows() throws {
        let s = try parse(#"{"type":"array","items":"int"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .object([:]))
        }
    }

    @Test func mapDefault() throws {
        let s = try parse(#"{"type":"map","values":"int"}"#)
        let result = try s.defaultValue(from: .object(["k": .int(3)])) as? [String: Any]
        #expect(result?["k"] as? Int32 == 3)
    }

    @Test func mapNonObjectThrows() throws {
        let s = try parse(#"{"type":"map","values":"int"}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .array([]))
        }
    }

    @Test func recordAllFields() throws {
        let s = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}"#)
        let result = try s.defaultValue(from: .object(["x": .int(1), "y": .string("hi")])) as? [String: Any]
        #expect(result?["x"] as? Int32 == 1)
        #expect(result?["y"] as? String == "hi")
    }

    @Test func recordFieldFromDefault() throws {
        let s = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":5}]}"#)
        let result = try s.defaultValue(from: .object([:])) as? [String: Any]
        #expect(result?["x"] as? Int32 == 5)
    }

    @Test func recordMissingFieldNoDefaultThrows() throws {
        let s = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#)
        #expect(throws: AvroSchemaResolutionError.WriterFieldMissingWithoutDefaultValue) {
            try s.defaultValue(from: .object([:]))
        }
    }

    @Test func recordNonObjectThrows() throws {
        let s = try parse(#"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .array([]))
        }
    }

    @Test func errorSchemaDefault() throws {
        let s = try parse(#"{"type":"error","name":"E","fields":[{"name":"m","type":"string"}]}"#)
        let result = try s.defaultValue(from: .object(["m": .string("err")])) as? [String: Any]
        #expect(result?["m"] as? String == "err")
    }

    @Test func unionDefaultFromFirstBranch() throws {
        let s = try parse(#"["null","int"]"#)
        #expect(try s.defaultValue(from: .null) == nil)
    }

    @Test func unionMismatchedBranchThrows() throws {
        let s = try parse(#"["null"]"#)
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .int(1))
        }
    }

    @Test func unknownSchemaDefaultThrows() throws {
        let s = AvroSchema.unknownSchema(AvroSchema.UnknownSchema("test"))
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try s.defaultValue(from: .null)
        }
    }
}
