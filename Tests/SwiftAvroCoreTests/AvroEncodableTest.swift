//
//  Changes vs original:
//
//  1. testTime: was an exact duplicate of testLong. Changed to test the
//     time-millis logical type (int + logicalType:"time-millis") with Int32.
//
//  2. testInnerDuration: the original Model struct had a wrapping `fields`
//     property that had no matching key in the schema (schema only had a
//     top-level "requestType" field). Flattened to match the schema.
//
//  3. testOptionalUnionsFirstLastNil: the schema string contained stray
//     backslashes — ["null\","string"] — which made it invalid JSON, causing
//     decodeSchema to return nil and the force-unwrap to crash. Fixed to
//     ["null","string"]. Also removed a trailing comma after the last field.
//
//  4. All XCTAssert(false, ...) replaced with XCTFail(...).
//
//  5. testNestedRecord: this test exposed the union-encoding cursor bug in
//     AvroKeyedEncodingContainer (see AvroEncoder.swift). The test itself is
//     correct; the fix is in the encoder.

import XCTest
@testable import SwiftAvroCore

class AvroEncodableTest: XCTestCase {

    var schema: AvroSchema = AvroSchema()

    override func setUp() {
        super.setUp()
        let schemaJson = """
        {
          "type": "record",
          "fields": [
            {"name": "requestId",   "type": "int"},
            {"name": "requestName", "type": "string"},
            {"name": "requestType", "type": {"type": "fixed", "size": 4}},
            {"name": "parameter",   "type": {"type": "array",  "items": "int"}},
            {"name": "parameter2",  "type": {"type": "map",    "values": "int"}}
          ]
        }
        """
        let avro = Avro()
        schema = avro.decodeSchema(schema: schemaJson)!
    }

    override func tearDown() {
        super.tearDown()
    }

    // MARK: - Primitive types

    func testBoolean() {
        let jsonSchema = "{ \"type\" : \"boolean\" }"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()

        if let falseValue = try? encoder.encode(false, schema: schema) {
            XCTAssertEqual(falseValue, Data([0x00]), "Value should be false.")
        } else {
            XCTFail("Failed to encode false.")
        }

        if let trueValue = try? encoder.encode(true, schema: schema) {
            XCTAssertEqual(trueValue, Data([0x01]), "Value should be true.")
        } else {
            XCTFail("Failed to encode true.")
        }
    }

    func testInt() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x03]
        let jsonSchema = "{ \"type\" : \"int\" }"
        let source: Int32 = 3_209_099
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Byte arrays don't match.")
        } else {
            XCTFail("Failed to encode Int.")
        }
    }

    func testLong() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x03]
        let jsonSchema = "{ \"type\" : \"long\" }"
        let source: Int64 = 3_209_099
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Byte arrays don't match.")
        } else {
            XCTFail("Failed to encode Long.")
        }
    }

    /// FIX: was a duplicate of testLong. Now tests the time-millis logical type.
    /// Avro time-millis is stored as an int (zigzag varint), representing
    /// milliseconds after midnight. The binary encoding is identical to int.
    func testTime() {
        let avroBytes: [UInt8] = [0x96, 0xde, 0x87, 0x03]
        // time-millis schema: underlying type is int
        let jsonSchema = "{ \"type\" : \"int\", \"logicalType\": \"time-millis\" }"
        let source: Int32 = 3_209_099   // 53 min 29.099 s after midnight
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "time-millis encoding mismatch.")
        } else {
            XCTFail("Failed to encode time-millis.")
        }
    }

    func testFloat() {
        let avroBytes: [UInt8] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"
        let source: Float = 3.14
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Byte arrays don't match.")
        } else {
            XCTFail("Failed to encode Float.")
        }
    }

    func testDouble() {
        let avroBytes: [UInt8] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"
        let source: Double = 3.14
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Byte arrays don't match.")
        } else {
            XCTFail("Failed to encode Double.")
        }
    }

    func testDate() {
        let avroBytes: [UInt8] = [0x00]
        let jsonSchema = "{ \"type\" : \"int\", \"logicalType\": \"date\" }"
        let source = Date(timeIntervalSince1970: 0)
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Date encoding mismatch.")
        } else {
            XCTFail("Failed to encode Date.")
        }
    }

    func testString() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"string\" }"
        let source = "foo"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "String encoding mismatch.")
        } else {
            XCTFail("Failed to encode String.")
        }
    }

    // MARK: - Complex types

    func testEnum() {
        let avroBytes: [UInt8] = [0x12]
        let jsonSchema = """
        {
          "type": "enum",
          "name": "ChannelKey",
          "doc": "Enum of valid channel keys.",
          "symbols": [
            "CityIphone","CityMobileWeb","GiltAndroid","GiltcityCom",
            "GiltCom","GiltIpad","GiltIpadSafari","GiltIphone",
            "GiltMobileWeb","NoChannel"
          ]
        }
        """
        enum ChannelKey: String, Codable {
            case CityIphone, CityMobileWeb, GiltAndroid, GiltcityCom
            case GiltCom, GiltIpad, GiltIpadSafari, GiltIphone
            case GiltMobileWeb, NoChannel
        }
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(ChannelKey.NoChannel, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Enum encoding mismatch.")
        } else {
            XCTFail("Failed to encode Enum.")
        }
    }

    func testBytes() {
        let avroBytes: [UInt8] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"
        let source: [UInt8] = [0x66, 0x6f, 0x6f]
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Bytes encoding mismatch.")
        } else {
            XCTFail("Failed to encode Bytes.")
        }
    }

    func testFixed() {
        let avroBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let jsonSchema = "{ \"type\" : \"fixed\", \"size\" : 4 }"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(avroBytes, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Fixed encoding mismatch.")
        } else {
            XCTFail("Failed to encode Fixed.")
        }
    }

    func testDuration() {
        let source: [UInt32] = [1, 1, 1970]
        let expected: [UInt8] = [0x01,0x00,0x00,0x00,
                                 0x01,0x00,0x00,0x00,
                                 0xB2,0x07,0x00,0x00]
        let jsonSchema = """
        { "type": "fixed", "size": 12, "logicalType": "duration" }
        """
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(expected), "Duration encoding mismatch.")
        } else {
            XCTFail("Failed to encode Duration.")
        }
    }

    /// FIX: the original wrapped the encodable value in a nested `Model(fields:)`
    /// struct that had no matching key in the schema. The schema only has a
    /// top-level "requestType" field, so the struct must be flat.
    func testInnerDuration() {
        struct Model: Encodable {
            let requestType: [UInt32] = [1, 1, 1970]
        }
        let expected: [UInt8] = [0x01,0x00,0x00,0x00,
                                 0x01,0x00,0x00,0x00,
                                 0xB2,0x07,0x00,0x00]
        let jsonSchema = """
        {
          "type": "record",
          "fields": [
            {"name": "requestType", "type": {"type": "fixed", "size": 12, "logicalType": "duration"}}
          ]
        }
        """
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(Model(), schema: schema) {
            XCTAssertEqual(value, Data(expected), "Inner duration encoding mismatch.")
        } else {
            XCTFail("Failed to encode inner Duration.")
        }
    }

    func testArray() {
        let avroBytes: [UInt8] = [0x04, 0x06, 0x36, 0x00]
        let source: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertEqual(value, Data(avroBytes), "Array encoding mismatch.")
        } else {
            XCTFail("Failed to encode Array.")
        }
    }

    func testMap() {
        // Dict iteration order is non-deterministic, so accept both orderings.
        let avroBytes1: [UInt8] = [
            0x04,
            0x06, 0x62, 0x6f, 0x6f, 0x04, 0x08, 0x38, 0x00,  // "boo":[4,28]
            0x06, 0x66, 0x6f, 0x6f, 0x04, 0x06, 0x36, 0x00,  // "foo":[3,27]
            0x00
        ]
        let avroBytes2: [UInt8] = [
            0x04,
            0x06, 0x66, 0x6f, 0x6f, 0x04, 0x06, 0x36, 0x00,  // "foo":[3,27]
            0x06, 0x62, 0x6f, 0x6f, 0x04, 0x08, 0x38, 0x00,  // "boo":[4,28]
            0x00
        ]
        let source: [String: [Int64]] = ["boo": [4, 28], "foo": [3, 27]]
        let jsonSchema = """
        { "type": "map", "values": {"type": "array", "items": "long"} }
        """
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()
        if let value = try? encoder.encode(source, schema: schema) {
            XCTAssertTrue(value == Data(avroBytes1) || value == Data(avroBytes2),
                          "Map encoding mismatch.")
        } else {
            XCTFail("Failed to encode Map.")
        }
    }

    func testUnion() {
        let jsonSchema = "[\"null\",\"string\"]"
        let avro = Avro()
        let schema = avro.decodeSchema(schema: jsonSchema)!
        let encoder = AvroEncoder()

        struct Arg { let bytes: [UInt8]; let source: String? }
        let cases = [
            Arg(bytes: [0x02, 0x02, 0x61], source: "a"),
            Arg(bytes: [0x00],             source: nil),
        ]
        for a in cases {
            if let value = try? encoder.encode(a.source, schema: schema) {
                XCTAssertEqual(value, Data(a.bytes), "Union encoding mismatch.")
            } else {
                XCTFail("Failed to encode Union.")
            }
        }
    }

    func testRecord() {
        struct Model: Codable {
            let requestId:   Int32
            let requestName: String
            let requestType: [UInt8]
            let parameter:   [Int32]
            let parameter2:  [String: Int32]
        }
        let expected = Data([
            0x54,
            0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
            0x01, 0x02, 0x03, 0x04,
            0x04, 0x02, 0x04,
            0x00,
            0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04,
            0x00
        ])
        let model = Model(requestId: 42, requestName: "hello",
                          requestType: [1,2,3,4], parameter: [1,2],
                          parameter2: ["foo": 2])
        let encoder = AvroEncoder()
        if let data = try? encoder.encode(model, schema: schema) {
            XCTAssertEqual(data, expected, "Record encoding mismatch.")
        } else {
            XCTFail("Failed to encode Record.")
        }
    }

    func testRequest() {
        struct Arg { let model: HandshakeRequest; let expected: Data }
        let encoder = AvroEncoder()
        guard let testSchema = Avro().newSchema(schema: MessageConstant.requestSchema) else {
            XCTFail("Could not build request schema.")
            return
        }
        let cases: [Arg] = [
            Arg(model: HandshakeRequest(
                    clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    clientProtocol: nil,
                    serverHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    meta: nil),
                expected: Data([
                    0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x00,
                    0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x00
                ])),
            Arg(model: HandshakeRequest(
                    clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    clientProtocol: "foo",
                    serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    meta: nil),
                expected: Data([
                    0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x02, 0x06, 0x66, 0x6f, 0x6f,
                    0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x00
                ])),
            Arg(model: HandshakeRequest(
                    clientHash: [0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    clientProtocol: "foo",
                    serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    meta: ["fo": [1,2,3]]),
                expected: Data([
                    0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x02, 0x06, 0x66, 0x6f, 0x6f,
                    0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                    0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x02, 0x02, 0x04, 0x66, 0x6f, 0x06, 0x1, 0x2, 0x3, 0x00
                ])),
        ]
        for t in cases {
            XCTAssertNoThrow(try {
                let data = try encoder.encode(t.model, schema: testSchema)
                let hexString = data.map { String(format: "%02hhx,", $0) }.joined()
                print(hexString)
                XCTAssertEqual(data, t.expected, "HandshakeRequest encoding mismatch.")
            }())
        }
    }

    func testResponse() {
        struct Arg { let model: HandshakeResponse; let expected: Data }
        let encoder = AvroEncoder()
        guard let testSchema = Avro().newSchema(schema: MessageConstant.responseSchema) else {
            XCTFail("Could not build response schema.")
            return
        }
        let cases: [Arg] = [
            Arg(model: HandshakeResponse(match: .BOTH, serverProtocol: nil, serverHash: nil),
                expected: Data([0x00, 0x00, 0x00, 0x00])),
            Arg(model: HandshakeResponse(
                    match: .CLIENT,
                    serverProtocol: "foo",
                    serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf]),
                expected: Data([
                    0x02,
                    0x02, 0x06, 0x66, 0x6f, 0x6f,
                    0x02, 0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                          0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf,
                    0x00
                ])),
            Arg(model: HandshakeResponse(
                    match: .CLIENT,
                    serverProtocol: "foo",
                    serverHash: [0x1,0x1,0x2,0x3,0x4,0x5,0x6,0x7,
                                 0x8,0x9,0xa,0xb,0xc,0xd,0xe,0xf],
                    meta: ["fo": [1,2,3]]),
                expected: Data([
                    0x02,
                    0x02, 0x06, 0x66, 0x6f, 0x6f,
                    0x02, 0x01,0x01,0x02,0x03,0x04,0x05,0x06,0x07,
                          0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
                    0x02, 0x02, 0x04, 0x66, 0x6f, 0x06, 0x01, 0x02, 0x03, 0x00
                ])),
        ]
        for t in cases {
            XCTAssertNoThrow(try {
                let data = try encoder.encode(t.model, schema: testSchema)
                XCTAssertEqual(data, t.expected, "HandshakeResponse encoding mismatch.")
            }())
        }
    }

    // MARK: - Nested record / optional union (the main regression test)

    /// This test exercises the union encoding cursor bug that caused
    /// testNestedRecord to fail. The bug: after encoding an optional field,
    /// the schema cursor was either not advanced (nil case) or advanced twice
    /// (non-nil case), so every field after the first optional was encoded
    /// against the wrong schema.
    ///
    /// Fix is in AvroKeyedEncodingContainer — field schemas are now looked up
    /// by name, not by cursor position.
    func testNestedRecord() {
        struct Model: Codable {
            let requestId:      Int32
            let requestName:    String
            let optionalDouble: Double?
            let requestType:    [UInt8]
            let parameter:      [Int32]
            let parameter2:     [String: Int32]
        }
        struct Wrapper: Codable {
            let message: Model
            let name:    String
        }

        let schemaJson = """
        {
          "name": "wrapper", "type": "record",
          "fields": [
            { "name": "message", "type": {
                "name": "message", "type": "record",
                "fields": [
                  {"name": "requestId",      "type": "int"},
                  {"name": "requestName",    "type": "string"},
                  {"name": "optionalDouble", "type": ["null", "double"]},
                  {"name": "requestType",    "type": {"type": "fixed", "size": 4}},
                  {"name": "parameter",      "type": {"type": "array",  "items": "int"}},
                  {"name": "parameter2",     "type": {"type": "map",    "values": "int"}}
                ]
            }},
            {"name": "name", "type": "string"}
          ]
        }
        """
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let encoder = AvroEncoder()
        let decoder = AvroDecoder(schema: schema)

        // --- non-nil optional ---
        let modelWithDouble = Model(
            requestId: 42, requestName: "hello", optionalDouble: 3.14,
            requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let wrapperWithDouble = Wrapper(message: modelWithDouble, name: "test")

        let expectedWithDouble = Data([
            0x54,
            0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f,        // "hello"
            0x02,                                        // union index 1 (double)
            0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40,  // 3.14
            0x01, 0x02, 0x03, 0x04,                      // fixed
            0x04, 0x02, 0x04, 0x00,                      // array [1,2]
            0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x00,   // map {"foo":2}
            0x08, 0x74, 0x65, 0x73, 0x74                 // "test"
        ])

        XCTAssertNoThrow(try {
            let data = try encoder.encode(wrapperWithDouble, schema: schema)
            XCTAssertEqual(data, expectedWithDouble,
                           "Nested record with non-nil optional: encoding mismatch.\n" +
                           "got:      \(data.map { String(format: "0x%02x", $0) }.joined(separator: " "))\n" +
                           "expected: \(expectedWithDouble.map { String(format: "0x%02x", $0) }.joined(separator: " "))")
        }())

        // Decode round-trip
        if let decoded = try? decoder.decode(Wrapper.self, from: expectedWithDouble) {
            XCTAssertEqual(decoded.message.optionalDouble, 3.14)
            XCTAssertEqual(decoded.message.requestId, 42)
            XCTAssertEqual(decoded.message.requestName, "hello")
            XCTAssertEqual(decoded.name, "test")
        } else {
            XCTFail("Failed to decode nested record with non-nil optional.")
        }

        // --- nil optional ---
        let modelWithNil = Model(
            requestId: 42, requestName: "hello", optionalDouble: nil,
            requestType: [1,2,3,4], parameter: [1,2], parameter2: ["foo": 2])
        let wrapperWithNil = Wrapper(message: modelWithNil, name: "test")

        let expectedWithNil = Data([
            0x54,
            0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
            0x00,                                        // union index 0 (null)
            // no value bytes for null
            0x01, 0x02, 0x03, 0x04,
            0x04, 0x02, 0x04, 0x00,
            0x02, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x00,
            0x08, 0x74, 0x65, 0x73, 0x74
        ])

        XCTAssertNoThrow(try {
            let data = try encoder.encode(wrapperWithNil, schema: schema)
            XCTAssertEqual(data, expectedWithNil,
                           "Nested record with nil optional: encoding mismatch.\n" +
                           "got:      \(data.map { String(format: "0x%02x", $0) }.joined(separator: " "))\n" +
                           "expected: \(expectedWithNil.map { String(format: "0x%02x", $0) }.joined(separator: " "))")
        }())

        if let decoded = try? decoder.decode(Wrapper.self, from: expectedWithNil) {
            XCTAssertNil(decoded.message.optionalDouble,
                         "optionalDouble should be nil after round-trip.")
        } else {
            XCTFail("Failed to decode nested record with nil optional.")
        }
    }

    func testNestedRecordJson() {
        struct Model: Codable {
            let requestId:   Int32
            let requestName: String
            let requestType: [UInt8]
            let parameter:   [Int32]
            let parameter2:  [String: Int32]
        }
        struct Wrapper: Codable {
            let message: Model
            let name:    String
        }
        let schemaJson = """
        {
          "name": "wrapper", "type": "record",
          "fields": [
            { "name": "message", "type": {
                "name": "message", "type": "record",
                "fields": [
                  {"name": "requestId",   "type": "int"},
                  {"name": "requestName", "type": "string"},
                  {"name": "requestType", "type": {"type": "fixed", "size": 4}},
                  {"name": "parameter",   "type": {"type": "array",  "items": "int"}},
                  {"name": "parameter2",  "type": {"type": "map",    "values": "int"}}
                ]
            }},
            {"name": "name", "type": "string"}
          ]
        }
        """
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = Model(requestId: 42, requestName: "hello",
                          requestType: [1,2,3,4], parameter: [1,2],
                          parameter2: ["foo": 2])
        let wrapper = Wrapper(message: model, name: "test")
        let encoder = AvroEncoder()
        let codingKey = CodingUserInfoKey(rawValue: "encodeOption")!
        encoder.setUserInfo(userInfo: [codingKey: AvroEncodingOption.AvroJson])

        guard let data = try? encoder.encode(wrapper, schema: schema),
              let jsonDecoded = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            XCTFail("Failed to encode or decode JSON.")
            return
        }

        XCTAssertEqual(jsonDecoded["name"] as? String, "test")
        let msg = jsonDecoded["message"] as! [String: Any]
        XCTAssertEqual(msg["requestName"] as? String, "hello")
        XCTAssertEqual(msg["requestId"] as? Int, 42)
        XCTAssertEqual(msg["requestType"] as? String, "\u{01}\u{02}\u{03}\u{04}")
        XCTAssertEqual((msg["parameter2"] as! [String: Int])["foo"], 2)
        XCTAssertEqual(msg["parameter"] as! [Int], [1, 2])
    }

    // MARK: - Optional union edge cases

    func testOptionalUnionsMixedNil() {
        let schemaJson = """
        {
          "type": "record",
          "name": "RecordIncludeOptionalUnions",
          "namespace": "org.avro.test",
          "fields": [
            {"name": "doulbeField",   "type": "double"},
            {"name": "doulbeField2",  "type": "double"},
            {"name": "stringField",   "type": "string"},
            {"name": "optionalDouble",  "type": ["null","double"]},
            {"name": "optionalDouble2", "type": ["null","double"]},
            {"name": "optionalString",  "type": ["null","string"]},
            {"name": "optionalString2", "type": ["null","string"]}
          ]
        }
        """
        struct R: Codable, Equatable {
            let doulbeField:    Double
            let doulbeField2:   Double
            let stringField:    String
            let optionalDouble:  Double?
            let optionalDouble2: Double?
            let optionalString:  String?
            let optionalString2: String?
        }
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = R(doulbeField: 0.1, doulbeField2: 0.2, stringField: "abc",
                      optionalDouble: nil, optionalDouble2: 0.3,
                      optionalString: nil, optionalString2: "def")
        let encoder = AvroEncoder()
        guard let data = try? encoder.encode(model, schema: schema) else {
            XCTFail("Encoding failed.")
            return
        }
        let decoder = AvroDecoder(schema: schema)
        if let got = try? decoder.decode(R.self, from: data) {
            XCTAssertEqual(got, model, "Round-trip mismatch.")
        } else {
            XCTFail("testOptionalUnionsMixedNil decoding failed.")
        }
    }

    /// FIX: original schema had invalid JSON escape sequences ["null\","string"]
    /// (backslash before closing quote). Removed the stray backslashes.
    /// Also removed trailing comma after last field (invalid JSON).
    func testOptionalUnionsFirstLastNil() {
        let schemaJson = """
        {
          "type": "record",
          "fields": [
            {"name": "doubleField",    "type": "double"},
            {"name": "stringFieldOpt", "type": ["null","string"]},
            {"name": "doubleFieldOpt3","type": ["null","double"]},
            {"name": "doubleFieldOpt1","type": ["null","double"]},
            {"name": "doubleFieldOpt2","type": ["null","double"]}
          ]
        }
        """
        struct R: Codable, Equatable {
            var doubleField:    Double
            var stringFieldOpt: String?
            var doubleFieldOpt1: Double?
            var doubleFieldOpt2: Double?
            var doubleFieldOpt3: Double?
        }
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = R(doubleField: 22.0, stringFieldOpt: nil,
                      doubleFieldOpt1: nil, doubleFieldOpt2: 99.0,
                      doubleFieldOpt3: 33.0)
        let encoder = AvroEncoder()
        guard let data = try? encoder.encode(model, schema: schema) else {
            XCTFail("Encoding failed.")
            return
        }
        let decoder = AvroDecoder(schema: schema)
        if let got = try? decoder.decode(R.self, from: data) {
            XCTAssertEqual(got, model, "Round-trip mismatch.")
        } else {
            XCTFail("testOptionalUnionsFirstLastNil decoding failed.")
        }
    }

    func testOptionalUnionsComplexType() {
        let schemaJson = """
        {
          "type": "record",
          "fields": [
            {"name": "doubleField",    "type": "double"},
            {"name": "stringFieldOpt", "type": ["null","string"]},
            {"name": "doubleFieldOpt1","type": ["null","double"]},
            {"name": "recordFieldOpt1","type": ["null",{
              "type":"record","name":"innerRecord",
              "fields":[{"name":"Name","type":"string"},{"name":"Number","type":"int"}]
            }]},
            {"name": "recordFieldOpt2","type": ["null",{
              "type":"record","name":"innerRecord",
              "fields":[{"name":"Name","type":"string"},{"name":"Number","type":"int"}]
            }]}
          ]
        }
        """
        struct Inner: Codable, Equatable {
            var Name:   String
            var Number: Int32
        }
        struct R: Codable, Equatable {
            var doubleField:    Double
            var stringFieldOpt: String?
            var doubleFieldOpt1: Double?
            var recordFieldOpt1: Inner?
            var recordFieldOpt2: Inner?
        }
        let schema = Avro().decodeSchema(schema: schemaJson)!
        let model = R(doubleField: 11.0, stringFieldOpt: "abc",
                      doubleFieldOpt1: nil, recordFieldOpt1: nil,
                      recordFieldOpt2: Inner(Name: "tom", Number: 123))
        let encoder = AvroEncoder()
        guard let data = try? encoder.encode(model, schema: schema) else {
            XCTFail("Encoding failed.")
            return
        }
        let decoder = AvroDecoder(schema: schema)
        if let got = try? decoder.decode(R.self, from: data) {
            XCTAssertEqual(got, model, "Round-trip mismatch.")
        } else {
            XCTFail("testOptionalUnionsComplexType decoding failed.")
        }
    }

    // MARK: - Performance

    func testPerformanceExample() {
        self.measure {
            for _ in 0...1000 { testNestedRecord() }
        }
    }

    // MARK: - allTests

    static var allTests = [
        ("testString",                       testString),
        ("testBytes",                        testBytes),
        ("testFixed",                        testFixed),
        ("testInt",                          testInt),
        ("testLong",                         testLong),
        ("testTime",                         testTime),
        ("testFloat",                        testFloat),
        ("testDouble",                       testDouble),
        ("testBoolean",                      testBoolean),
        ("testEnum",                         testEnum),
        ("testArray",                        testArray),
        ("testMap",                          testMap),
        ("testUnion",                        testUnion),
        ("testRecord",                       testRecord),
        ("testNestedRecord",                 testNestedRecord),
        ("testOptionalUnionsMixedNil",        testOptionalUnionsMixedNil),
        ("testOptionalUnionsFirstLastNil",    testOptionalUnionsFirstLastNil),
        ("testOptionalUnionsComplexType",     testOptionalUnionsComplexType),
    ]
}
