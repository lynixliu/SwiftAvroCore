import Foundation
import Testing
import SwiftAvroCore

@Suite("JSONValue Tests")
struct JSONValueTests {

    @Test("JSONValue equatable works within same type")
    func equatableSameType() {
        #expect(JSONValue.null == .null)
        #expect(JSONValue.bool(true) == .bool(true))
        #expect(JSONValue.bool(true) != .bool(false))
        #expect(JSONValue.int(42) == .int(42))
        #expect(JSONValue.int(42) != .int(43))
        #expect(JSONValue.double(3.14) == .double(3.14))
        #expect(JSONValue.double(3.14) != .double(2.71))
        #expect(JSONValue.string("a") == .string("a"))
        #expect(JSONValue.string("a") != .string("b"))
        #expect(JSONValue.array([.int(1)]) == .array([.int(1)]))
        #expect(JSONValue.array([.int(1)]) != .array([.int(2)]))
        #expect(JSONValue.object(["a": .int(1)]) == .object(["a": .int(1)]))
        #expect(JSONValue.object(["a": .int(1)]) != .object(["b": .int(1)]))
    }

    @Test("JSONValue equatable across different types returns false")
    func equatableAcrossTypes() {
        #expect(JSONValue.int(1) != .string("1"))
        #expect(JSONValue.bool(true) != .int(1))
        #expect(JSONValue.null != .int(0))
        #expect(JSONValue.string("") != .null)
        #expect(JSONValue.array([]) != .object([:]))
    }

    @Test("JSONValue used in Avro encoder - int")
    func usedInAvroInt() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: "\"int\""))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode(Int32(42))
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("42"))
    }

    @Test("JSONValue used in Avro encoder - string")
    func usedInAvroString() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: "\"string\""))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode("hello")
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("hello"))
    }

    @Test("JSONValue used in Avro encoder - bool")
    func usedInAvroBool() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: "\"boolean\""))
        avro.setSchema(schema: schema)
        avro.setAvroFormat(option: .AvroJson)
        let data = try avro.encode(true)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("true"))
    }
}
