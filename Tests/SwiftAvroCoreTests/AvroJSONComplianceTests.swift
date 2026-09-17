import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro JSON Compliance")
struct AvroJSONComplianceTests {

    private func makeJSONDecoder(schema: AvroSchema) -> AvroDecoder {
        let decoder = AvroDecoder(schema: schema)
        let key = CodingUserInfoKey(rawValue: "encodeOption")!
        decoder.setUserInfo(userInfo: [key: AvroEncodingOption.AvroJson])
        return decoder
    }

    @Test("Simple union decodes correctly")
    func simpleUnion() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"string": "hello"}"#.data(using: .utf8))
        
        let result: String? = try decoder.decode(String?.self, from: data)
        #expect(result == "hello")
    }

    @Test("Null union decodes correctly")
    func nullUnion() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"null": null}"#.data(using: .utf8))
        
        let result: String? = try decoder.decode(String?.self, from: data)
        #expect(result == nil)
    }

    @Test("Record union decodes correctly")
    func recordUnion() throws {
        struct User: Codable, Equatable { let name: String }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        ["null", {"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}]
        """#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"User": {"name": "Alice"}}"#.data(using: .utf8))
        
        let result: User? = try decoder.decode(User?.self, from: data)
        #expect(result == User(name: "Alice"))
    }

    @Test("Logical date decodes correctly")
    func logicalDate() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"19159"#.data(using: .utf8)) // 2023-01-01 (approx)
        
        let result: Date = try decoder.decode(Date.self, from: data)
        #expect(result != Date(timeIntervalSince1970: 0))
    }

    @Test("Strict type enforcement throws on mismatch")
    func strictTypeEnforcement() throws {
        struct Model: Codable { let age: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"age","type":"int"}]}
        """#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"age": "thirty"}"#.data(using: .utf8))
        
        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: data)
        }
    }

    @Test("Array of unions decodes correctly")
    func arrayOfUnions() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","string"]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"[{"string": "a"}, {"null": null}]"#.data(using: .utf8))
        
        let result: [String?] = try decoder.decode([String?].self, from: data)
        #expect(result == ["a", nil])
    }

    @Test("Map of unions decodes correctly")
    func mapOfUnions() throws {
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"map","values":["null","string"]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"key": {"string": "val"}}"#.data(using: .utf8))
        
        let result: [String: String?] = try decoder.decode([String: String?].self, from: data)
        #expect(result["key"] == "val")
    }

    @Test("Nested union decodes correctly")
    func nestedUnion() throws {
        struct Inner: Codable, Equatable { let val: String }
        struct Outer: Codable, Equatable { let inner: Inner? }
        
        let schema = try #require(Avro().decodeSchema(schema: #"""
        {"type":"record","name":"Outer","fields":[
          {"name":"inner","type":["null", {"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}]}
        ]}
        """#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"inner": {"Inner": {"val": "deep"}}}"#.data(using: .utf8))
        
        let result: Outer = try decoder.decode(Outer.self, from: data)
        #expect(result.inner == Inner(val: "deep"))
    }

    @Test("Bare null decodes as the null branch")
    func bareNullUnion() throws {
        // The spec writes the null branch as a bare JSON null, not as an object.
        let schema = try #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require("null".data(using: .utf8))

        let result: String? = try decoder.decode(String?.self, from: data)
        #expect(result == nil)
    }

    @Test("Whole numbers decode against a double schema")
    func wholeNumberDouble() throws {
        // JSON writes 1.0 as 1, so a double schema must accept an integer literal.
        struct Model: Codable { let v: Double }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"double"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)

        let whole: Model = try decoder.decode(Model.self, from: try #require(#"{"v": 1}"#.data(using: .utf8)))
        #expect(whole.v == 1.0)
        let fractional: Model = try decoder.decode(Model.self, from: try #require(#"{"v": 1.5}"#.data(using: .utf8)))
        #expect(fractional.v == 1.5)
    }

    @Test("Whole numbers decode against a float schema")
    func wholeNumberFloat() throws {
        struct Model: Codable { let v: Float }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"float"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)

        let result: Model = try decoder.decode(Model.self, from: try #require(#"{"v": 2}"#.data(using: .utf8)))
        #expect(result.v == 2.0)
    }

    @Test("Union names a logical-type branch by its underlying primitive")
    func unionLogicalTypeBranch() throws {
        struct Model: Codable { let v: Date? }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":["null",{"type":"int","logicalType":"date"}]}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"v": {"int": 19159}}"#.data(using: .utf8))

        let result: Model = try decoder.decode(Model.self, from: data)
        #expect(result.v == LogicalTypeConverter.decodeDate(19159))
    }

    @Test("Unknown union branch name is rejected")
    func unknownUnionBranch() throws {
        struct Inner: Codable { let x: String }
        struct Model: Codable { let v: Inner? }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Outer","fields":[{"name":"v","type":["null",{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}]}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"v": {"Bogus": {"x":"a"}}}"#.data(using: .utf8))

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: data)
        }
    }

    @Test("Bytes decode from a one-character-per-byte string")
    func bytesFromString() throws {
        struct Model: Codable { let v: [UInt8] }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let text = "\u{00ff}\u{0041}"
        let data = try #require(#"{"v": "\#(text)"}"#.data(using: .utf8))

        let result: Model = try decoder.decode(Model.self, from: data)
        #expect(result.v == [0xFF, 0x41])
    }

    @Test("Data decodes from a bytes schema")
    func dataFromBytes() throws {
        struct Model: Codable { let v: Data }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"v": "Hi"}"#.data(using: .utf8))

        let result: Model = try decoder.decode(Model.self, from: data)
        #expect(Array(result.v) == [0x48, 0x69])
    }

    @Test("Code point above U+00FF is rejected for bytes")
    func bytesOutOfRange() throws {
        struct Model: Codable { let v: [UInt8] }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let text = "\u{0100}"
        let data = try #require(#"{"v": "\#(text)"}"#.data(using: .utf8))

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: data)
        }
    }

    @Test("Fixed decodes at the declared size and rejects other sizes")
    func fixedSize() throws {
        struct Model: Codable { let v: [UInt8] }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"fixed","name":"Tide","size":4}}]}"#))
        let decoder = makeJSONDecoder(schema: schema)

        let result: Model = try decoder.decode(Model.self, from: try #require(#"{"v": "ABCD"}"#.data(using: .utf8)))
        #expect(result.v == [0x41, 0x42, 0x43, 0x44])

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: try #require(#"{"v": "AB"}"#.data(using: .utf8)))
        }
    }

    @Test("Enum accepts a declared symbol and rejects any other")
    func enumSymbols() throws {
        struct Model: Codable { let v: String }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}"#))
        let decoder = makeJSONDecoder(schema: schema)

        let result: Model = try decoder.decode(Model.self, from: try #require(#"{"v": "B"}"#.data(using: .utf8)))
        #expect(result.v == "B")

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: try #require(#"{"v": "Z"}"#.data(using: .utf8)))
        }
    }

    @Test("A number is rejected against a string schema")
    func numberAgainstStringSchema() throws {
        struct Model: Codable { let v: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"v": 5}"#.data(using: .utf8))

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: data)
        }
    }

    @Test("Value beyond the int range is rejected")
    func intOverflow() throws {
        struct Model: Codable { let v: Int32 }
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"{"v": 5000000000}"#.data(using: .utf8))

        #expect(throws: (any Error).self) {
            let _: Model = try decoder.decode(Model.self, from: data)
        }
    }

    @Test("setAvroFormat reaches the typed decode paths")
    func facadeTypedJSONDecode() throws {
        // AvroDecoder defaults to binary, so a decode path that does not carry
        // the chosen format reads Avro JSON as binary.
        struct Model: Codable, Equatable { let name: String }
        let avro = Avro()
        avro.setAvroFormat(option: .AvroJson)
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}"#))
        let data = try #require(#"{"name":"hello"}"#.data(using: .utf8))

        let stateless: Model = try avro.decodeFrom(from: data, schema: schema)
        #expect(stateless == Model(name: "hello"))

        avro.setSchema(schema: schema)
        let stored: Model = try avro.decode(from: data)
        #expect(stored == Model(name: "hello"))
    }

    @Test("setAvroFormat reaches the untyped decode paths")
    func facadeUntypedJSONDecode() throws {
        let avro = Avro()
        avro.setAvroFormat(option: .AvroJson)
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}"#))
        let data = try #require(#"{"name":"hello"}"#.data(using: .utf8))

        let result = try avro.decodeFrom(from: data, schema: schema) as? [String: Any]
        #expect(result?["name"] as? String == "hello")
    }

    @Test("Facade round-trips its own Avro JSON output")
    func facadeRoundTrip() throws {
        struct Model: Codable, Equatable { let name: String; let count: Int32 }
        let avro = Avro()
        avro.setAvroFormat(option: .AvroJson)
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"name","type":"string"},{"name":"count","type":"int"}]}"#))

        let original = Model(name: "hello", count: 7)
        let encoded = try avro.encodeFrom(original, schema: schema)
        let decoded: Model = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(decoded == original)
    }

    @Test("Binary stays the default format")
    func facadeBinaryUnchanged() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #""int""#))
        let encoded = try avro.encodeFrom(Int32(42), schema: schema)

        let typed: Int32 = try avro.decodeFrom(from: encoded, schema: schema)
        #expect(typed == 42)
        #expect(try avro.decodeFrom(from: encoded, schema: schema) as? Int32 == 42)
    }

    @Test("Null between values keeps the array in step")
    func nullInsideArray() throws {
        // decodeNil() must not consume the element that it reports on.
        let schema = try #require(Avro().decodeSchema(schema: #"{"type":"array","items":["null","string"]}"#))
        let decoder = makeJSONDecoder(schema: schema)
        let data = try #require(#"[{"string":"a"}, null, {"string":"b"}]"#.data(using: .utf8))

        let result: [String?] = try decoder.decode([String?].self, from: data)
        #expect(result == ["a", nil, "b"])
    }
}
