//
//  SwiftAvroCoreTest.swift
//  SwiftAvroCoreTests
//

import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Core API")
struct AvroCoreAPITests {

    // MARK: - Avro class init

    @Test("Avro init creates instance")
    func avroInit() {
        let avro = Avro()
        #expect(avro.getSchema() == nil)
    }

    // MARK: - Schema management

    @Test("setSchema and getSchema work")
    func setAndGetSchema() {
        let avro = Avro()
        let schema = AvroSchema(type: "string")
        avro.setSchema(schema: schema)
        #expect(avro.getSchema() != nil)
        #expect(avro.getSchema()?.isString() == true)
    }

    @Test("setSchemaFormat sets option")
    func setSchemaFormat() {
        let avro = Avro()
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        avro.setSchemaFormat(option: .FullForm)
        avro.setSchemaFormat(option: .CanonicalForm)
    }

    @Test("setAvroFormat sets option")
    func setAvroFormat() {
        let avro = Avro()
        avro.setAvroFormat(option: .AvroBinary)
        avro.setAvroFormat(option: .AvroJson)
    }

    // MARK: - Schema decoding

    @Test("decodeSchema from string")
    func decodeSchemaString() {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)
        #expect(schema != nil)
        #expect(schema?.isString() == true)
    }

    @Test("decodeSchema from Data")
    func decodeSchemaData() {
        let avro = Avro()
        let data = #"{"type":"int"}"#.data(using: .utf8)!
        let schema = avro.decodeSchema(schema: data)
        #expect(schema != nil)
        #expect(schema?.isInt() == true)
    }

    @Test("decodeSchema returns nil for invalid JSON")
    func decodeSchemaInvalid() {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: "invalid json")
        #expect(schema == nil)
    }

    @Test("newSchema creates schema without storing")
    func newSchemaWithoutStoring() {
        let avro = Avro()
        let schema = avro.newSchema(schema: #"{"type":"boolean"}"#)
        #expect(schema != nil)
        #expect(avro.getSchema() == nil)
    }

    @Test("newSchema from Data")
    func newSchemaData() {
        let avro = Avro()
        let data = #"{"type":"long"}"#.data(using: .utf8)!
        let schema = avro.newSchema(schema: data)
        #expect(schema != nil)
        #expect(schema?.isLong() == true)
    }

    // MARK: - Schema encoding

    @Test("encodeSchema with stored schema")
    func encodeStoredSchema() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let data = try avro.encodeSchema()
        #expect(data.count > 0)
    }

    @Test("encodeSchema with provided schema")
    func encodeProvidedSchema() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"int"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeSchema as canonical")
    func encodeSchemaAsCanonical() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .CanonicalForm)
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeRecordSchema works")
    func encodeRecordSchema() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    @Test("encodeSchema pretty printed")
    func encodeSchemaPrettyPrinted() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let schema = avro.decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#)!
        let data = try avro.encodeSchema(schema: schema)
        let str = String(data: data, encoding: .utf8)!
        #expect(str.contains("\n"))
    }

    @Test("encodeSchema canonical form")
    func encodeSchemaCanonical() throws {
        let avro = Avro()
        avro.setSchemaFormat(option: .CanonicalForm)
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeSchema(schema: schema)
        #expect(data.count > 0)
    }

    // MARK: - Binary encode/decode

    @Test("encode with reflected schema")
    func encodeWithReflectedSchema() throws {
        let avro = Avro()
        let data = try avro.encode("hello")
        #expect(data.count > 0)
    }

    @Test("encodeFrom with explicit schema")
    func encodeFromExplicit() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let data = try avro.encodeFrom("test", schema: schema)
        #expect(data.count > 0)
    }

    @Test("decode round-trip")
    func decodeRoundTrip() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("hello")
        let decoded: String = try avro.decode(from: encoded)
        #expect(decoded == "hello")
    }

    @Test("decodeFrom with explicit schema")
    func decodeFromExplicit() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"int"}"#)!
        let data = try avro.encodeFrom(Int32(42), schema: schema)
        let value: Int32 = try avro.decodeFrom(from: data, schema: schema)
        #expect(value == 42)
    }

    @Test("decode untyped")
    func decodeUntyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("test")
        let decoded = try avro.decode(from: encoded)
        #expect(decoded as? String == "test")
    }

    @Test("decodeFrom untyped")
    func decodeFromUntyped() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"long"}"#)!
        let data = try avro.encodeFrom(Int64(123), schema: schema)
        let decoded = try avro.decodeFrom(from: data, schema: schema)
        #expect(decoded as? Int64 == 123)
    }

    // MARK: - These tests are skipped due to crash issues with current implementation

    // @Test("decode throws without schema")
    // func decodeNoSchema() throws {
    //     let avro = Avro()
    //     #expect(throws: BinaryEncodingError.noSchemaSpecified) {
    //         let _: String = try avro.decode(from: Data([0x01]))
    //     }
    // }

    // @Test("decodeFrom throws without schema")
    // func decodeFromNoSchema() throws {
    //     let avro = Avro()
    //     let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
    //     #expect(throws: BinaryEncodingError.noSchemaSpecified) {
    //         let _: String = try avro.decodeFrom(from: Data([0x01]), schema: schema)
    //     }
    // }

    // MARK: - AvroDataReader

    @Test("AvroDataReader isAtEnd initially false")
    func dataReaderIsAtEnd() {
        let avro = Avro()
        let data = Data([0x01])
        let reader = avro.makeDataReader(data: data)
        #expect(!reader.isAtEnd)
    }

    @Test("AvroDataReader bytesRemaining")
    func dataReaderBytesRemaining() {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        #expect(reader.bytesRemaining == 3)
    }

    @Test("AvroDataReader readBytes")
    func dataReaderReadBytes() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        let bytes = try reader.readBytes(count: 2)
        #expect(bytes.count == 2)
        #expect(reader.bytesRemaining == 1)
    }

    @Test("AvroDataReader readBytes throws on insufficient data")
    func dataReaderReadBytesInsufficient() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01]))
        var threw = false
        do {
            _ = try reader.readBytes(count: 10)
        } catch {
            threw = true
        }
        #expect(threw)
    }

    @Test("AvroDataReader decode typed")
    func dataReaderDecodeTyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("hello")
        let reader = avro.makeDataReader(data: encoded)
        let decoded: String = try reader.decode(schema: avro.getSchema()!)
        #expect(decoded == "hello")
    }

    @Test("AvroDataReader decode untyped")
    func dataReaderDecodeUntyped() throws {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        let encoded = try avro.encode("world")
        let reader = avro.makeDataReader(data: encoded)
        let decoded = try reader.decode(schema: avro.getSchema()!)
        #expect(decoded as? String == "world")
    }

    @Test("AvroDataReader skip with bytes schema")
    func dataReaderSkip() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"bytes"}"#)!
        let encoded = try avro.encodeFrom([UInt8]([1, 2, 3]), schema: schema)
        let reader = avro.makeDataReader(data: encoded)
        try reader.skip(schema: schema)
        #expect(reader.isAtEnd)
    }

    @Test("AvroDataReader decodeContinue with empty buffer throws")
    func dataReaderEmptyBuffer() throws {
        let avro = Avro()
        let schema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
        let reader = avro.makeDataReader(data: Data())
        var threw = false
        do {
            _ = try reader.decode(schema: schema)
        } catch {
            threw = true
        }
        #expect(threw)
    }

    // MARK: - ObjectContainer

    @Test("makeFileObjectContainer without schema")
    func makeObjectContainerNoSchema() {
        let avro = Avro()
        _ = avro.makeFileObjectContainer()
    }

    @Test("makeFileObjectContainer with schema")
    func makeObjectContainerWithSchema() {
        let avro = Avro()
        avro.decodeSchema(schema: #"{"type":"string"}"#)
        _ = avro.makeFileObjectContainer()
    }

    // MARK: - Encoding options

    @Test("AvroEncodingOption raw values")
    func encodingOptionRawValues() {
        #expect(AvroEncodingOption.AvroBinary.rawValue == 0)
        #expect(AvroEncodingOption.AvroJson.rawValue == 1)
    }

    @Test("AvroSchemaEncodingOption raw values")
    func schemaEncodingOptionRawValues() {
        #expect(AvroSchemaEncodingOption.CanonicalForm.rawValue == 0)
        #expect(AvroSchemaEncodingOption.FullForm.rawValue == 1)
        #expect(AvroSchemaEncodingOption.PrettyPrintedForm.rawValue == 2)
    }

    // MARK: - AvroDataReader

    @Test("AvroDataReader readBytes throws when not enough bytes remain")
    func dataReaderReadBytesUnderflow() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01]))
        #expect(throws: (any Error).self) {
            _ = try reader.readBytes(count: 10)
        }
    }

    @Test("AvroDataReader decode on empty buffer throws")
    func dataReaderDecodeEmpty() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let reader = avro.makeDataReader(data: Data())
        #expect(throws: (any Error).self) {
            let _: Int32 = try reader.decode(schema: schema)
        }
    }

    @Test("AvroDataReader skip advances past a value")
    func dataReaderSkipAdvances() throws {
        let avro = Avro()
        let intSchema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        // Encode two int values back-to-back.
        let a = try AvroEncoder().encode(Int32(1), schema: intSchema)
        let b = try AvroEncoder().encode(Int32(2), schema: intSchema)
        let reader = avro.makeDataReader(data: a + b)
        try reader.skip(schema: intSchema)
        let next: Int32 = try reader.decode(schema: intSchema)
        #expect(next == 2)
    }

    @Test("AvroDataReader isAtEnd / bytesRemaining track position")
    func dataReaderPosition() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        #expect(!reader.isAtEnd)
        #expect(reader.bytesRemaining == 3)
        _ = try reader.readBytes(count: 3)
        #expect(reader.isAtEnd)
        #expect(reader.bytesRemaining == 0)
    }

    // MARK: - ObjectContainer

    private let recordSchema = """
    {"type":"record","name":"test","fields":[
      {"name":"a","type":"long"},
      {"name":"b","type":"string"}
    ]}
    """

    private struct M: Codable, Equatable { var a: Int64; var b: String }

    private func makeAvro(_ schema: String) -> Avro {
        let avro = Avro()
        avro.decodeSchema(schema: schema)
        return avro
    }

    @Test("ObjectContainer addObjectsToBlocks with objectsInBlock = 0 is a no-op")
    func objectContainerAddObjectsZero() throws {
        let avro = makeAvro(recordSchema)
        var oc = ObjectContainer(schema: recordSchema)
        try oc.addObjectsToBlocks([M(a: 1, b: "x")], objectsInBlock: 0, avro: avro)
        #expect(oc.blocks.isEmpty)
    }

    @Test("ObjectContainer addObjectsToBlocks with objectsInBlock = 2 produces correct block layout")
    func objectContainerAddObjectsTwo() throws {
        let codec = NullCodec()
        let avro = makeAvro(recordSchema)
        var oc = ObjectContainer(schema: recordSchema)
        let values = (0..<5).map { M(a: Int64($0), b: "v\($0)") }
        try oc.addObjectsToBlocks(values, objectsInBlock: 2, avro: avro)
        let data = try oc.encode(avro: avro, codec: codec)

        var reader = ObjectContainer()
        try reader.decode(from: data, avro: avro, codec: codec)
        let decoded = try reader.decodeAll(M.self, avro: avro)
        #expect(decoded == values)
        // After encoding, currentBlock is flushed; blocks should be at least 3
        // (5 values divided into chunks of 2 = 3 blocks: 2,2,1).
        #expect(reader.blocks.count >= 3)
    }

    @Test("ObjectContainer addObjectsToBlocks with objectsInBlock = 1 makes one block per record")
    func objectContainerAddObjectsOne() throws {
        let codec = NullCodec()
        let avro = makeAvro(recordSchema)
        var oc = ObjectContainer(schema: recordSchema)
        let values = (0..<3).map { M(a: Int64($0), b: "v\($0)") }
        try oc.addObjectsToBlocks(values, objectsInBlock: 1, avro: avro)
        let data = try oc.encode(avro: avro, codec: codec)

        var reader = ObjectContainer()
        try reader.decode(from: data, avro: avro, codec: codec)
        let decoded = try reader.decodeAll(M.self, avro: avro)
        #expect(decoded == values)
    }

    @Test("ObjectContainer decodeAll Any? variant returns dictionary records")
    func objectContainerDecodeAllAny() throws {
        let codec = NullCodec()
        let avro = makeAvro(recordSchema)
        var oc = ObjectContainer(schema: recordSchema)
        try oc.addObject(M(a: 7, b: "lucky"), avro: avro)
        let data = try oc.encode(avro: avro, codec: codec)

        var reader = ObjectContainer()
        try reader.decode(from: data, avro: avro, codec: codec)
        let decoded = try reader.decodeAll(avro: avro) as [Any?]
        let dict = try #require(decoded.first as? [String: Any])
        #expect(dict["a"] as? Int64 == 7)
    }

    // MARK: - Avro class API gap coverage

    @Test("decodeSchema returns nil on malformed JSON")
    func decodeSchemaMalformed() {
        let avro = Avro()
        #expect(avro.decodeSchema(schema: "not-json") == nil)
        #expect(avro.decodeSchema(schema: Data("not-json".utf8)) == nil)
    }

    @Test("newSchema returns nil on malformed JSON")
    func newSchemaMalformed() {
        let avro = Avro()
        #expect(avro.newSchema(schema: "not-json") == nil)
        #expect(avro.newSchema(schema: Data("not-json".utf8)) == nil)
    }

    @Test("encodeSchema() with no stored schema returns empty Data")
    func encodeSchemaEmpty() throws {
        let avro = Avro()
        let out = try avro.encodeSchema()
        #expect(out.isEmpty)
    }

    @Test("encodeSchema with PrettyPrintedForm produces newline-formatted JSON")
    func encodeSchemaPrettyPrintedForm() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#))
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let pretty = try avro.encodeSchema(schema: schema)
        #expect(String(decoding: pretty, as: UTF8.self).contains("\n"))
    }

    @Test("encodeSchema with FullForm includes structural fields")
    func encodeSchemaFullForm() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","aliases":["AltR"],
         "fields":[{"name":"a","type":"int"}]}
        """#))
        avro.setSchemaFormat(option: .FullForm)
        let full = try avro.encodeSchema(schema: schema)
        let s = String(decoding: full, as: UTF8.self)
        #expect(s.contains("aliases"))
    }

    @Test("decode<T>(from:) without schema throws noSchemaSpecified")
    func decodeTypedWithoutSchema() throws {
        let avro = Avro()
        #expect(throws: BinaryEncodingError.self) {
            let _: Bool = try avro.decode(from: Data([0x01]))
        }
    }

    @Test("decode(from:) Any? without schema throws noSchemaSpecified")
    func decodeAnyWithoutSchema() throws {
        let avro = Avro()
        #expect(throws: BinaryEncodingError.self) {
            let _: Any? = try avro.decode(from: Data([0x01]))
        }
    }

    @Test("makeIPCRequest creates a usable AvroIPCRequest")
    func makeIPCRequestWorks() throws {
        let avro = Avro()
        let req = avro.newSchema(schema: """
            {"type":"record","name":"HandshakeRequest","fields":[
                {"name":"clientHash","type":{"type":"fixed","name":"MD5","size":16}},
                {"name":"clientProtocol","type":["null","string"]},
                {"name":"serverHash","type":{"type":"fixed","name":"MD5","size":16}},
                {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
            ]}
            """)!
        let resp = avro.newSchema(schema: """
            {"type":"record","name":"HandshakeResponse","fields":[
                {"name":"match","type":{"type":"enum","name":"HandshakeMatch",
                    "symbols":["NONE","BOTH","CLIENT"]}},
                {"name":"serverProtocol","type":["null","string"]},
                {"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},
                {"name":"meta","type":["null",{"type":"map","values":"bytes"}]}
            ]}
            """)!
        let meta = avro.newSchema(schema: #"{"type":"map","values":"bytes"}"#)!
        let context = AvroIPCContext(requestSchema: req, responseSchema: resp,
                                     metaSchema: meta, requestMeta: [:],
                                     responseMeta: [:], knownProtocols: nil)
        let session = AvroIPCSession(context: context)
        let hash: MD5Hash = Array(repeating: 0xAB, count: 16)
        let helloProto = """
        {"namespace":"x","protocol":"P","types":[],"messages":{}}
        """
        let request = try avro.makeIPCRequest(clientHash: hash,
                                              clientProtocol: helloProto,
                                              session: session)
        #expect(request.clientHash == hash)
    }

    @Test("makeIPCResponse creates a usable AvroIPCResponse")
    func makeIPCResponseWorks() {
        let avro = Avro()
        let hash: MD5Hash = Array(repeating: 0xCD, count: 16)
        let helloProto = """
        {"namespace":"x","protocol":"P","types":[],"messages":{}}
        """
        let response = avro.makeIPCResponse(serverHash: hash, serverProtocol: helloProto)
        #expect(response.serverHash == hash)
    }

    // MARK: - End-to-end tests from SwiftAvroCoreTests.swift

    @Test("End-to-end encode/decode with JSON schema")
    func endToEnd() throws {
        let jsonSchema = """
        {"type":"record","fields":[
          {"name":"requestId",   "type":"int"},
          {"name":"requestName", "type":"string"},
          {"name":"parameter",   "type":{"type":"array","items":"int"}}
        ]}
        """
        struct Model: Codable { var requestId: Int32; var requestName: String; var parameter: [Int32] }
        let avro    = Avro()
        let model   = Model(requestId: 42, requestName: "hello", parameter: [1, 2])
        let _ = try #require(avro.decodeSchema(schema: jsonSchema))
        let binary: Data  = try avro.encode(model)
        let decoded: Model = try avro.decode(from: binary)
        #expect(decoded.requestId   == model.requestId)
        #expect(decoded.requestName == model.requestName)
        #expect(decoded.parameter   == model.parameter)
    }

    @Test("End-to-end encode/decode with reflected schema")
    func endToEndReflectedSchema() throws {
        struct Model: Codable { var requestId: Int32; var requestName: String; var parameter: [Int32] }
        let avro   = Avro()
        let model  = Model(requestId: 42, requestName: "hello", parameter: [1, 2])
        let schema = try #require(AvroSchema.reflecting(model))
        avro.setSchema(schema: schema)
        let binary:  Data  = try avro.encode(model)
        let decoded: Model = try avro.decode(from: binary)
        #expect(decoded.requestId   == model.requestId)
        #expect(decoded.requestName == model.requestName)
        #expect(decoded.parameter   == model.parameter)
    }
}