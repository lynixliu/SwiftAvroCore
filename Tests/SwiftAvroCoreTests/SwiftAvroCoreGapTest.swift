import Foundation
import Testing
@testable import SwiftAvroCore

// MARK: - Public Avro API gap coverage

@Suite("Avro public API – gap coverage")
struct AvroPublicAPIGapTests {

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
    func prettyPrintedSchema() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#))
        avro.setSchemaFormat(option: .PrettyPrintedForm)
        let pretty = try avro.encodeSchema(schema: schema)
        #expect(String(decoding: pretty, as: UTF8.self).contains("\n"))
    }

    @Test("encodeSchema with FullForm includes structural fields")
    func fullFormSchema() throws {
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
}

// MARK: - AvroDataReader gap coverage

@Suite("AvroDataReader – gap coverage")
struct AvroDataReaderGapTests {

    @Test("readBytes throws when not enough bytes remain")
    func readBytesUnderflow() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01]))
        #expect(throws: (any Error).self) {
            _ = try reader.readBytes(count: 10)
        }
    }

    @Test("decode on empty buffer throws Empty data buffer")
    func decodeOnEmptyBuffer() throws {
        let avro = Avro()
        let schema = try #require(avro.decodeSchema(schema: #"{"type":"int"}"#))
        let reader = avro.makeDataReader(data: Data())
        #expect(throws: (any Error).self) {
            let _: Int32 = try reader.decode(schema: schema)
        }
    }

    @Test("skip advances past a value")
    func skipAdvances() throws {
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

    @Test("isAtEnd / bytesRemaining track position")
    func position() throws {
        let avro = Avro()
        let reader = avro.makeDataReader(data: Data([0x01, 0x02, 0x03]))
        #expect(!reader.isAtEnd)
        #expect(reader.bytesRemaining == 3)
        _ = try reader.readBytes(count: 3)
        #expect(reader.isAtEnd)
        #expect(reader.bytesRemaining == 0)
    }
}

// MARK: - ObjectContainer gap coverage

@Suite("ObjectContainer – gap coverage")
struct ObjectContainerGapTests {

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

    @Test("addObjectsToBlocks with objectsInBlock = 0 is a no-op")
    func addObjectsToBlocksZero() throws {
        let avro = makeAvro(recordSchema)
        var oc = ObjectContainer(schema: recordSchema)
        try oc.addObjectsToBlocks([M(a: 1, b: "x")], objectsInBlock: 0, avro: avro)
        #expect(oc.blocks.isEmpty)
    }

    @Test("addObjectsToBlocks with objectsInBlock = 2 produces correct block layout")
    func addObjectsToBlocksTwo() throws {
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

    @Test("addObjectsToBlocks with objectsInBlock = 1 makes one block per record")
    func addObjectsToBlocksOne() throws {
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

    @Test("decodeAll Any? variant returns dictionary records")
    func decodeAllAny() throws {
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
}
