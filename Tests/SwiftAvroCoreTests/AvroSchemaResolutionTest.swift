import Foundation
import Testing
@testable import SwiftAvroCore

private func parse(_ json: String) throws -> AvroSchema {
    let avro = Avro()
    return try #require(avro.decodeSchema(schema: json))
}

// MARK: - Associated-value accessors

@Suite("AvroSchema – accessors")
struct SchemaAccessorTests {

    @Test("getEnumSymbols / getEnumIndex on enum")
    func enumAccessors() throws {
        let schema = try parse(#"""
        {"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}
        """#)
        #expect(schema.getEnumSymbols() == ["RED", "GREEN", "BLUE"])
        #expect(schema.getEnumIndex("GREEN") == 1)
        #expect(schema.getEnumIndex("PURPLE") == nil)
    }

    @Test("getEnumSymbols on non-enum returns empty")
    func nonEnumAccessor() throws {
        #expect(try parse(#"{"type":"int"}"#).getEnumSymbols().isEmpty)
        #expect(try parse(#"{"type":"int"}"#).getEnumIndex("X") == nil)
    }

    @Test("getMapAttribute / getMapValues")
    func mapAccessor() throws {
        let schema = try parse(#"{"type":"map","values":"int"}"#)
        #expect(schema.getMapAttribute() != nil)
        #expect(schema.getMapValues() != nil)
        #expect(try parse(#"{"type":"int"}"#).getMapAttribute() == nil)
        #expect(try parse(#"{"type":"int"}"#).getMapValues() == nil)
    }

    @Test("getInt / getLong / getBytes")
    func intLongBytes() throws {
        #expect(try parse(#"{"type":"int"}"#).getInt() != nil)
        #expect(try parse(#"{"type":"long"}"#).getLong() != nil)
        #expect(try parse(#"{"type":"bytes"}"#).getBytes() != nil)
        // Negative cases
        #expect(try parse(#"{"type":"long"}"#).getInt() == nil)
        #expect(try parse(#"{"type":"int"}"#).getLong() == nil)
        #expect(try parse(#"{"type":"int"}"#).getBytes() == nil)
    }

    @Test("getFixedSize / getArrayItems / getUnionList")
    func sizeItemsUnion() throws {
        let fixed = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        #expect(fixed.getFixedSize() == 4)
        #expect(try parse(#"{"type":"int"}"#).getFixedSize() == nil)

        let arr = try parse(#"{"type":"array","items":"int"}"#)
        #expect(arr.getArrayItems() != nil)
        #expect(try parse(#"{"type":"int"}"#).getArrayItems() == nil)

        let uni = try parse(#"["null","int"]"#)
        #expect(uni.getUnionList().count == 2)
        #expect(try parse(#"{"type":"int"}"#).getUnionList().isEmpty)
    }

    @Test("getRecord / getError / getField return associated values")
    func recordErrorField() throws {
        let rec = try parse(#"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#)
        #expect(rec.getRecord() != nil)
        #expect(try parse(#"{"type":"int"}"#).getRecord() == nil)
        #expect(try parse(#"{"type":"int"}"#).getError() == nil)
        #expect(try parse(#"{"type":"int"}"#).getField() == nil)
    }

    @Test("getRecordInnerTypes returns field types or empty")
    func recordInnerTypes() throws {
        let rec = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int"},
          {"name":"b","type":"string"}]}
        """#)
        let inner = rec.getRecordInnerTypes()
        #expect(inner.count == 2)
        #expect(try parse(#"{"type":"int"}"#).getRecordInnerTypes().isEmpty)
    }

    @Test("getSerializedSchema for various variants")
    func serializedSchema() throws {
        let rec = try parse(#"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#)
        let recFields = rec.getSerializedSchema()
        #expect(recFields.count == 1)
        let arr = try parse(#"{"type":"array","items":"int"}"#)
        #expect(arr.getSerializedSchema().count == 1)
        let map = try parse(#"{"type":"map","values":"int"}"#)
        #expect(map.getSerializedSchema().count == 1)
        let uni = try parse(#"["null","int"]"#)
        #expect(uni.getSerializedSchema().count == 1)
        let prim = try parse(#"{"type":"int"}"#)
        #expect(prim.getSerializedSchema().count == 1)
    }
}

// MARK: - Schema resolution

@Suite("AvroSchema – schema resolution")
struct SchemaResolutionTests {

    @Test("identical primitive schemas resolve")
    func samePrimitive() throws {
        var a = try parse(#"{"type":"int"}"#)
        let b = try parse(#"{"type":"int"}"#)
        try a.resolving(from: b)
    }

    @Test("int → long promotion")
    func intToLong() throws {
        var reader = try parse(#"{"type":"long"}"#)
        let writer = try parse(#"{"type":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("int → float promotion")
    func intToFloat() throws {
        var reader = try parse(#"{"type":"float"}"#)
        let writer = try parse(#"{"type":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("int → double promotion")
    func intToDouble() throws {
        var reader = try parse(#"{"type":"double"}"#)
        let writer = try parse(#"{"type":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("long → float promotion")
    func longToFloat() throws {
        var reader = try parse(#"{"type":"float"}"#)
        let writer = try parse(#"{"type":"long"}"#)
        try reader.resolving(from: writer)
    }

    @Test("long → double promotion")
    func longToDouble() throws {
        var reader = try parse(#"{"type":"double"}"#)
        let writer = try parse(#"{"type":"long"}"#)
        try reader.resolving(from: writer)
    }

    @Test("float → double promotion")
    func floatToDouble() throws {
        var reader = try parse(#"{"type":"double"}"#)
        let writer = try parse(#"{"type":"float"}"#)
        try reader.resolving(from: writer)
    }

    @Test("string ↔ bytes promotion")
    func stringBytes() throws {
        var reader = try parse(#"{"type":"bytes"}"#)
        let writer = try parse(#"{"type":"string"}"#)
        try reader.resolving(from: writer)
    }

    @Test("bytes → string promotion")
    func bytesToString() throws {
        var reader = try parse(#"{"type":"string"}"#)
        let writer = try parse(#"{"type":"bytes"}"#)
        try reader.resolving(from: writer)
    }

    @Test("incompatible primitive throws SchemaMismatch")
    func incompatiblePrimitive() throws {
        var reader = try parse(#"{"type":"int"}"#)
        let writer = try parse(#"{"type":"boolean"}"#)
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("array same items resolves")
    func arraySame() throws {
        var reader = try parse(#"{"type":"array","items":"int"}"#)
        let writer = try parse(#"{"type":"array","items":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("map same values resolves")
    func mapSame() throws {
        var reader = try parse(#"{"type":"map","values":"int"}"#)
        let writer = try parse(#"{"type":"map","values":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("enum same resolves with .accept")
    func enumSame() throws {
        var reader = try parse(#"""
        {"type":"enum","name":"E","symbols":["A","B"]}
        """#)
        let writer = try parse(#"""
        {"type":"enum","name":"E","symbols":["A","B"]}
        """#)
        try reader.resolving(from: writer)
    }

    @Test("fixed same resolves with .accept")
    func fixedSame() throws {
        var reader = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        let writer = try parse(#"{"type":"fixed","name":"F","size":4}"#)
        try reader.resolving(from: writer)
    }

    @Test("union reader matches scalar writer")
    func unionReaderScalarWriter() throws {
        var reader = try parse(#"["int","string"]"#)
        let writer = try parse(#"{"type":"int"}"#)
        try reader.resolving(from: writer)
    }

    @Test("scalar reader matches union writer")
    func scalarReaderUnionWriter() throws {
        var reader = try parse(#"{"type":"int"}"#)
        let writer = try parse(#"["null","int"]"#)
        try reader.resolving(from: writer)
    }

    @Test("scalar reader against union writer with no match throws")
    func scalarReaderUnionNoMatchThrows() throws {
        var reader = try parse(#"{"type":"int"}"#)
        let writer = try parse(#"["null","string"]"#)
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("record same fields resolves")
    func recordSameFields() throws {
        var reader = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int"},
          {"name":"b","type":"string"}]}
        """#)
        let writer = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int"},
          {"name":"b","type":"string"}]}
        """#)
        try reader.resolving(from: writer)
    }

    @Test("union reader against union writer that has no matching branch returns false-like")
    func unionReaderUnionWriterMismatch() throws {
        var reader = try parse(#"["int"]"#)
        let writer = try parse(#"["string"]"#)
        // resolvingDifferent's reader-union path doesn't throw — it just
        // returns false, propagating to outer resolving's SchemaMismatch.
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("string vs int (no promotion) throws")
    func stringIntThrows() throws {
        var reader = try parse(#"{"type":"string"}"#)
        let writer = try parse(#"{"type":"int"}"#)
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }
}
