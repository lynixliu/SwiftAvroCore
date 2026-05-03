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

    @Test("findSchema returns nil for non-matching name")
    func findSchemaMissing() throws {
        let rec = try parse(#"{"type":"record","name":"Person","fields":[]}"#)
        #expect(rec.findSchema(name: "NonExistent") == nil)
    }

    @Test("validate accepts a well-formed record")
    func validateRecord() throws {
        var schema = try parse(#"{"type":"record","name":"R","fields":[]}"#)
        try schema.validate(typeName: "record", name: "R", nameSpace: nil)
    }

    @Test("isDecimal returns false for non-decimal schemas")
    func isDecimalNegative() {
        #expect(!AvroSchema(type: "bytes").isDecimal())
        #expect(!AvroSchema(type: "int").isDecimal())
    }

    @Test("getTypeName on plain longSchema (no logicalType) returns 'long'")
    func getTypeNameLongNoLogicalType() {
        let schema = AvroSchema.longSchema(AvroSchema.IntSchema(isLong: true))
        #expect(schema.getTypeName() == "long")
    }

    @Test("getTypeName on plain bytesSchema (no logicalType) returns 'bytes'")
    func getTypeNameBytesNoLogicalType() {
        let schema = AvroSchema.bytesSchema(AvroSchema.BytesSchema())
        #expect(schema.getTypeName() == "bytes")
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

    @Test("equal unions resolve through union switch case")
    func unionEqualsUnion() throws {
        var reader = try parse(#"["int","string"]"#)
        let writer = try parse(#"["int","string"]"#)
        try reader.resolving(from: writer)
    }

    @Test("decimal-bytes reader resolves against decimal-fixed writer")
    func bytesDecimalToFixedDecimal() throws {
        var reader = try parse(#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#)
        let writer = try parse(#"{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":4,"scale":2}"#)
        try reader.resolving(from: writer)
    }

    @Test("decimal-fixed reader resolves against decimal-bytes writer")
    func fixedDecimalToBytesDecimal() throws {
        var reader = try parse(#"{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":4,"scale":2}"#)
        let writer = try parse(#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#)
        try reader.resolving(from: writer)
    }
}

// MARK: - findSchema branches

@Suite("AvroSchema – findSchema")
struct FindSchemaTests {

    @Test("findSchema on record returns matching field type")
    func recordFindMatchingField() throws {
        let schema = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int"},
          {"name":"b","type":"string"}]}
        """#)
        #expect(schema.findSchema(name: "a") != nil)
        #expect(schema.findSchema(name: "missing") == nil)
    }

    @Test("findSchema with name fields returns fieldsSchema")
    func recordFindFieldsKey() throws {
        let schema = try parse(#"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#)
        let result = schema.findSchema(name: "fields")
        #expect(result != nil)
    }

    @Test("findSchema on union descends into branches")
    func unionFindBranch() throws {
        let schema = try parse(#"["int","string"]"#)
        #expect(schema.findSchema(name: "int") != nil)
        #expect(schema.findSchema(name: "missing") == nil)
    }

    @Test("findSchema on enum returns self for matching symbol")
    func enumFindSymbol() throws {
        let schema = try parse(#"""
        {"type":"enum","name":"Color","symbols":["RED","GREEN"]}
        """#)
        #expect(schema.findSchema(name: "RED") != nil)
        #expect(schema.findSchema(name: "BLUE") == nil)
    }

    @Test("findSchema on primitive returns self only when name matches")
    func primitiveFindSelf() throws {
        let schema = try parse(#"{"type":"int"}"#)
        #expect(schema.findSchema(name: "int") != nil)
        #expect(schema.findSchema(name: "long") == nil)
    }
}

// MARK: - Type predicates on matching types

@Suite("AvroSchema – type predicates")
struct TypePredicateTests {

    @Test("isRecord / isField return true for matching schemas")
    func recordAndField() throws {
        let record = try parse(#"""
        {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}
        """#)
        #expect(record.isRecord())
        #expect(!record.isField())
        // FieldSchema is reachable via getSerializedSchema(record) which wraps fields as .fieldSchema.
        let serialized = record.getSerializedSchema()
        let fieldSchema = try #require(serialized.first)
        #expect(fieldSchema.isField())
        #expect(!fieldSchema.isRecord())
    }

    @Test("isNamed returns true for record/enum/fixed and false otherwise")
    func isNamed() throws {
        #expect(try parse(#"""
        {"type":"record","name":"R","fields":[]}
        """#).isNamed())
        #expect(try parse(#"""
        {"type":"enum","name":"E","symbols":["A"]}
        """#).isNamed())
        #expect(try parse(#"{"type":"fixed","name":"F","size":4}"#).isNamed())
        #expect(!(try parse(#"{"type":"int"}"#).isNamed()))
    }

    @Test("isContainer covers array/map/record/union/fixed and is false for primitives")
    func isContainer() throws {
        #expect(try parse(#"{"type":"array","items":"int"}"#).isContainer())
        #expect(try parse(#"{"type":"map","values":"int"}"#).isContainer())
        #expect(try parse(#"""
        {"type":"record","name":"R","fields":[]}
        """#).isContainer())
        #expect(try parse(#"["null","int"]"#).isContainer())
        #expect(try parse(#"{"type":"fixed","name":"F","size":4}"#).isContainer())
        #expect(!(try parse(#"{"type":"int"}"#).isContainer()))
    }
}

// MARK: - Validate covers errorSchema branch

@Suite("AvroSchema – validate")
struct ValidateTests {

    @Test("decoding a protocol with an error type exercises errorSchema validate")
    func validateErrorSchema() throws {
        // .errorSchema validate (AvroSchema.swift line 369-371) runs while the
        // protocol decoder normalises a top-level error type.
        let protocolJSON = #"""
        {
          "namespace":"test","protocol":"P",
          "types":[
            {"type":"error","name":"E","fields":[{"name":"reason","type":"string"}]}
          ],
          "messages":{}
        }
        """#
        let data = protocolJSON.data(using: .utf8)!
        _ = try JSONDecoder().decode(AvroProtocol.self, from: data)
    }
}

// MARK: - Resolution error paths

@Suite("AvroSchema – resolution error paths")
struct ResolutionErrorPathTests {

    @Test("empty-branch union resolving from itself throws SchemaMismatch")
    func emptyUnionThrows() {
        var r: AvroSchema = .unionSchema(AvroSchema.UnionSchema(branches: []))
        let w: AvroSchema = .unionSchema(AvroSchema.UnionSchema(branches: []))
        #expect(throws: (any Error).self) {
            try r.resolving(from: w)
        }
    }

    @Test("float writer with non-double reader throws SchemaMismatch")
    func floatWriterNonDoubleReader() throws {
        var reader = try parse(#"{"type":"int"}"#)
        let writer: AvroSchema = .floatSchema
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("string writer with non-bytes reader throws SchemaMismatch")
    func stringWriterNonBytesReader() throws {
        var reader = try parse(#"{"type":"int"}"#)
        let writer = try parse(#"{"type":"string"}"#)
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("fixed decimal writer with mismatched bytes reader throws SchemaMismatch")
    func fixedDecimalMismatchThrows() throws {
        var reader = try parse(#"{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}"#)
        let writer = try parse(#"{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":5,"scale":3}"#)
        #expect(throws: (any Error).self) {
            try reader.resolving(from: writer)
        }
    }

    @Test("RecordSchema.resolving appends writer-only fields with .skip resolution")
    func recordResolvingAppendsSkipFields() throws {
        // Calling RecordSchema.resolving directly bypasses AvroSchema's
        // outer == check (which rejects readers with fewer fields than the
        // writer) and exercises the writer-only-field skip path.
        let reader = try parse(#"""
        {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
        """#)
        let writer = try parse(#"""
        {"type":"record","name":"R","fields":[
          {"name":"a","type":"int"},
          {"name":"b","type":"string"}
        ]}
        """#)
        guard case .recordSchema(var r) = reader, case .recordSchema(let w) = writer else {
            Issue.record("expected record schemas")
            return
        }
        try r.resolving(from: w)
        #expect(r.fields.count == 2)
        #expect(r.fields.last?.resolution == .skip)
    }
}
