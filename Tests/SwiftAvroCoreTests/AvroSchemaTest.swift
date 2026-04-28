import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Schema")
struct AvroSchemaTests {

    // MARK: - Default Init

    @Test("default init creates unknown schema")
    func defaultInit() {
        let schema = AvroSchema()
        #expect(schema.isUnknown())
    }

    // MARK: - Type Predicates

    @Test("isNull returns true for nullSchema")
    func isNull() {
        let schema = AvroSchema(type: "null")
        #expect(schema.isNull())
        #expect(!schema.isBoolean())
    }

    @Test("isBoolean returns true for booleanSchema")
    func isBoolean() {
        let schema = AvroSchema(type: "boolean")
        #expect(schema.isBoolean())
    }

    @Test("isInt returns true for intSchema")
    func isInt() {
        let schema = AvroSchema(type: "int")
        #expect(schema.isInt())
    }

    @Test("isLong returns true for longSchema")
    func isLong() {
        let schema = AvroSchema(type: "long")
        #expect(schema.isLong())
    }

    @Test("isFloat returns true for floatSchema")
    func isFloat() {
        let schema = AvroSchema(type: "float")
        #expect(schema.isFloat())
    }

    @Test("isDouble returns true for doubleSchema")
    func isDouble() {
        let schema = AvroSchema(type: "double")
        #expect(schema.isDouble())
    }

    @Test("isString returns true for stringSchema")
    func isString() {
        let schema = AvroSchema(type: "string")
        #expect(schema.isString())
    }

    @Test("isBytes returns true for bytesSchema")
    func isBytes() {
        let schema = AvroSchema(type: "bytes")
        #expect(schema.isBytes())
    }

    @Test("isInteger returns true for int and long")
    func isInteger() {
        let intSchema = AvroSchema(type: "int")
        let longSchema = AvroSchema(type: "long")
        let stringSchema = AvroSchema(type: "string")
        #expect(intSchema.isInteger())
        #expect(longSchema.isInteger())
        #expect(!stringSchema.isInteger())
    }

    @Test("isByte returns true for bytes and fixed")
    func isByte() {
        let bytesSchema = AvroSchema(type: "bytes")
        let fixedSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"fixed","name":"F","size":16}"#))
        #expect(bytesSchema.isByte())
        #expect(fixedSchema.isByte())
    }

    @Test("isNamed returns true for record, enum, fixed")
    func isNamed() {
        let stringSchema = AvroSchema(type: "string")
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        let enumSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"enum","name":"E","symbols":["A"]}"#))
        let fixedSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"fixed","name":"F","size":16}"#))
        #expect(!stringSchema.isNamed())
        #expect(recordSchema.isNamed())
        #expect(enumSchema.isNamed())
        #expect(fixedSchema.isNamed())
    }

    @Test("isContainer returns true for complex types")
    func isContainer() {
        let arraySchema = try! #require(Avro().decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let mapSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        let stringSchema = AvroSchema(type: "string")
        #expect(arraySchema.isContainer())
        #expect(mapSchema.isContainer())
        #expect(recordSchema.isContainer())
        #expect(!stringSchema.isContainer())
    }

    @Test("isRecord returns true for recordSchema")
    func isRecord() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        #expect(recordSchema.isRecord())
        #expect(!recordSchema.isArray())
    }

    @Test("isArray returns true for arraySchema")
    func isArray() {
        let arraySchema = try! #require(Avro().decodeSchema(schema: #"{"type":"array","items":"int"}"#))
        #expect(arraySchema.isArray())
        #expect(!arraySchema.isMap())
    }

    @Test("isMap returns true for mapSchema")
    func isMap() {
        let mapSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        #expect(mapSchema.isMap())
        #expect(!mapSchema.isArray())
    }

    @Test("isEnum returns true for enumSchema")
    func isEnum() {
        let enumSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"enum","name":"E","symbols":["A"]}"#))
        #expect(enumSchema.isEnum())
        #expect(!enumSchema.isRecord())
    }

    @Test("isFixed returns true for fixedSchema")
    func isFixed() {
        let fixedSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"fixed","name":"F","size":16}"#))
        #expect(fixedSchema.isFixed())
        #expect(!fixedSchema.isArray())
    }

    @Test("isUnion returns true for unionSchema")
    func isUnion() {
        let unionSchema = try! #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        #expect(unionSchema.isUnion())
        #expect(!unionSchema.isRecord())
    }

    @Test("isField returns true for fieldSchema")
    func isField() {
        let fieldSchema = AvroSchema.fieldSchema(AvroSchema.FieldSchema(
            name: "f", type: AvroSchema(type: "int"),
            doc: nil, order: nil, aliases: nil,
            defaultValue: nil, optional: nil
        ))
        #expect(fieldSchema.isField())
    }

    @Test("isUnknown returns true for unknownSchema")
    func isUnknown() {
        let schema = AvroSchema(type: "unknown")
        #expect(schema.isUnknown())
        #expect(!schema.isRecord())
    }

    // MARK: - Name Accessors

    @Test("getName returns correct names for primitive types")
    func getName() {
        #expect(AvroSchema(type: "null").getName() == "null")
        #expect(AvroSchema(type: "boolean").getName() == "boolean")
        #expect(AvroSchema(type: "int").getName() == "int")
        #expect(AvroSchema(type: "long").getName() == "long")
        #expect(AvroSchema(type: "float").getName() == "float")
        #expect(AvroSchema(type: "double").getName() == "double")
        #expect(AvroSchema(type: "string").getName() == "string")
        #expect(AvroSchema(type: "bytes").getName() == "bytes")
    }

    @Test("getName returns correct name for record")
    func getNameRecord() {
        let schema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Person","fields":[]}"#))
        #expect(schema.getName() == "Person")
    }

    @Test("getName returns correct name for enum")
    func getNameEnum() {
        let schema = try! #require(Avro().decodeSchema(schema: #"{"type":"enum","name":"Color","symbols":["RED"]}"#))
        #expect(schema.getName() == "Color")
    }

    @Test("getName returns correct name for fixed")
    func getNameFixed() {
        let schema = try! #require(Avro().decodeSchema(schema: #"{"type":"fixed","name":"Md5","size":16}"#))
        #expect(schema.getName() == "Md5")
    }

    @Test("getName returns union for unionSchema")
    func getNameUnion() {
        let schema = try! #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        #expect(schema.getName() == "union")
    }

    @Test("getName returns field for fieldSchema")
    func getNameField() {
        let fieldSchema = AvroSchema.fieldSchema(AvroSchema.FieldSchema(
            name: "myField", type: AvroSchema(type: "int"),
            doc: nil, order: nil, aliases: nil,
            defaultValue: nil, optional: nil
        ))
        #expect(fieldSchema.getName() == "myField")
    }

    @Test("getRecordInnerTypes returns field types")
    func getRecordInnerTypes() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}"#))
        let fields = recordSchema.getRecordInnerTypes()
        #expect(fields.count == 1)
    }

    // MARK: - getTypeName

    @Test("getTypeName returns type string")
    func getTypeName() {
        #expect(AvroSchema(type: "null").getTypeName() == "null")
        #expect(AvroSchema(type: "boolean").getTypeName() == "boolean")
        #expect(AvroSchema(type: "int").getTypeName() == "int")
        #expect(AvroSchema(type: "string").getTypeName() == "string")
    }

    // MARK: - getFullname

    @Test("getFullname returns full name with namespace")
    func getFullname() {
        let schema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Person","namespace":"com.example","fields":[]}"#))
        #expect(schema.getFullname() == "com.example.Person")
    }

    // MARK: - Associated Value Accessors

    @Test("getInt returns IntSchema for int schema")
    func getInt() {
        let schema = AvroSchema(type: "int")
        let intSchema = schema.getInt()
        #expect(intSchema != nil)
    }

    @Test("getLong returns IntSchema for long schema")
    func getLong() {
        let schema = AvroSchema(type: "long")
        let longSchema = schema.getLong()
        #expect(longSchema != nil)
    }

    @Test("getBytes returns BytesSchema for bytes schema")
    func getBytes() {
        let schema = AvroSchema(type: "bytes")
        let bytesSchema = schema.getBytes()
        #expect(bytesSchema != nil)
    }

    @Test("getArrayItems returns schema for array items")
    func getArrayItems() {
        let arraySchema = try! #require(Avro().decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let items = arraySchema.getArrayItems()
        #expect(items?.isString() == true)
    }

    @Test("getMapValues returns schema for map values")
    func getMapValues() {
        let mapSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"map","values":"int"}"#))
        let values = mapSchema.getMapValues()
        #expect(values?.isInt() == true)
    }

    @Test("getUnionList returns branches for union")
    func getUnionList() {
        let unionSchema = try! #require(Avro().decodeSchema(schema: #"["null","string"]"#))
        let branches = unionSchema.getUnionList()
        #expect(branches.count == 2)
    }

    @Test("getFixedSize returns size for fixed")
    func getFixedSize() {
        let fixedSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"fixed","name":"B","size":16}"#))
        #expect(fixedSchema.getFixedSize() == 16)
    }

    @Test("getRecordInnerTypes returns field types")
    func getRecordInnerTypesTest() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}"#))
        let types = recordSchema.getRecordInnerTypes()
        #expect(types.count == 1)
        #expect(types[0].isString())
    }

    @Test("getSerializedSchema returns correct schemas")
    func getSerializedSchema() {
        let arraySchema = try! #require(Avro().decodeSchema(schema: #"{"type":"array","items":"string"}"#))
        let serialized = arraySchema.getSerializedSchema()
        #expect(serialized.count == 1)
    }

    @Test("getEnumSymbols returns symbols for enum")
    func getEnumSymbols() {
        let enumSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"enum","name":"Color","symbols":["RED","GREEN"]}"#))
        let symbols = enumSchema.getEnumSymbols()
        #expect(symbols == ["RED", "GREEN"])
    }

    @Test("getEnumIndex returns index for valid symbol")
    func getEnumIndex() {
        let enumSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"enum","name":"Color","symbols":["RED","GREEN"]}"#))
        #expect(enumSchema.getEnumIndex("GREEN") == 1)
        #expect(enumSchema.getEnumIndex("BLUE") == nil)
    }

    @Test("getField returns field schema for fieldSchema")
    func testGetField() {
        let fieldSchema = AvroSchema.fieldSchema(AvroSchema.FieldSchema(
            name: "myField", type: AvroSchema(type: "int"),
            doc: nil, order: nil, aliases: nil,
            defaultValue: nil, optional: nil
        ))
        let field = fieldSchema.getField()
        #expect(field != nil)
        #expect(field?.name == "myField")
    }

    @Test("getRecord returns record schema for recordSchema")
    func getRecord() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        let record = recordSchema.getRecord()
        #expect(record != nil)
        #expect(record?.name == "R")
    }

    @Test("getMapAttribute returns map schema")
    func getMapAttribute() {
        let mapSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"map","values":"string"}"#))
        let map = mapSchema.getMapAttribute()
        #expect(map != nil)
    }

    // MARK: - findSchema

    @Test("findSchema returns nil for non-matching name")
    func findSchema() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Person","fields":[]}"#))
        let found = recordSchema.findSchema(name: "NonExistent")
        #expect(found == nil)
    }

    // MARK: - Schema Resolution

    @Test("resolvingSameType performs no changes for same schema")
    func resolvingSameType() throws {
        var reader = AvroSchema(type: "int")
        let writer = AvroSchema(type: "int")
        try reader.resolving(from: writer)
    }

    @Test("resolvingIntToLong works")
    func resolvingIntToLong() throws {
        var reader = AvroSchema(type: "long")
        let writer = AvroSchema(type: "int")
        try reader.resolving(from: writer)
    }

    @Test("resolvingIntToFloat works")
    func resolvingIntToFloat() throws {
        var reader = AvroSchema(type: "float")
        let writer = AvroSchema(type: "int")
        try reader.resolving(from: writer)
    }

    @Test("resolvingIntToDouble works")
    func resolvingIntToDouble() throws {
        var reader = AvroSchema(type: "double")
        let writer = AvroSchema(type: "int")
        try reader.resolving(from: writer)
    }

    @Test("resolvingLongToFloat works")
    func resolvingLongToFloat() throws {
        var reader = AvroSchema(type: "float")
        let writer = AvroSchema(type: "long")
        try reader.resolving(from: writer)
    }

    @Test("resolvingLongToDouble works")
    func resolvingLongToDouble() throws {
        var reader = AvroSchema(type: "double")
        let writer = AvroSchema(type: "long")
        try reader.resolving(from: writer)
    }

    @Test("resolvingFloatToDouble works")
    func resolvingFloatToDouble() throws {
        var reader = AvroSchema(type: "double")
        let writer = AvroSchema(type: "float")
        try reader.resolving(from: writer)
    }

    @Test("resolvingStringToBytes works")
    func resolvingStringToBytes() throws {
        var reader = AvroSchema(type: "bytes")
        let writer = AvroSchema(type: "string")
        try reader.resolving(from: writer)
    }

    @Test("resolvingBytesToString works")
    func resolvingBytesToString() throws {
        var reader = AvroSchema(type: "string")
        let writer = AvroSchema(type: "bytes")
        try reader.resolving(from: writer)
    }

    @Test("resolving throws on mismatch")
    func resolvingMismatch() throws {
        var reader = AvroSchema(type: "string")
        let writer = AvroSchema(type: "int")
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try reader.resolving(from: writer)
        }
    }

    @Test("resolving union reader accepts writer")
    func resolvingUnionReader() throws {
        var unionReader = try! #require(Avro().decodeSchema(schema: #"["int","string"]"#))
        let stringWriter = AvroSchema(type: "string")
        try unionReader.resolving(from: stringWriter)
    }

    @Test("resolving union writer matches reader")
    func resolvingUnionWriter() throws {
        let unionWriter = try! #require(Avro().decodeSchema(schema: #"["int","string"]"#))
        var intReader = AvroSchema(type: "int")
        try intReader.resolving(from: unionWriter)
    }

    // MARK: - Validate

    @Test("validate passes for valid record")
    func validate() throws {
        var schema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        try schema.validate(typeName: "record", name: "R", nameSpace: nil)
    }

    // MARK: - Decimal

    @Test("isDecimal returns false for bytes without decimal")
    func isDecimal() {
        let bytesSchema = AvroSchema(type: "bytes")
        let intSchema = AvroSchema(type: "int")
        #expect(!bytesSchema.isDecimal())
        #expect(!intSchema.isDecimal())
    }

    // MARK: - Resolution edge cases

    @Test("resolving throws for string to int mismatch")
    func resolvingStringToIntMismatch() throws {
        var reader = AvroSchema(type: "int")
        let writer = AvroSchema(type: "string")
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try reader.resolving(from: writer)
        }
    }

    @Test("resolving throws for long to int mismatch")
    func resolvingLongToIntMismatch() throws {
        var reader = AvroSchema(type: "int")
        let writer = AvroSchema(type: "long")
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try reader.resolving(from: writer)
        }
    }

    @Test("resolving throws for bytes to int mismatch")
    func resolvingBytesToIntMismatch() throws {
        var reader = AvroSchema(type: "int")
        let writer = AvroSchema(type: "bytes")
        #expect(throws: AvroSchemaResolutionError.SchemaMismatch) {
            try reader.resolving(from: writer)
        }
    }

    // MARK: - FieldsSchema

    @Test("getSerializedSchema returns fields for record")
    func fieldsSchema() {
        let recordSchema = try! #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"f1","type":"int"},{"name":"f2","type":"string"}]}"#))
        let serialized = recordSchema.getSerializedSchema()
        #expect(serialized.count == 2)
    }

    // MARK: - Union branch resolution

    @Test("union resolving matches first branch")
    func unionResolvingFirstBranch() throws {
        var union = try! #require(Avro().decodeSchema(schema: #"["int","string"]"#))
        let intWriter = AvroSchema(type: "int")
        try union.resolving(from: intWriter)
    }
}