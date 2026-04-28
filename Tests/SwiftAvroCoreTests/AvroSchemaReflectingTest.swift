import Foundation
import Testing
import SwiftAvroCore
@testable import SwiftAvroCore

@Suite("Avro Schema Reflecting")
struct AvroSchemaReflectingTests {

    // MARK: - avroType

    @Test("avroType returns int for Int")
    func avroTypeInt() {
        #expect(AvroSchema.avroType(for: Int.self) == "int")
        #expect(AvroSchema.avroType(for: Int32.self) == "int")
    }

    @Test("avroType returns long for Int64 and UInt64")
    func avroTypeLong() {
        #expect(AvroSchema.avroType(for: Int64.self) == "long")
        #expect(AvroSchema.avroType(for: UInt64.self) == "long")
    }

    @Test("avroType returns string for String and NSString")
    func avroTypeString() {
        #expect(AvroSchema.avroType(for: String.self) == "string")
        #expect(AvroSchema.avroType(for: NSString.self) == "string")
    }

    @Test("avroType returns double for Double")
    func avroTypeDouble() {
        #expect(AvroSchema.avroType(for: Double.self) == "double")
    }

    @Test("avroType returns float for Float")
    func avroTypeFloat() {
        #expect(AvroSchema.avroType(for: Float.self) == "float")
    }

    @Test("avroType returns boolean for Bool")
    func avroTypeBool() {
        #expect(AvroSchema.avroType(for: Bool.self) == "boolean")
    }

    @Test("avroType returns int for Date (logical date)")
    func avroTypeDate() {
        #expect(AvroSchema.avroType(for: Date.self) == "int")
    }

    @Test("avroType returns bytes for Array<UInt8>")
    func avroTypeBytes() {
        #expect(AvroSchema.avroType(for: [UInt8].self) == "bytes")
    }

    @Test("avroType returns nil for unknown types")
    func avroTypeUnknown() {
        #expect(AvroSchema.avroType(for: [Int].self) == nil)
        #expect(AvroSchema.avroType(for: Data.self) == nil)
    }

    // MARK: - reflecting

    @Test("reflecting returns int schema for Int")
    func reflectingInt() {
        let schema = AvroSchema.reflecting(Int(42))
        #expect(schema?.isInt() == true)
    }

    @Test("reflecting returns int schema for Int32")
    func reflectingInt32() {
        let schema = AvroSchema.reflecting(Int32(42))
        #expect(schema?.isInt() == true)
    }

    @Test("reflecting returns long schema for Int64")
    func reflectingInt64() {
        let schema = AvroSchema.reflecting(Int64(42))
        #expect(schema?.isLong() == true)
    }

    @Test("reflecting returns long schema for UInt64")
    func reflectingUInt64() {
        let schema = AvroSchema.reflecting(UInt64(42))
        #expect(schema?.isLong() == true)
    }

    @Test("reflecting returns string schema for String")
    func reflectingString() {
        let schema = AvroSchema.reflecting("hello")
        #expect(schema?.isString() == true)
    }

    @Test("reflecting returns double schema for Double")
    func reflectingDouble() {
        let schema = AvroSchema.reflecting(Double(3.14))
        #expect(schema?.isDouble() == true)
    }

    @Test("reflecting returns float schema for Float")
    func reflectingFloat() {
        let schema = AvroSchema.reflecting(Float(3.14))
        #expect(schema?.isFloat() == true)
    }

    @Test("reflecting returns boolean schema for Bool")
    func reflectingBool() {
        let schema = AvroSchema.reflecting(true)
        #expect(schema?.isBoolean() == true)
    }

    @Test("reflecting returns date schema for Date")
    func reflectingDate() {
        let schema = AvroSchema.reflecting(Date())
        #expect(schema?.isInt() == true)
    }

    @Test("reflecting returns bytes schema for [UInt8]")
    func reflectingBytes() {
        let schema = AvroSchema.reflecting([UInt8]([1, 2, 3]))
        #expect(schema?.isBytes() == true)
    }

    @Test("reflecting returns null schema for nil optional")
    func reflectingNilOptional() {
        let nilOpt: String? = nil
        let schema = AvroSchema.reflecting(nilOpt)
        #expect(schema?.isNull() == true)
    }

    @Test("reflecting returns union schema for non-nil optional")
    func reflectingNonNilOptional() {
        let opt: String? = "hello"
        let schema = AvroSchema.reflecting(opt)
        #expect(schema?.isUnion() == true)
    }

    @Test("reflecting returns array schema for array")
    func reflectingArray() {
        let schema = AvroSchema.reflecting([1, 2, 3])
        #expect(schema?.isArray() == true)
    }

    @Test("reflecting returns array schema for set")
    func reflectingSet() {
        let schema = AvroSchema.reflecting(Set([1, 2, 3]))
        #expect(schema?.isArray() == true)
    }

    @Test("reflecting returns nil for empty collection")
    func reflectingEmptyCollection() {
        let empty: [Int] = []
        let schema = AvroSchema.reflecting(empty)
        #expect(schema == nil)
    }

    @Test("reflecting returns nil for dictionary")
    func reflectingDictionary() {
        let dict = ["key": "value"]
        let schema = AvroSchema.reflecting(dict)
        #expect(schema == nil)
    }

    @Test("reflecting returns nil for tuple")
    func reflectingTuple() {
        let tuple = (1, "hello")
        let schema = AvroSchema.reflecting(tuple)
        #expect(schema == nil)
    }

    // MARK: - RecordSchema init(reflecting:)

    @Test("RecordSchema init reflectng builds schema from struct")
    func recordSchemaReflecting() {
        struct TestStruct { var name: String = "test"; var value: Int = 42 }
        let mirror = Mirror(reflecting: TestStruct())
        let schema = AvroSchema.RecordSchema(reflecting: mirror, name: "Test")
        #expect(schema.name == "Test")
        #expect(schema.fields.count >= 2)
    }

    @Test("RecordSchema init reflecting handles empty struct")
    func recordSchemaReflectingEmpty() {
        struct Empty {}
        let mirror = Mirror(reflecting: Empty())
        let schema = AvroSchema.RecordSchema(reflecting: mirror, name: nil)
        #expect(schema.name == "Empty")
    }

    // MARK: - Custom name parameter

    @Test("reflecting uses custom name for struct")
    func reflectingWithCustomName() {
        struct Point { var x: Double = 0; var y: Double = 0 }
        let schema = AvroSchema.reflecting(Point(), name: "CustomPoint")
        #expect(schema?.getName() == "CustomPoint")
    }
}