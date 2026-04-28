//
//  AvroSchemaEquatableTest.swift
//  SwiftAvroCoreTests
//
//  Converted to Swift Testing framework.
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Avro Schema Equatable")
struct AvroSchemaEquatableTests {

    private func assertEqual(_ s1: String, _ s2: String, notEqual s3: String,
                              file: StaticString = #filePath, line: UInt = #line) {
        let a = Avro()
        #expect(a.decodeSchema(schema: s1) == a.decodeSchema(schema: s2))
        #expect(a.decodeSchema(schema: s1) != a.decodeSchema(schema: s3))
    }

    @Test("Null schema equality")
    func null() {
        assertEqual(#"{"type":"null"}"#, #"{"type":"null"}"#,
                    notEqual: #"{"type":"int"}"#)
    }

    @Test("Boolean schema equality")
    func boolean() {
        assertEqual(#"{"type":"boolean"}"#, #"{"type":"boolean"}"#,
                    notEqual: #"{"type":"int"}"#)
    }

    @Test("Int schema equality")
    func int() {
        assertEqual(#"{"type":"int"}"#, #"{"type":"int"}"#,
                    notEqual: #"{"type":"long"}"#)
    }

    @Test("Long schema equality")
    func long() {
        assertEqual(#"{"type":"long"}"#, #"{"type":"long"}"#,
                    notEqual: #"{"type":"float"}"#)
    }

    @Test("Float schema equality")
    func float() {
        assertEqual(#"{"type":"float"}"#, #"{"type":"float"}"#,
                    notEqual: #"{"type":"double"}"#)
    }

    @Test("Double schema equality")
    func double() {
        assertEqual(#"{"type":"double"}"#, #"{"type":"double"}"#,
                    notEqual: #"{"type":"string"}"#)
    }

    @Test("String schema equality")
    func string() {
        assertEqual(#"{"type":"string"}"#, #"{"type":"string"}"#,
                    notEqual: #"{"type":"bytes"}"#)
    }

    @Test("Bytes schema equality")
    func bytes() {
        assertEqual(#"{"type":"bytes"}"#, #"{"type":"bytes"}"#,
                    notEqual: #"{"type":"array","items":"bytes"}"#)
    }

    @Test("Array schema equality")
    func array() {
        assertEqual(#"{"type":"array","items":"double"}"#,
                    #"{"type":"array","items":"double"}"#,
                    notEqual: #"{"type":"array","items":"float"}"#)
    }

    @Test("Map schema equality")
    func map() {
        assertEqual(#"{"type":"map","values":"string"}"#,
                    #"{"type":"map","values":"string"}"#,
                    notEqual: #"{"type":"map","values":"boolean"}"#)
    }

    @Test("Enum schema equality and inequality")
    func enumSchema() {
        let a     = Avro()
        let same1 = #"{"type":"enum","name":"same1","symbols":["a","b"]}"#
        let same2 = #"{"type":"enum","name":"same1","symbols":["a"]}"#
        let diff1 = #"{"type":"enum","name":"same1","symbols":["a","b","c"]}"#
        let diff2 = #"{"type":"enum","name":"diff","symbols":["a","b"]}"#
        #expect(a.decodeSchema(schema: same1) == a.decodeSchema(schema: same2))
        #expect(a.decodeSchema(schema: same1) != a.decodeSchema(schema: diff1))
        #expect(a.decodeSchema(schema: same1) != a.decodeSchema(schema: diff2))
    }

    @Test("Fixed schema equality and inequality")
    func fixed() {
        let a     = Avro()
        let same  = #"{"type":"fixed","name":"barcode","size":16}"#
        let diff1 = #"{"type":"fixed","name":"barcode","size":15}"#
        let diff2 = #"{"type":"fixed","name":"barcode2","size":16}"#
        #expect(a.decodeSchema(schema: same)  == a.decodeSchema(schema: same))
        #expect(a.decodeSchema(schema: same)  != a.decodeSchema(schema: diff1))
        #expect(a.decodeSchema(schema: same)  != a.decodeSchema(schema: diff2))
    }

    @Test("Union schema equality")
    func union() {
        assertEqual(#"["double","int","long","float"]"#,
                    #"["double","int","long","float"]"#,
                    notEqual: #"["double","float","int","long"]"#)
    }

    @Test("Record schema inequality across different structural variants")
    func record() {
        let a     = Avro()
        let diff1 = #"{"type":"record","name":"Test","fields":[{"name":"f","type":"long"}]}"#
        let diff2 = #"{"type":"error","name":"Test","fields":[{"name":"f","type":"long"}]}"#
        let diff3 = #"{"type":"record","name":"Node","fields":[{"name":"f","type":"string"}]}"#
        let diff4 = #"{"type":"record","name":"Node","fields":[{"name":"label","type":"string"}]}"#
        #expect(a.decodeSchema(schema: diff1) != a.decodeSchema(schema: diff2))
        #expect(a.decodeSchema(schema: diff1) != a.decodeSchema(schema: diff3))
        #expect(a.decodeSchema(schema: diff1) != a.decodeSchema(schema: diff4))
    }

    @Test("Error schema equality")
    func errorSchema() {
        let a = Avro()
        let same1 = #"{"type":"error","name":"MyError","fields":[]}"#
        let same2 = #"{"type":"error","name":"MyError","fields":[]}"#
        let diff = #"{"type":"error","name":"OtherError","fields":[]}"#
        #expect(a.decodeSchema(schema: same1) == a.decodeSchema(schema: same2))
        #expect(a.decodeSchema(schema: same1) != a.decodeSchema(schema: diff))
    }

    @Test("Field schema equality")
    func fieldSchema() throws {
        let s = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}"#))
        let fields = s.getRecordInnerTypes()
        #expect(fields.count == 1)
    }

    @Test("Unknown schema is unknown type")
    func unknownSchema() {
        let schema = AvroSchema()
        #expect(schema.isUnknown())
    }

    // MARK: - Hashable Tests

    @Test("Hashable produces consistent hash for same schema")
    func hashable() throws {
        let s1 = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        let s2 = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[]}"#))
        var h1 = Hasher()
        var h2 = Hasher()
        s1.hash(into: &h1)
        s2.hash(into: &h2)
        #expect(h1.finalize() == h2.finalize())
    }

    @Test("Hashable different schemas produce different hashes")
    func hashableDifferent() throws {
        let s1 = try #require(Avro().decodeSchema(schema: #"{"type":"int"}"#))
        let s2 = try #require(Avro().decodeSchema(schema: #"{"type":"string"}"#))
        var h1 = Hasher()
        var h2 = Hasher()
        s1.hash(into: &h1)
        s2.hash(into: &h2)
        #expect(h1.finalize() != h2.finalize())
    }
}
