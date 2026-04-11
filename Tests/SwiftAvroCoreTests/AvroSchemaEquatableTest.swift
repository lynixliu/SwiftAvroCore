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
}
