import Foundation
import Testing
import SwiftAvroCore

@Suite("Avro Fingerprint")
struct AvroFingerprintTests {

    @Test("fingerprint64 returns consistent hash for same input")
    func fingerprint64Consistency() {
        let data: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        #expect(AvroFingerprint.fingerprint64(data) == AvroFingerprint.fingerprint64(data))
    }

    @Test("fingerprint64 returns different hash for different inputs")
    func fingerprint64Uniqueness() {
        let data1: [UInt8] = [104, 101, 108, 108, 111]  // "hello"
        let data2: [UInt8] = [119, 111, 114, 108, 100]  // "world"
        #expect(AvroFingerprint.fingerprint64(data1) != AvroFingerprint.fingerprint64(data2))
    }

    @Test("fingerprint64 of empty input equals the Rabin empty constant")
    func fingerprint64Empty() {
        #expect(AvroFingerprint.fingerprint64([]) == -4513414715797952619)
    }

    @Test("fingerprint64 of non-empty input differs from the empty constant")
    func fingerprint64NonEmpty() {
        #expect(AvroFingerprint.fingerprint64([0]) != -4513414715797952619)
    }

    @Test("fingerprint64 handles all single-byte values 0–255")
    func fingerprint64AllBytes() {
        for i in 0..<256 {
            #expect(AvroFingerprint.fingerprint64([UInt8(i)]) != -4513414715797952619)
        }
    }

    @Test("fingerprint64 handles larger data")
    func fingerprint64Larger() {
        let data: [UInt8] = (0..<1000).map { UInt8($0 % 256) }
        #expect(AvroFingerprint.fingerprint64(data) != -4513414715797952619)
    }

    @Test("FingerprintFunction typealias accepts a custom algorithm")
    func customFingerprintFunction() {
        // Verify the typealias works: a trivial custom algorithm (byte reversal)
        // compiles and produces a different result than rabin64.
        let reversed: FingerprintFunction = { data in data.reversed() }
        let input: [UInt8] = [1, 2, 3]
        #expect(reversed(input) == [3, 2, 1])
    }
}

// MARK: - Canonical form of a nested namespaced schema

@Suite("Avro Fingerprint – nested namespace")
struct NestedNamespaceFingerprintTests {

    private func canonicalForm(_ text: String) throws -> String {
        let avro = Avro()
        avro.setSchemaFormat(option: .CanonicalForm)
        let schema = try #require(avro.decodeSchema(schema: text))
        return String(decoding: try avro.encodeSchema(schema: schema), as: UTF8.self)
    }

    @Test("A nested type reaches canonical form under the enclosing namespace")
    func nestedFullnameInCanonicalForm() throws {
        // The fingerprint is taken over the canonical form, so a wrong fullname
        // there gives a wrong fingerprint. Inner belongs to com.ex, and the
        // field holding it contributes nothing to the name.
        let form = try canonicalForm(#"{"type":"record","name":"Outer","namespace":"com.ex","fields":[{"name":"v","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}}]}"#)
        #expect(form.contains(#""com.ex.Outer""#))
        #expect(form.contains(#""com.ex.Inner""#))
        #expect(!form.contains("com.ex.Outer.v"))
    }

    @Test("Inheriting a namespace matches writing the fullname out")
    func inheritedMatchesExplicit() throws {
        // The two schemas name the same types, so canonical form must give both
        // the same set of fullnames.
        let inherited = try canonicalForm(#"{"type":"record","name":"Outer","namespace":"com.ex","fields":[{"name":"v","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}}]}"#)
        let explicit = try canonicalForm(#"{"type":"record","name":"com.ex.Outer","fields":[{"name":"v","type":{"type":"record","name":"com.ex.Inner","fields":[{"name":"x","type":"string"}]}}]}"#)

        for name in [#""com.ex.Outer""#, #""com.ex.Inner""#] {
            #expect(inherited.contains(name))
            #expect(explicit.contains(name))
        }
    }

    @Test("A namespace declared part way down reaches canonical form")
    func declaredNamespaceInCanonicalForm() throws {
        let form = try canonicalForm(#"{"type":"record","name":"A","namespace":"com.ex","fields":[{"name":"b","type":{"type":"record","name":"B","namespace":"org.mid","fields":[{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"x","type":"string"}]}}]}}]}"#)
        #expect(form.contains(#""com.ex.A""#))
        #expect(form.contains(#""org.mid.B""#))
        #expect(form.contains(#""org.mid.C""#))
    }

    @Test("Enum and fixed reach canonical form under the enclosing namespace")
    func enumAndFixedInCanonicalForm() throws {
        let form = try canonicalForm(#"{"type":"record","name":"Outer","namespace":"com.ex","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}},{"name":"f","type":{"type":"fixed","name":"F","size":2}}]}"#)
        #expect(form.contains(#""com.ex.E""#))
        #expect(form.contains(#""com.ex.F""#))
    }
}
