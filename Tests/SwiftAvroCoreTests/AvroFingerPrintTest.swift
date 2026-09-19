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

// MARK: - Rabin fingerprint against the Avro test vectors

@Suite("Avro Fingerprint – Avro test vectors")
struct RabinVectorTests {

    /// Canonical form and its fingerprint, from apache/avro
    /// share/test/data/schema-tests.txt. The values are taken over the bytes of
    /// the text, so they exercise fingerprint64 on its own.
    static let vectors: [(text: String, fingerprint: Int64)] = [
        (#""null""#, 7195948357588979594),
        (#""boolean""#, -6970731678124411036),
        (#""int""#, 8247732601305521295),
        (#""long""#, -3434872931120570953),
        (#""float""#, 5583340709985441680),
        (#""double""#, -8181574048448539266),
        (#""bytes""#, 5746618253357095269),
        (#""string""#, -8142146995180207161),
        ("[]", -1241056759729112623),
        (#"["int"]"#, -5232228896498058493),
        (#"["int","boolean"]"#, 5392556393470105090),
        (#"{"name":"foo","type":"record","fields":[]}"#, -4824392279771201922),
        (#"{"name":"x.y.foo","type":"record","fields":[]}"#, 5916914534497305771),
        (#"{"name":"a.b.foo","type":"record","fields":[]}"#, -4616218487480524110),
        (#"{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}"#, 7843277075252814651),
        (#"{"name":"foo","type":"enum","symbols":["A1"]}"#, -6342190197741309591),
        (#"{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}"#, -4448647247586288245),
        (#"{"name":"foo","type":"fixed","size":15}"#, 1756455273707447556),
        (#"{"name":"x.y.z.foo","type":"fixed","size":32}"#, -3064184465700546786),
        (#"{"type":"array","items":"null"}"#, -589620603366471059),
        (#"{"type":"map","values":"string"}"#, -8732877298790414990),
    ]

    @Test("fingerprint64 matches the Avro test vectors", arguments: vectors)
    func matchesVectors(vector: (text: String, fingerprint: Int64)) {
        #expect(AvroFingerprint.fingerprint64([UInt8](Data(vector.text.utf8))) == vector.fingerprint)
    }

    @Test("A value that turns negative part way through still matches")
    func negativeRunningValue() {
        // The algorithm shifts zeroes in from the left. An arithmetic shift
        // carries the sign bit down instead, which goes wrong as soon as the
        // running value has its top bit set, as it does for most inputs.
        #expect(AvroFingerprint.fingerprint64([UInt8](Data(#""boolean""#.utf8))) == -6970731678124411036)
        #expect(AvroFingerprint.fingerprint64([UInt8](Data(#""double""#.utf8))) == -8181574048448539266)
    }
}
