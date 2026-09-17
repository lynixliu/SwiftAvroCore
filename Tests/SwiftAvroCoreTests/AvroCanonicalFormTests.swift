import Foundation
import Testing
@testable import SwiftAvroCore

// Vectors from apache/avro share/test/data/schema-tests.txt, which the Java
// implementation cross-checks against a direct fingerprint implementation.

@Suite("Avro Parsing Canonical Form")
struct ParsingCanonicalFormTests {

    struct Vector: Sendable {
        let id: Int
        let input: String
        let canonical: String
        let fingerprint: Int64?
    }

    static let vectors: [Vector] = [
        Vector(id: 0, input: #""null""#, canonical: #""null""#, fingerprint: 7195948357588979594),
        Vector(id: 1, input: #"{"type":"null"}"#, canonical: #""null""#, fingerprint: nil),
        Vector(id: 2, input: #""boolean""#, canonical: #""boolean""#, fingerprint: -6970731678124411036),
        Vector(id: 3, input: #"{"type":"boolean"}"#, canonical: #""boolean""#, fingerprint: nil),
        Vector(id: 4, input: #""int""#, canonical: #""int""#, fingerprint: 8247732601305521295),
        Vector(id: 5, input: #"{"type":"int"}"#, canonical: #""int""#, fingerprint: nil),
        Vector(id: 6, input: #""long""#, canonical: #""long""#, fingerprint: -3434872931120570953),
        Vector(id: 7, input: #"{"type":"long"}"#, canonical: #""long""#, fingerprint: nil),
        Vector(id: 8, input: #""float""#, canonical: #""float""#, fingerprint: 5583340709985441680),
        Vector(id: 9, input: #"{"type":"float"}"#, canonical: #""float""#, fingerprint: nil),
        Vector(id: 10, input: #""double""#, canonical: #""double""#, fingerprint: -8181574048448539266),
        Vector(id: 11, input: #"{"type":"double"}"#, canonical: #""double""#, fingerprint: nil),
        Vector(id: 12, input: #""bytes""#, canonical: #""bytes""#, fingerprint: 5746618253357095269),
        Vector(id: 13, input: #"{"type":"bytes"}"#, canonical: #""bytes""#, fingerprint: nil),
        Vector(id: 14, input: #""string""#, canonical: #""string""#, fingerprint: -8142146995180207161),
        Vector(id: 15, input: #"{"type":"string"}"#, canonical: #""string""#, fingerprint: nil),
        Vector(id: 16, input: #"[  ]"#, canonical: #"[]"#, fingerprint: -1241056759729112623),
        Vector(id: 17, input: #"[ "int"  ]"#, canonical: #"["int"]"#, fingerprint: -5232228896498058493),
        Vector(id: 18, input: #"[ "int" , {"type":"boolean"} ]"#, canonical: #"["int","boolean"]"#, fingerprint: 5392556393470105090),
        Vector(id: 19, input: #"{"fields":[], "type":"record", "name":"foo"}"#, canonical: #"{"name":"foo","type":"record","fields":[]}"#, fingerprint: -4824392279771201922),
        Vector(id: 20, input: #"{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}"#, canonical: #"{"name":"x.y.foo","type":"record","fields":[]}"#, fingerprint: 5916914534497305771),
        Vector(id: 21, input: #"{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}"#, canonical: #"{"name":"a.b.foo","type":"record","fields":[]}"#, fingerprint: -4616218487480524110),
        Vector(id: 22, input: #"{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}"#, canonical: #"{"name":"foo","type":"record","fields":[]}"#, fingerprint: -4824392279771201922),
        Vector(id: 23, input: #"{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}"#, canonical: #"{"name":"foo","type":"record","fields":[]}"#, fingerprint: -4824392279771201922),
        Vector(id: 24, input: #"{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}"#, canonical: #"{"name":"foo","type":"record","fields":[]}"#, fingerprint: -4824392279771201922),
        Vector(id: 25, input: #"{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}"#, canonical: #"{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}"#, fingerprint: 7843277075252814651),
        Vector(id: 26, input: #"{ "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true}, {"order":"descending","name":"f2","doc":"Hello","type":"int"}], "type":"record", "name":"foo" }"#, canonical: #"{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"},{"name":"f2","type":"int"}]}"#, fingerprint: -4860222112080293046),
        Vector(id: 27, input: #"{"type":"enum", "name":"foo", "symbols":["A1"]}"#, canonical: #"{"name":"foo","type":"enum","symbols":["A1"]}"#, fingerprint: -6342190197741309591),
        Vector(id: 28, input: #"{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}"#, canonical: #"{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}"#, fingerprint: -4448647247586288245),
        Vector(id: 29, input: #"{"name":"foo","type":"fixed","size":15}"#, canonical: #"{"name":"foo","type":"fixed","size":15}"#, fingerprint: 1756455273707447556),
        Vector(id: 30, input: #"{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}"#, canonical: #"{"name":"x.y.z.foo","type":"fixed","size":32}"#, fingerprint: -3064184465700546786),
        Vector(id: 31, input: #"{ "items":{"type":"null"}, "type":"array"}"#, canonical: #"{"type":"array","items":"null"}"#, fingerprint: -589620603366471059),
        Vector(id: 32, input: #"{ "values":"string", "type":"map"}"#, canonical: #"{"type":"map","values":"string"}"#, fingerprint: -8732877298790414990),
        Vector(id: 33, input: #"{"name":"PigValue","type":"record", "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}"#, canonical: #"{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}"#, fingerprint: -1759257747318642341),
    ]

    @Test("Canonical form matches the Avro test vectors", arguments: vectors)
    func canonicalMatches(vector: Vector) throws {
        let schema = try #require(Avro().decodeSchema(schema: vector.input), "case \(vector.id) did not parse")
        #expect(schema.parsingCanonicalForm() == vector.canonical, "case \(vector.id)")
    }

    @Test("Fingerprint matches the Avro test vectors", arguments: vectors.filter { $0.fingerprint != nil })
    func fingerprintMatches(vector: Vector) throws {
        let schema = try #require(Avro().decodeSchema(schema: vector.input), "case \(vector.id) did not parse")
        #expect(schema.fingerprint() == vector.fingerprint, "case \(vector.id)")
    }

    @Test("Canonical form is stable across calls")
    func stableAcrossCalls() throws {
        let text = #"{"type":"record","name":"Outer","namespace":"com.ex","fields":[{"name":"v","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}},{"name":"y","type":"int"}]}"#
        let schema = try #require(Avro().decodeSchema(schema: text))
        let forms = (0..<20).map { _ in schema.parsingCanonicalForm() }
        #expect(Set(forms).count == 1)
        let prints = (0..<20).map { _ in schema.fingerprint() }
        #expect(Set(prints).count == 1)
    }

    @Test("An inherited namespace fingerprints like a written-out fullname")
    func inheritedMatchesExplicit() throws {
        let inherited = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"Outer","namespace":"com.ex","fields":[{"name":"v","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}}]}"#))
        let explicit = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"com.ex.Outer","fields":[{"name":"v","type":{"type":"record","name":"com.ex.Inner","fields":[{"name":"x","type":"string"}]}}]}"#))

        #expect(inherited.parsingCanonicalForm() == explicit.parsingCanonicalForm())
        #expect(inherited.fingerprint() == explicit.fingerprint())
    }

    @Test("A doc or an alias does not change the fingerprint")
    func documentationIgnored() throws {
        let plain = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#))
        let documented = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","doc":"hello","aliases":["Old"],"fields":[{"name":"x","type":"int","doc":"a field","order":"ascending"}]}"#))
        #expect(plain.fingerprint() == documented.fingerprint())
    }

    @Test("Different schemas fingerprint differently")
    func distinctSchemas() throws {
        let a = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}"#))
        let b = try #require(Avro().decodeSchema(schema: #"{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}"#))
        #expect(a.fingerprint() != b.fingerprint())
    }
}
