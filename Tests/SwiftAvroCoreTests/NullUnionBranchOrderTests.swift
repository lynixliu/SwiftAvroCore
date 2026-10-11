//
//  NullUnionBranchOrderTests.swift
//  SwiftAvroCoreTests
//

import Testing
import Foundation
@testable import SwiftAvroCore

@Suite("Null union branch order")
struct NullUnionBranchOrderTests {

    private struct ReferralParams: Codable {
        let affiliateKey: String?
        let from: String?
        let refType: String?
    }

    private struct AppOpen: Codable {
        let url: String
        let referralParams: ReferralParams
    }

    private struct Inner: Codable {
        let a: String?
        let b: String?
    }

    private struct Middle: Codable {
        let inner: Inner
        let c: String?
    }

    private struct Outer: Codable {
        let middle: Middle
        let d: String
    }

    private struct ExplicitNilParams: Codable {
        let affiliateKey: String?
        let from: String

        private enum CodingKeys: String, CodingKey {
            case affiliateKey, from
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeNil(forKey: .affiliateKey)
            try container.encode(from, forKey: .from)
        }
    }

    private struct Reordered: Codable {
        let a: String?
        let b: String

        private enum CodingKeys: String, CodingKey {
            case a, b
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(b, forKey: .b)
            try container.encodeIfPresent(a, forKey: .a)
        }
    }

    private struct MixedUnionEvent: Codable {
        let value: Int64?
    }

    private struct MapUnionEvent: Codable {
        let props: [String: Int64]
    }

    private struct CustomEncodedParams: Codable {
        let affiliateKey: String?
        let from: String?

        private enum CodingKeys: String, CodingKey {
            case affiliateKey, from
        }

        init(affiliateKey: String?, from: String?) {
            self.affiliateKey = affiliateKey
            self.from = from
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            affiliateKey = try container.decodeIfPresent(String.self, forKey: .affiliateKey)
            from = try container.decodeIfPresent(String.self, forKey: .from)
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode("forced", forKey: .affiliateKey)
            try container.encodeIfPresent(from, forKey: .from)
        }
    }

    private struct CustomEncodedAppOpen: Codable {
        let url: String
        let referralParams: CustomEncodedParams
    }

    private let appOpenSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"AppOpen","fields":[
          {"name":"url","type":"string"},
          {"name":"referralParams","type":{"type":"record","name":"ReferralParams","fields":[
            {"name":"affiliateKey","type":["string","null"]},
            {"name":"from","type":["string","null"]},
            {"name":"refType","type":["string","null"]}
          ]}}
        ]}
        """)!

    private let mixedUnionSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"MixedUnionEvent","fields":[
          {"name":"value","type":["null","string","long"]}
        ]}
        """)!

    private let mapUnionSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"MapUnionEvent","fields":[
          {"name":"props","type":{"type":"map","values":["null","string","long"]}}
        ]}
        """)!

    private let customEncodedSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"CustomEncodedAppOpen","fields":[
          {"name":"url","type":"string"},
          {"name":"referralParams","type":{"type":"record","name":"CustomEncodedParams","fields":[
            {"name":"affiliateKey","type":["string","null"]},
            {"name":"from","type":["string","null"]}
          ]}}
        ]}
        """)!

    private let nestedSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"Outer","fields":[
          {"name":"middle","type":{"type":"record","name":"Middle","fields":[
            {"name":"inner","type":{"type":"record","name":"Inner","fields":[
              {"name":"a","type":["string","null"]},
              {"name":"b","type":["string","null"]}
            ]}},
            {"name":"c","type":["string","null"]}
          ]}},
          {"name":"d","type":"string"}
        ]}
        """)!

    private let explicitNilSchema: AvroSchema = Avro().decodeSchema(schema: """
        {"type":"record","name":"ExplicitNilParams","fields":[
          {"name":"affiliateKey","type":["string","null"]},
          {"name":"from","type":["string","null"]}
        ]}
        """)!

    private let url = "https://www.example.com"

    @Test("All-nil nested record encodes a null branch index for every field")
    func allNilNestedRecordEncodesNullBranchIndices() throws {
        let model = AppOpen(
            url: url,
            referralParams: ReferralParams(affiliateKey: nil, from: nil, refType: nil)
        )

        let data = try Avro().encodeFrom(model, schema: appOpenSchema)

        #expect(data.count == 27)
        #expect([UInt8](data.prefix(1)) == [0x2e])
        #expect([UInt8](data.suffix(3)) == [0x02, 0x02, 0x02])
    }

    @Test("All-nil nested record survives a round trip")
    func allNilNestedRecordRoundTrips() throws {
        let avro = Avro()
        let model = AppOpen(
            url: url,
            referralParams: ReferralParams(affiliateKey: nil, from: nil, refType: nil)
        )

        let data = try avro.encodeFrom(model, schema: appOpenSchema)
        let decoded: AppOpen = try avro.decodeFrom(from: data, schema: appOpenSchema)

        #expect(decoded.url == url)
        #expect(decoded.referralParams.affiliateKey == nil)
        #expect(decoded.referralParams.from == nil)
        #expect(decoded.referralParams.refType == nil)
    }

    @Test("All-nil nested record encodes every field as null in Avro JSON")
    func allNilNestedRecordEncodesNullFieldsInJSON() throws {
        let avro = Avro()
        avro.setAvroFormat(option: .AvroJson)
        let model = AppOpen(
            url: url,
            referralParams: ReferralParams(affiliateKey: nil, from: nil, refType: nil)
        )

        let data = try avro.encodeFrom(model, schema: appOpenSchema)
        let json = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        let params = try #require(json["referralParams"] as? [String: Any])

        #expect(Set(params.keys) == ["affiliateKey", "from", "refType"])
        #expect(params.values.allSatisfy { $0 is NSNull })
    }

    @Test("All-nil records nested two deep encode every null index")
    func allNilRecordsNestedTwoDeep() throws {
        let avro = Avro()
        let model = Outer(middle: Middle(inner: Inner(a: nil, b: nil), c: nil), d: "z")

        let data = try avro.encodeFrom(model, schema: nestedSchema)
        let decoded: Outer = try avro.decodeFrom(from: data, schema: nestedSchema)

        #expect([UInt8](data) == [0x02, 0x02, 0x02, 0x02, 0x7a])
        #expect(decoded.d == "z")
    }

    @Test("Explicit encodeNil on a union field encodes its null branch index")
    func explicitEncodeNilOnUnionField() throws {
        let model = ExplicitNilParams(affiliateKey: nil, from: "f")

        let data = try Avro().encodeFrom(model, schema: explicitNilSchema)

        #expect([UInt8](data) == [0x02, 0x00, 0x02, 0x66])
    }

    @Test("A field encoded after a later field throws instead of encoding twice")
    func outOfOrderNullableFieldThrows() throws {
        let schema = try #require(Avro().decodeSchema(schema: """
            {"type":"record","name":"Reordered","fields":[
              {"name":"a","type":["null","string"]},
              {"name":"b","type":"string"}
            ]}
            """))

        #expect(throws: BinaryEncodingError.fieldOutOfOrder) {
            try Avro().encodeFrom(Reordered(a: "q", b: "z"), schema: schema)
        }
    }

    @Test("Nested record with only the first field set encodes trailing null indices")
    func onlyFirstFieldPopulated() throws {
        let model = AppOpen(
            url: url,
            referralParams: ReferralParams(affiliateKey: "k", from: nil, refType: nil)
        )

        let data = try Avro().encodeFrom(model, schema: appOpenSchema)

        #expect(data.count == 29)
        #expect([UInt8](data.suffix(5)) == [0x00, 0x02, 0x6b, 0x02, 0x02])
    }

    @Test("Nested record with only the last field set encodes leading null indices")
    func onlyLastFieldPopulated() throws {
        let model = AppOpen(
            url: url,
            referralParams: ReferralParams(affiliateKey: nil, from: nil, refType: "t")
        )

        let data = try Avro().encodeFrom(model, schema: appOpenSchema)

        #expect(data.count == 29)
        #expect([UInt8](data.suffix(5)) == [0x02, 0x02, 0x00, 0x02, 0x74])
    }

    @Test("Long branch of union{null,string,long} encodes")
    func longBranchOfThreeMemberUnion() throws {
        let model = MixedUnionEvent(value: Int64(1))

        let data = try Avro().encodeFrom(model, schema: mixedUnionSchema)

        #expect([UInt8](data) == [0x04, 0x02])
    }

    @Test("Long branch of union{null,string,long} survives a round trip")
    func longBranchOfThreeMemberUnionRoundTrips() throws {
        let avro = Avro()
        let model = MixedUnionEvent(value: Int64(1))

        let data = try avro.encodeFrom(model, schema: mixedUnionSchema)
        let decoded: MixedUnionEvent = try avro.decodeFrom(from: data, schema: mixedUnionSchema)

        #expect(decoded.value == Int64(1))
    }

    @Test("Long branch inside a map of union{null,string,long} encodes")
    func longBranchInsideMapOfThreeMemberUnion() throws {
        let model = MapUnionEvent(props: ["a": Int64(1)])

        let data = try Avro().encodeFrom(model, schema: mapUnionSchema)

        #expect([UInt8](data) == [0x02, 0x02, 0x61, 0x04, 0x02, 0x00])
    }

    @Test("Custom encode(to:) is honoured when every stored property is nil")
    func customEncoderNotBypassedWhenStoredPropertiesNil() throws {
        let model = CustomEncodedAppOpen(
            url: url,
            referralParams: CustomEncodedParams(affiliateKey: nil, from: nil)
        )

        let data = try Avro().encodeFrom(model, schema: customEncodedSchema)

        #expect([UInt8](data.suffix(9)) == [0x00, 0x0c, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x64, 0x02])
    }
}
