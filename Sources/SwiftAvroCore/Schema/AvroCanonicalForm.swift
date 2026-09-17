//
//  AvroClient/AvroCanonicalForm.swift
//
//  Copyright © 2026 柳洋 and the project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation

extension AvroSchema {

    /// The Parsing Canonical Form of this schema.
    ///
    /// Two schemas that a reader cannot tell apart share one canonical form, so
    /// this is what a fingerprint is taken over. The spec states the transform
    /// as seven rules, and all seven are applied here:
    ///
    /// - a primitive becomes its bare type name, so `{"type":"int"}` is `"int"`
    /// - a named type is written by its fullname, and `namespace` disappears
    ///   into it
    /// - only `type`, `name`, `fields`, `symbols`, `items`, `values` and `size`
    ///   survive; `doc`, `aliases`, `default`, `order` and a logical type are
    ///   dropped, because a reader does not parse differently for them
    /// - attributes are written in the order name, type, fields, symbols,
    ///   items, values, size
    /// - `size` is a bare integer
    /// - no whitespace sits outside a string
    ///
    /// Building the text directly is what makes the result stable. Going
    /// through JSONEncoder left the attribute order to a dictionary, so the
    /// same schema gave a different string, and a different fingerprint, on
    /// each call.
    public func parsingCanonicalForm() -> String {
        Self.canonicalText(of: self)
    }

    /// The 64-bit Rabin fingerprint of the Parsing Canonical Form.
    ///
    /// Stable across calls, processes and machines, and equal to the value any
    /// other Avro implementation computes for the same schema.
    public func fingerprint() -> Int64 {
        AvroFingerprint.fingerprint64([UInt8](Data(parsingCanonicalForm().utf8)))
    }

    private static func canonicalText(of schema: AvroSchema) -> String {
        switch schema {
        case .nullSchema:    return "\"null\""
        case .booleanSchema: return "\"boolean\""
        case .intSchema:     return "\"int\""
        case .longSchema:    return "\"long\""
        case .floatSchema:   return "\"float\""
        case .doubleSchema:  return "\"double\""
        case .bytesSchema:   return "\"bytes\""
        case .stringSchema:  return "\"string\""

        case .recordSchema(let record):
            return namedRecord(record)

        case .errorSchema(let error):
            // An error is a record to a reader, so it canonicalises as one.
            return namedRecord(error)

        case .enumSchema(let enumSchema):
            let symbols = enumSchema.symbols.map { quoted($0) }.joined(separator: ",")
            return "{\"name\":\(quoted(enumSchema.getFullname())),\"type\":\"enum\",\"symbols\":[\(symbols)]}"

        case .fixedSchema(let fixed):
            return "{\"name\":\(quoted(fixed.getFullname())),\"type\":\"fixed\",\"size\":\(fixed.size)}"

        case .arraySchema(let array):
            return "{\"type\":\"array\",\"items\":\(canonicalText(of: array.items))}"

        case .mapSchema(let map):
            return "{\"type\":\"map\",\"values\":\(canonicalText(of: map.values))}"

        case .unionSchema(let union):
            let branches = union.branches.map { canonicalText(of: $0) }.joined(separator: ",")
            return "[\(branches)]"

        case .fieldSchema(let field):
            return canonicalText(of: field.type)

        case .fieldsSchema(let fields):
            return "[\(fields.map { canonicalField($0) }.joined(separator: ","))]"

        case .unknownSchema(let unknown):
            // A name the parser could not resolve still has to appear, or the
            // form would silently describe a different schema.
            return quoted(unknown.name ?? unknown.type)
        }
    }

    private static func namedRecord(_ record: RecordSchema) -> String {
        let fields = record.fields.map { canonicalField($0) }.joined(separator: ",")
        return "{\"name\":\(quoted(record.getFullname())),\"type\":\"record\",\"fields\":[\(fields)]}"
    }

    private static func canonicalField(_ field: FieldSchema) -> String {
        "{\"name\":\(quoted(field.name)),\"type\":\(canonicalText(of: field.type))}"
    }

    /// A JSON string literal, escaped the way the spec's string rule asks: the
    /// two mandatory escapes, the control characters, and nothing else, so a
    /// name outside ASCII stays as its own UTF-8 bytes.
    private static func quoted(_ text: String) -> String {
        var out = "\""
        for scalar in text.unicodeScalars {
            switch scalar {
            case "\"":  out += "\\\""
            case "\\":  out += "\\\\"
            case "\u{08}": out += "\\b"
            case "\u{0C}": out += "\\f"
            case "\n":  out += "\\n"
            case "\r":  out += "\\r"
            case "\t":  out += "\\t"
            default:
                if scalar.value < 0x20 {
                    out += String(format: "\\u%04x", scalar.value)
                } else {
                    out.unicodeScalars.append(scalar)
                }
            }
        }
        return out + "\""
    }
}
