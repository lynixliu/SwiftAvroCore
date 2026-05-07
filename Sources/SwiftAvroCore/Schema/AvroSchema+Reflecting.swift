// AvroSchema+Reflecting.swift
//
//  Created by Yang Liu on 30/08/18.
//  Copyright © 2018 柳洋 and the project authors.
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

// MARK: - RecordSchema reflection init

extension AvroSchema.RecordSchema {

    /// Builds a `RecordSchema` by reflecting the stored properties of `mirror`.
    init(reflecting mirror: Mirror, name: String?) {
        self.name = name ?? String(describing: mirror.subjectType)
        self.type = AvroSchema.Types.record.rawValue
        self.fields = mirror.children.compactMap { child in
            guard let label = child.label,
                let schema = AvroSchema.reflecting(child.value, name: label)
            else { return nil }
            return AvroSchema.FieldSchema(
                name: label,
                type: schema,
                doc: nil,
                order: nil,
                aliases: nil,
                defaultValue: nil,
                optional: nil
            )
        }
        self.doc = nil
    }
}

// MARK: - AvroSchema reflection

extension AvroSchema {

    // MARK: Swift type → Avro primitive name

    /// Maps a Swift metatype to its Avro primitive type string, or `nil` for
    /// complex / unknown types.
    static func avroType(for swiftType: Any.Type) -> String? {
        switch ObjectIdentifier(swiftType) {
        case ObjectIdentifier(Int.self),
            ObjectIdentifier(Int32.self):
            return Types.int.rawValue
        case ObjectIdentifier(UInt64.self),
            ObjectIdentifier(Int64.self):
            return Types.long.rawValue
        case ObjectIdentifier(String.self),
            ObjectIdentifier(NSString.self):
            return Types.string.rawValue
        case ObjectIdentifier(Double.self): return Types.double.rawValue
        case ObjectIdentifier(Float.self): return Types.float.rawValue
        case ObjectIdentifier(Bool.self): return Types.boolean.rawValue
        case ObjectIdentifier(Date.self): return Types.long.rawValue  // logical timestamp-millis
        default:
            // [UInt8] has no stable ObjectIdentifier; match by description.
            return String(describing: swiftType) == "Array<UInt8>"
                ? Types.bytes.rawValue : nil
        }
    }

    // MARK: Public entry point

    /// Reflects `subject` and returns the best-matching `AvroSchema`, or `nil`
    /// if the type cannot be represented.
    public static func reflecting(_ subject: Any, name: String? = nil)
        -> AvroSchema?
    {
        let mirror = Mirror(reflecting: subject)

        if mirror.displayStyle == .optional {
            return reflectOptional(mirror, name: name)
        }

        if let schema = reflectPrimitive(type: mirror.subjectType) {
            return schema
        }

        switch mirror.displayStyle {
        case .struct, .class: return reflectRecord(mirror, name: name)
        case .enum: return reflectEnum(mirror, name: name)
        case .collection, .set: return reflectArray(mirror)
        case .dictionary, .tuple: return nil  // map reflection is not yet supported
        case .optional, .none: return reflectPrimitive(type: mirror.subjectType)
        default: return nil
        }
    }

    // MARK: Private helpers

    private static func reflectPrimitive(type: Any.Type) -> AvroSchema? {
        if type == Date.self {
            return .longSchema(
                IntSchema(
                    type: Types.long.rawValue,
                    logicalType: .timestampMillis
                )
            )
        }
        guard let name = avroType(for: type) else { return nil }
        return AvroSchema(type: name)
    }

    private static func reflectRecord(_ mirror: Mirror, name: String?)
        -> AvroSchema?
    {
        .recordSchema(RecordSchema(reflecting: mirror, name: name))
    }

    /// Builds an `EnumSchema` from a Swift enum value.
    ///
    /// Uses `CaseIterable` when available for all case names; falls back to
    /// reflecting the current value only.
    private static func reflectEnum(_ mirror: Mirror, name: String?)
        -> AvroSchema?
    {
        let typeName = String(describing: mirror.subjectType)
        let caseNames: [String]
        if let iterable = mirror.subjectType as? any CaseIterable.Type {
            caseNames = iterable.allCases.map {
                Mirror(reflecting: $0).children.first?.label ?? "\($0)"
            }
        } else {
            caseNames = [
                mirror.children.first?.label ?? "\(mirror.subjectType)"
            ]
        }
        return .enumSchema(
            EnumSchema(
                name: typeName,
                type: Types.enums.rawValue,
                doc: nil,
                symbols: caseNames
            )
        )
    }

    /// Reflects an array or set by inspecting the first element.
    /// Returns `nil` for empty collections (item type is unknowable via Mirror).
    private static func reflectArray(_ mirror: Mirror) -> AvroSchema? {
        guard let first = mirror.children.first,
            let itemSchema = reflecting(first.value)
        else { return nil }
        return .arraySchema(
            ArraySchema(type: Types.array.rawValue, items: itemSchema)
        )
    }

    /// Handles `Optional<T>`.
    /// - `.some(wrapped)` → union of `[null, innerSchema]`
    /// - `.none`          → `.nullSchema` (inner type not accessible via Mirror)
    private static func reflectOptional(_ mirror: Mirror, name: String?)
        -> AvroSchema?
    {
        guard let (_, wrapped) = mirror.children.first else {
            return .nullSchema
        }
        guard let inner = reflecting(wrapped, name: name) else { return nil }
        return .unionSchema(UnionSchema(branches: [.nullSchema, inner]))
    }
}
