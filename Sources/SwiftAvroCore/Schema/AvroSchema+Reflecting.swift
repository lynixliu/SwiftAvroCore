import Foundation

extension AvroSchema.RecordSchema {

    /// Builds a `RecordSchema` by reflecting the children of `mirror`.
    /// Each child whose value can be mapped to an `AvroSchema` becomes a field.
    init(reflecting mirror: Mirror, name: String?) {
        self.name   = name ?? String(describing: mirror.subjectType)
        self.type   = AvroSchema.Types.record.rawValue
        self.fields = []
        self.doc    = nil

        for child in mirror.children {
            guard let fieldName = child.label else { continue }
            guard let fieldSchema = AvroSchema.reflecting(child.value, name: fieldName) else { continue }
            fields.append(
                AvroSchema.FieldSchema(
                    name: fieldName, type: fieldSchema,
                    doc: nil, order: nil, aliases: nil,
                    defaultValue: nil, optional: nil
                )
            )
        }
    }
}

// MARK: - AvroSchema reflection

extension AvroSchema {

    // MARK: Swift type → Avro primitive type name

    /// Maps a Swift metatype to its Avro primitive type string, or `nil` for
    /// complex / unknown types.
    static func avroType(for swiftType: Any.Type) -> String? {
        switch ObjectIdentifier(swiftType) {
        case ObjectIdentifier(Int.self),
             ObjectIdentifier(Int32.self):   return Types.int.rawValue
        case ObjectIdentifier(UInt64.self),
             ObjectIdentifier(Int64.self):   return Types.long.rawValue
        case ObjectIdentifier(String.self),
             ObjectIdentifier(NSString.self): return Types.string.rawValue
        case ObjectIdentifier(Double.self):  return Types.double.rawValue
        case ObjectIdentifier(Float.self):   return Types.float.rawValue
        case ObjectIdentifier(Bool.self):    return Types.boolean.rawValue
        case ObjectIdentifier(Date.self):    return Types.int.rawValue   // logical date
        default:
            // [UInt8] has no stable ObjectIdentifier, match by description.
            if String(describing: swiftType) == "Array<UInt8>" {
                return Types.bytes.rawValue
            }
            return nil
        }
    }

    // MARK: Public entry point

    /// Reflects `subject` and returns the best-matching `AvroSchema`, or `nil`
    /// if the type cannot be represented.
    public static func reflecting(_ subject: Any, name: String? = nil) -> AvroSchema? {
        let mirror = Mirror(reflecting: subject)

        // Unwrap Optional before inspecting display style.
        if mirror.displayStyle == .optional {
            return reflectingOptional(mirror, name: name)
        }

        // Primitive types are identified by metatype, not display style.
        if let schema = reflectingPrimitive(type: mirror.subjectType) {
            return schema
        }

        switch mirror.displayStyle {
        case .struct, .class:
            return reflectingRecord(mirror, name: name)
        case .enum:
            return reflectingEnum(mirror, name: name)
        case .collection, .set:
            return reflectingArray(mirror, name: name)
        case .dictionary:
            return reflectingMap(mirror, name: name)
        case .tuple:
            return reflectingMap(mirror, name: name)
        case .optional, .none:
            return reflectingPrimitive(type: mirror.subjectType)
        default:
            return nil
        }
    }

    // MARK: Private helpers

    private static func reflectingPrimitive(type: Any.Type) -> AvroSchema? {
        if type == Date.self {
            return .intSchema(IntSchema(type: Types.int.rawValue, logicalType: .date))
        }
        guard let avroTypeName = avroType(for: type) else { return nil }
        return AvroSchema(type: avroTypeName)
    }

    private static func reflectingRecord(_ mirror: Mirror, name: String?) -> AvroSchema? {
        .recordSchema(RecordSchema(reflecting: mirror, name: name))
    }

    /// Derives enum case names using `Mirror.children` on a synthesised
    /// `CasesWrapper` — works for any Swift enum without external dependencies.
    ///
    /// For enums with associated values `Mirror` only sees the current case, so
    /// we fall back to the subject-type description as the sole symbol. Enums
    /// that conform to `CaseIterable` give us all cases reliably.
    private static func reflectingEnum(_ mirror: Mirror, name: String?) -> AvroSchema? {
        let typeName = String(describing: mirror.subjectType)

        // Best path: CaseIterable gives all case names without Runtime.
        let caseNames: [String]
        if let iterable = mirror.subjectType as? any CaseIterable.Type {
            caseNames = iterable.allCases.map { "\(Mirror(reflecting: $0).children.first?.label ?? "\($0)")" }
        } else {
            // Fallback: reflect only the current value — one symbol.
            let label = mirror.children.first?.label ?? "\(mirror.subjectType)"
            caseNames = [label]
        }

        let enumSchema = EnumSchema(
            name: typeName,
            type: Types.enums.rawValue,
            doc: nil,
            symbols: caseNames
        )
        return .enumSchema(enumSchema)
    }

    /// Reflects an array or set.
    ///
    /// `Mirror` for a collection exposes its *elements* as children, so we
    /// reflect the first element to discover the item schema. Empty collections
    /// cannot be reflected (no elements to inspect), so they return `nil`.
    private static func reflectingArray(_ mirror: Mirror, name: String?) -> AvroSchema? {
        guard let firstChild = mirror.children.first else {
            // Empty collection: item type is unknowable via Mirror alone.
            return nil
        }
        guard let itemSchema = reflecting(firstChild.value) else { return nil }
        return .arraySchema(ArraySchema(type: Types.array.rawValue, items: itemSchema))
    }

    /// Map reflection is not yet implemented — maps require key/value type
    /// information that cannot be reliably extracted from `Mirror` alone
    /// without `Runtime` or a protocol constraint.
    private static func reflectingMap(_ mirror: Mirror, name: String?) -> AvroSchema? {
        nil
    }

    /// Handles `Optional<T>`: if the value is `.some(wrapped)` reflect the
    /// wrapped value; if `.none` we cannot determine the inner type from Mirror
    /// alone, so return `.nullSchema`.
    private static func reflectingOptional(_ mirror: Mirror, name: String?) -> AvroSchema? {
        if let (_, wrapped) = mirror.children.first {
            // .some(wrapped) — reflect the inner value and wrap in a union.
            if let inner = reflecting(wrapped, name: name) {
                return .unionSchema(UnionSchema(branches: [.nullSchema, inner]))
            }
        }
        // .none — type of the inner value is not accessible via Mirror.
        return .nullSchema
    }
}
