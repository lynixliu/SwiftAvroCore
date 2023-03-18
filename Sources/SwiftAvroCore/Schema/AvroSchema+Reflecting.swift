import Foundation

import Runtime


extension AvroSchema.RecordSchema {
    
    init(reflecting mirror:Mirror, name: String?) {
        self.name = String(describing: mirror.subjectType)
        self.type = AvroSchema.Types.record.rawValue
        fields = []
        doc = nil
        mirror.children.forEach { child in
            guard let fieldName = child.label else { return }
            guard let fieldSchema = AvroSchema.reflecting(child.value, name: fieldName) else {
                return
            }
            fields.append(AvroSchema.FieldSchema(name: fieldName, type: fieldSchema, doc: nil, order: nil, aliases: nil, defaultValue: nil, optional: nil))

        }

    }
}

extension AvroSchema {
    static func avroType(for swiftType: Any.Type) -> String? {
        switch String(describing: swiftType) {
        case "Int", "Int32":
            return Types.int.rawValue
        case "UInt64", "Long":
            return Types.long.rawValue
        case "String", "NSString":
            return Types.string.rawValue
        case "Array<UInt8>":
            return Types.bytes.rawValue
        case "Double":
            return Types.double.rawValue
        case "Float":
            return Types.float.rawValue
        case "Bool":
            return Types.boolean.rawValue
        case "Date":
            return Types.int.rawValue
        default:
            return nil
        }
    }
    
    
    private static func reflectingRecord(_ m: Mirror, name: String?) -> AvroSchema? {
        let record = RecordSchema(reflecting: m, name: name)
        return .recordSchema(record)
    }

    private static func reflectingEnum(_ m: Mirror, name: String?) -> AvroSchema? {
        let metadata = try! typeInfo(of: m.subjectType)
        let caseNames = metadata.cases.map(\.name)
        let enumSchema = EnumSchema(name: String(describing: m.subjectType), type: Types.enums.rawValue, doc: nil, symbols: caseNames)
        return .enumSchema(enumSchema)
    }
    
    private static func reflectingArray(_ m: Mirror, name: String?) -> AvroSchema? {
        let metadata = try? typeInfo(of: m.subjectType)
        guard let itemsType = metadata?.genericTypes.first else { return nil }
        
        guard let itemsSchema = reflectingPrimitive(type: itemsType) else { return nil }
        let arraySchema = ArraySchema(type: Types.array.rawValue, items: itemsSchema)
        return .arraySchema(arraySchema)
    }
    
    
    private static func reflectingMap(_ m: Mirror, name: String?) -> AvroSchema? {
        return nil
    }
    
    private static func reflectingPrimitive(type: Any.Type) -> AvroSchema? {
        if type == Date.self {
            return .intSchema(IntSchema(type: Types.int.rawValue, logicalType: .date))
        } else {
            guard let avroType = avroType(for:type) else {
                return nil
            }
            let schema = AvroSchema(type: avroType)
            return schema
        }
    }


    public static func reflecting(_ subject: Any, name: String? = nil) -> AvroSchema? {
        let m = Mirror(reflecting: subject)
        if let _ = avroType(for: m.subjectType) {
            return Self.reflectingPrimitive(type: m.subjectType)
        }
    
        
        
        switch m.displayStyle {
            case .struct, .class:
            return Self.reflectingRecord(m, name: name)
            case .enum:
            return Self.reflectingEnum(m, name: name)
            case .set, .collection:
            return Self.reflectingArray(m, name: name)
            case .tuple, .dictionary:
            return Self.reflectingMap(m, name: name)
            case .optional, .none:
            return Self.reflectingPrimitive(type: m.subjectType)
            case .some(_):
            return nil
        }
    }
}
