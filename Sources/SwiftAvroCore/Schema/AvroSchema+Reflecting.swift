

extension AvroSchema {
    private static func reflectingRecord(_ m: Mirror) -> AvroSchema? {
        var record = RecordSchema(reflecting: m)

        return .recordSchema(record)
    }

    private static func reflectingEnum(_ m: Mirror) -> AvroSchema? {
        return nil
    }
    
    private static func reflectingArray(_ m: Mirror) -> AvroSchema? {
        return nil
    }
    
    private static func reflectingMap(_ m: Mirror) -> AvroSchema? {
        return nil
    }

    public static func reflecting(_ subject: Any) -> AvroSchema? { 
        let m = Mirror(reflecting: subject)
        switch m.displayStyle {
            case .struct, .class:
            return Self.reflectingRecord(m)
            case .enum:
            return Self.reflectingEnum(m)
            case .set, .collection:
            return Self.reflectingArray(m)
            case .tuple, .dictionary:
            return Self.reflectingMap(m)
            case .optional:
            return nil
            case .some(_):
            return nil
            case .none:
            return nil

        }

        return nil
    }
}