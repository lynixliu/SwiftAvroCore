/// Native Swift representation of JSON values, replacing Foundation types for OS-independence.
/// Not Codable directly — encoding is handled by AvroJSONEncoder's container stack.
public enum JSONValue: Equatable, Codable, Sendable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public init(from decoder: Decoder) throws {
        if var array = try? decoder.unkeyedContainer() {
            var values: [JSONValue] = []
            while !array.isAtEnd {
                values.append(try array.decode(JSONValue.self))
            }
            self = .array(values)
        } else if let object = try? decoder.container(keyedBy: JSONCodingKey.self) {
            var values: [String: JSONValue] = [:]
            for key in object.allKeys {
                values[key.stringValue] = try object.decode(JSONValue.self, forKey: key)
            }
            self = .object(values)
        } else {
            let single = try decoder.singleValueContainer()
            if single.decodeNil() {
                self = .null
            } else if let value = try? single.decode(Bool.self) {
                self = .bool(value)
            } else if let value = try? single.decode(Int64.self) {
                self = .int(value)
            } else if let value = try? single.decode(Double.self) {
                self = .double(value)
            } else {
                self = .string(try single.decode(String.self))
            }
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .array(let values):
            var container = encoder.unkeyedContainer()
            for value in values { try container.encode(value) }
        case .object(let values):
            var container = encoder.container(keyedBy: JSONCodingKey.self)
            for (key, value) in values {
                try container.encode(value, forKey: JSONCodingKey(stringValue: key)!)
            }
        default:
            var container = encoder.singleValueContainer()
            switch self {
            case .null:           try container.encodeNil()
            case .bool(let v):   try container.encode(v)
            case .int(let v):    try container.encode(v)
            case .double(let v): try container.encode(v)
            case .string(let v): try container.encode(v)
            default: break
            }
        }
    }
}

private struct JSONCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}
