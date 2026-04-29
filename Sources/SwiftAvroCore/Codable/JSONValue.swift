/// Native Swift representation of JSON values, replacing Foundation types for OS-independence.
/// Not Codable directly — encoding is handled by AvroJSONEncoder's container stack.
public enum JSONValue: Equatable, Sendable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])
}
