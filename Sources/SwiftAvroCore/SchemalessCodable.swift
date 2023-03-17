//
//  SchemalessCodable.swift
//  
//
//  Created by standard on 3/17/23.
//

import Foundation
public enum SchemalessCodable: Codable {
    indirect case record(RecordSchemalessCodable)
    indirect case enums(EnumSchemalessCodable)
    indirect case array(ArraySchemalessCodable)
    indirect case map(MapSchemalessCodable)

    init(from decoder: Decoder) throws {
        let avroBinaryDecoder = decoder as! AvroBinaryDecoder
                
        switch avroBinaryDecoder.schema {
        case .recordSchema(_), .fieldsSchema(_):
            self = .record(try RecordSchemalessCodable(from: decoder))
        case .enumSchema(_):
            self = .enums(try EnumSchemalessCodable(from: decoder))
        case .mapSchema(_):
            self = .map(try MapSchemalessCodable(from: decoder))
        case .arraySchema(_):
            self = .array(try ArraySchemalessCodable(from: decoder))
        default:
            throw fatalError("non record shcema")
        }
    }
}

public struct EnumSchemalessCodable: Codable {
    init(from decoder: Decoder) throws {
    }
    
    func encode(to encoder: Encoder) throws {
        throw fatalError("dont encode it like that")
    }
}


public struct ArraySchemalessCodable: Codable {
    init(from decoder: Decoder) throws {
    }
    
    func encode(to encoder: Encoder) throws {
        throw fatalError("dont encode it like that")
    }
}

public struct MapSchemalessCodable: Codable {
    init(from decoder: Decoder) throws {
    }
    
    func encode(to encoder: Encoder) throws {
        throw fatalError("dont encode it like that")
    }
}


public struct RecordSchemalessCodable: Codable {
    private var data: [String: Any]
    init(from decoder: Decoder) throws {
        let avroBinaryDecoder = decoder as! AvroBinaryDecoder
        
        var fields = [AvroSchema.FieldSchema]()
        
        switch avroBinaryDecoder.schema {
        case let .recordSchema(recordSchema):
            fields = recordSchema.fields
        case let .fieldsSchema(fieldSchema):
            fields = fieldSchema
        default:
            throw fatalError("non record shcema")
        }
        
        let keys = fields.map {
            RuntimeCodingKey(stringValue: $0.name)! }

        
        let container = try avroBinaryDecoder.container(keyedBy: RuntimeCodingKey.self)
        var data = [String: Any]()

        let types: [Decodable.Type] = [
            Bool.self,
            Int.self,
            Double.self,
            Float.self,
            [UInt8].self,
            String.self,
            SchemalessCodable.self,
            [String: SchemalessCodable].self,
            [SchemalessCodable].self
        ]

        for key in keys {
            for type in types {
                if let value = try? container.decode(type, forKey: key) {
                    data[key.stringValue] = value
                    break
                }
            }
            if data[key.stringValue] == nil {
                throw DecodingError.dataCorruptedError(
                    forKey: key,
                    in: container,
                    debugDescription: "Unknown value type"
                )
            }
        }

        self.data = data
    }


    func encode(to encoder: Encoder) throws {
        throw fatalError("dont encode it like that")
    }

    private struct RuntimeCodingKey: CodingKey {
        init?(stringValue: String) {
            self.stringValue = stringValue
        }
        
        init?(intValue: Int) {
            self.intValue = intValue
            self.stringValue = ""
        }
        
        var stringValue: String
        
        
        var intValue: Int?
        
    }
}


public extension SchemalessCodable {
    subscript(keyPath: String) -> Any? {
        get {
            guard case let .record(recordSchemalessCodable) = self else {
                return nil
            }
            return recordSchemalessCodable[keyPath]

        }
    }
}

public extension RecordSchemalessCodable {
    
    subscript(keyPath: String) -> Any? {
        get {
            var keys = keyPath.components(separatedBy: ".")
            guard let firstKey = keys.first else {
                return nil
            }
            keys.removeFirst()
            if keys.isEmpty {
                return self.data[firstKey]
            } else {
                let nestedData = self.data[firstKey] as? [String: Any]
                let nestedKeyPath = keys.joined(separator: ".")
                return nestedData?[nestedKeyPath]
            }
        }
        
        set {
            var keys = keyPath.components(separatedBy: ".")
            guard let firstKey = keys.first else {
                return
            }
            keys.removeFirst()
            if keys.isEmpty {
                self.data[firstKey] = newValue
            } else {
                let nestedKeyPath = keys.joined(separator: ".")
                var nestedData = self.data[firstKey] as? [String: Any] ?? [:]
                nestedData[nestedKeyPath] = newValue
                self.data[firstKey] = nestedData
            }
        }
    }
    
}
