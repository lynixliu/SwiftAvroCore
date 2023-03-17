//
//  SchemalessDecodable.swift
//  
//
//  Created by standard on 3/17/23.
//

import Foundation
public enum SchemalessDecodable: Decodable {
    indirect case record(RecordSchemalessDecodable)
    indirect case enums(EnumSchemalessDecodable)
    indirect case array(ArraySchemalessDecodable)
    indirect case map(MapSchemalessDecodable)

    public init(from decoder: Decoder) throws {
        let avroBinaryDecoder = decoder as! AvroBinaryDecoder
                
        switch avroBinaryDecoder.schema {
        case .recordSchema(_), .fieldsSchema(_):
            self = .record(try RecordSchemalessDecodable(from: decoder))
        case .enumSchema(_):
            self = .enums(try EnumSchemalessDecodable(from: decoder))
        case .mapSchema(_):
            self = .map(try MapSchemalessDecodable(from: decoder))
        case .arraySchema(_):
            self = .array(try ArraySchemalessDecodable(from: decoder))
        default:
            throw fatalError("non record shcema")
        }
    }
}

public struct EnumSchemalessDecodable: Decodable {
    public init(from decoder: Decoder) throws {
    }
}


public struct ArraySchemalessDecodable: Decodable {
    public init(from decoder: Decoder) throws {
    }
}

public struct MapSchemalessDecodable: Decodable {
    public init(from decoder: Decoder) throws {
    }
}


public struct RecordSchemalessDecodable: Decodable {
    private var data: [String: Any]
    public init(from decoder: Decoder) throws {
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
            SchemalessDecodable.self,
            [String: SchemalessDecodable].self,
            [SchemalessDecodable].self
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


public extension SchemalessDecodable {
    subscript(keyPath: String) -> Any? {
        get {
            guard case let .record(recordSchemalessDecodable) = self else {
                return nil
            }
            return recordSchemalessDecodable[keyPath]

        }
    }
}

public extension RecordSchemalessDecodable {
    
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
