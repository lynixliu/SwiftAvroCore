//
//  AvroClient/AvroSchema.swift
//
//  Created by Yang Liu on 24/08/18.
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

/// define the AVRO Schema
public enum AvroSchema: Codable, Hashable {
    /// primitive types
    case nullSchema
    case booleanSchema
    case intSchema(IntSchema)
    case longSchema(IntSchema)
    case floatSchema
    case doubleSchema
    case bytesSchema(BytesSchema)
    case stringSchema
    /// complex types
    indirect case recordSchema(RecordSchema)
    indirect case enumSchema(EnumSchema)
    indirect case arraySchema(ArraySchema)
    indirect case mapSchema(MapSchema)
    indirect case unionSchema(UnionSchema)
    case fixedSchema(FixedSchema)
    /// rpc types
    indirect case messageSchema(MessageSchema)
    indirect case protocolSchema(ProtocolSchema)
    indirect case errorSchema(ErrorSchema)
    /// private types
    indirect case fieldsSchema([FieldSchema])
    indirect case fieldSchema(FieldSchema)
    /// invalid type
    case invalidSchema
    
    internal enum LogicalType: String, Codable {
        case decimal, date,
        timeMillis = "time-millis", timeMicros = "time-micros",
        timestampMillis = "timestamp-millis", timestampMicros = "timestamp-micros", duration
    }
    internal enum Types: String, Codable {
        case null,boolean,int,long,float,double,bytes,string,
        /// complex types
        record,enums = "enum",array,map,union,fixed,
        /// rpc types
        protocolName = "protocol", message, errors,
        /// private type
        field,error,
        /// invalid type
        invalid
    }
 
    /// default init to invalid schema
    public init() {
        self = .invalidSchema
    }
    func findSchema(name: String) -> AvroSchema? {
        switch self {
        case let .recordSchema(schema):
            if name == "fields" {
                return .fieldsSchema(schema.fields)
            }
            for field in schema.fields {
                if field.name == name {
                    return field.type
                }
            }
        default:
            return .invalidSchema
        }
        return nil
    }
    func getName() -> String? {
        switch self {
        case .nullSchema: return Types.null.rawValue
        case .booleanSchema: return Types.boolean.rawValue
        case .intSchema(let param):
            if let logicType = param.logicalType { return logicType.rawValue}
            return Types.int.rawValue
        case .longSchema(let param):
            if let logicType = param.logicalType { return logicType.rawValue}
            return Types.long.rawValue
        case .floatSchema: return Types.float.rawValue
        case .doubleSchema: return Types.double.rawValue
        case .bytesSchema(let param):
            if let logicType = param.logicalType { return logicType.rawValue}
            return Types.bytes.rawValue
        case .stringSchema: return Types.string.rawValue
        /// complex types
        case .recordSchema(let param):
            return param.name
        case .enumSchema(let param):
            return param.name
        case .arraySchema(let param):
            return param.type
        case .mapSchema(let param):
            return param.type
        case .unionSchema:
            return "union"
        case .fixedSchema(let param):
            return param.name
        /// private type
        case .fieldsSchema:
            return "fields"
        case .fieldSchema(let param):
            if param.name.isEmpty {
                return param.name
            }
            return param.name
        /// rpc type
        case .protocolSchema(let param):
            return param.type
        case .errorSchema(let param):
            return param.name
        default: return nil
        }
    }

/// structure to encode and decode record in json
public struct RecordSchema : Equatable, NameSchemaProtocol {
    var name: String?
    var namespace: String?
    var type: String
    var fields: [FieldSchema]
    var aliases: Set<String>?
    let doc: String?
    private enum CodingKeys: CodingKey {
        case name, type, namespace, aliases, fields, doc
    }
    
    var resolution: ResolutionMethod = .useDefault
}

/// structure to encode and decode fields in json
public struct FieldSchema : Equatable, Codable {
    let name: String
    var type: AvroSchema
    let doc: String?
    let order: String?
    let aliases: [String]?
    let defaultValue: String?
    let optional: Bool?
    var resolution: ResolutionMethod = .useDefault
}
/// structure to encode and decode enum in json
public struct EnumSchema : Equatable, NameSchemaProtocol {
    var name: String?
    var namespace: String?
    var type: String
    var aliases: Set<String>?
    let doc: String?
    var symbols: [String]
    var resolution: ResolutionMethod = .useDefault
    
    private enum CodingKeys: CodingKey {
        case name, type, namespace, aliases, symbols, doc
    }
}

/// structure to encode and decode array in json
public struct ArraySchema : Equatable, Codable {
    let type: String
    var items: AvroSchema
    var resolution: ResolutionMethod = .useDefault
    
    private enum CodingKeys: CodingKey {
        case type, items
    }
}

/// structure to encode and decode map in json
public struct MapSchema : Equatable, Codable {
    let type: String
    var values: AvroSchema
    var resolution: ResolutionMethod = .useDefault
    private enum CodingKeys: CodingKey {
        case type, values
    }
}

/// structure to encode and decode fixed in json
public struct FixedSchema : Equatable, NameSchemaProtocol {
    var name: String? = nil
    var namespace: String? = nil
    var type: String = "fixed"
    var aliases: Set<String>? = nil
    var logicalType: LogicalType? = nil /// must be "duration/decimal" if set, the size of duration must be 12
    var size: Int = 0
    var precision: Int? = nil
    var scale: Int? = nil
    var resolution: ResolutionMethod = .useDefault
    private enum CodingKeys: CodingKey {
        case name, type, namespace, aliases, size, logicalType, precision, scale
    }
    func validate() -> Bool {
        if let logic = logicalType, logic == .decimal {
            if let p = precision, p > 0 {
                if let s = scale, s > p || s < 0 {
                    return false
                }
                /// Todo: the log10 is base on Darwin
                /// need to be replace with a func of cross platform lib
                if p > size {
                    let bits = (size - 1) << 3
                    var realPrecision = Int (bits / 10) * 3
                    if p <= realPrecision {
                        return true
                    }
                    let lowerBits = bits % 10
                    if lowerBits > 0 {
                        var lowerNum = 1 << lowerBits - 1
                        while lowerNum > 10 {
                            lowerNum /= 10
                            realPrecision += 1
                            if p <= realPrecision {
                                return true
                            }
                        }
                    }
                    return p <= realPrecision
                }
            }
            return false
        }
        return true
    }
}

/// structure to encode and decode bytes in json
public struct BytesSchema : Equatable, Codable {
    var type:String = "bytes"
    /// for logic decimal type
    var logicalType: LogicalType? = nil //must be "decimal" if set
    var precision: Int? = nil
    var scale: Int? = nil
    
    init(){}
    init(logicalType: LogicalType, precision: Int, scale: Int) {
        self.logicalType = logicalType
        self.precision = precision
        self.scale = scale
    }
    func validate() -> Bool {
        if logicalType != nil {
            if let p = precision, p > 0 {
                if let s = scale, s <= p {
                    return true
                }
            }
            return false
        }
        return true
    }
}

/// structure to encode and decode int, logic date and millis in json
public struct UnionSchema : Equatable, Codable {
    var name: String?
    let optional: String?
    /// for logic decimal type
    var branches: [AvroSchema]//can be <"date"/"time-millis">
    init(branches: [AvroSchema]) {
        self.name = nil
        self.optional = nil
        self.branches = branches
    }
    init(name: String, optional: String, branches: [AvroSchema]) {
        self.name = name
        self.optional = optional
        self.branches = branches
    }
}

/// structure to encode and decode int, logic date and millis in json
/// or long, logic time-micros, timestamp-millis
public struct IntSchema : Equatable, Codable {
    let type:String
    /// for logic decimal type
    var logicalType: LogicalType? = nil//can be <"date"/"time-millis">
    init() {
        self.type = "int"
        self.logicalType = nil
    }
    init(isLong: Bool) {
        self.type = isLong ? "long" : "int"
        self.logicalType = nil
    }
    init(type: String, logicalType: LogicalType) {
        self.type = type
        self.logicalType = logicalType
    }
}
public typealias ErrorSchema = RecordSchema
// structure to encode and decode record in json
public struct ProtocolSchema : Equatable, NameSchemaProtocol {
    var type: String
    var name: String?
    var namespace: String? //{get set}
    var types: [AvroSchema]?
    var messages: Dictionary<String,Message>?
    var aliases: Set<String>? //{get set}
    let doc: String? //{get set}
    enum CodingKeys: String, CodingKey {
        case type = "protocol", name, namespace, types, messages, aliases, doc
    }
    var resolution: ResolutionMethod = .useDefault
}
    struct Message : Equatable, Codable {
       let doc: String?
       let request: [RequestType]?
       let response: String?
       let errors: [String]?
       let optional: Bool?
       var resolution: ResolutionMethod = .useDefault
    }
public struct MessageSchema : Equatable, Codable {
    let doc: String?
    let request: [AvroSchema]?
    let response: AvroSchema?
    let errors: ErrorSchema?
    let optional: Bool?
    var resolution: ResolutionMethod = .useDefault
}
/// structure to encode and decode fields in json
    
struct RequestType: Equatable, Codable {
    let name: String
    let type: String
}

enum ResolutionMethod: Int, Codable {
    case useDefault
    case accept
    case skip
}
}
protocol NameSchemaProtocol: Codable {
    /// attributes required by named schema
    var type: String {get set}
    var name: String? {get set}
    var namespace: String? {get set}
    var aliases: Set<String>? {get set}
    /// skip flag for schema resolution, if the skip is true,
    /// the decoder should skip the block in reading data
    var resolution: AvroSchema.ResolutionMethod {get set}
}

extension NameSchemaProtocol {
    func getFullname() -> String {
        if let n = self.name {
            if n.contains(".") {
                return n
            }
            if let ns = self.namespace {
                return (ns + "." + n)
            }
            return n
        }
        return self.type
    }
    
    func getNamespace() -> String? {
        if let n = name, n.contains(".") {
            let index = n.lastIndex(of: ".") ?? n.endIndex
            let beginning = n[..<index]
            return String(beginning)
        }
        return namespace
    }
}

