//
//  Protocol.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 1/05/22.
//

import Foundation
public struct AvroProtocol : Equatable, Codable {
    let type: String = "protocol"
    var name: String
    var namespace: String? //{get set}
    var types: [AvroSchema]?
    var messages: Dictionary<String, Message>?
    var aliases: Set<String>? //{get set}
    let doc: String? //{get set}
    private var typeMap: [String: AvroSchema]
    enum CodingKeys: String, CodingKey {
        case type, name = "protocol", namespace, types, messages, aliases, doc
    }
    var resolution: AvroSchema.ResolutionMethod = .useDefault
    public init(from decoder: Decoder) throws {
        self.resolution = .useDefault
        if let container = try? decoder.container(keyedBy: CodingKeys.self) {
            if let protocolName = try! container.decodeIfPresent(String.self, forKey: .name){
                self.name = protocolName
            } else {
                throw AvroSchemaDecodingError.unknownSchemaJsonFormat
            }
            if let ns = try? container.decodeIfPresent(String.self, forKey: .namespace) {
                self.namespace = ns
            } else {
                self.namespace = ""
            }
            if let types = try container.decodeIfPresent([AvroSchema].self, forKey: .types) {
                self.types = types
                self.typeMap = [String: AvroSchema]()
                for t in types {
                    typeMap[t.getName()!] = t
                }
            } else {
                self.types = []
                self.typeMap = [String:AvroSchema]()
            }
            if let aliases = try? container.decodeIfPresent(Set<String>.self, forKey: .aliases) {
                self.aliases = aliases
            } else {
                self.aliases = nil
            }
            if let doc = try? container.decodeIfPresent(String.self, forKey: .doc) {
                self.doc = doc
            } else {
                self.doc = ""
            }
            let nestContainer = try container.nestedContainer(keyedBy: StringCodingKey.self, forKey: .messages)
                var messageMap = Dictionary<String, Message>()
                for k in nestContainer.allKeys {
                        messageMap[k.stringValue] = try nestContainer.decodeIfPresent(Message.self, forKey: k)
                }
                self.messages = messageMap
        } else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
    }

    public static func == (lhs: AvroProtocol, rhs: AvroProtocol) -> Bool {
        if (lhs.type != rhs.type) {return false}
        if (lhs.namespace != rhs.namespace) {return false}
        if lhs.types?.count != rhs.types?.count {return false}
        if let types = lhs.types {
            for t in types {
                if !rhs.types!.contains(t) {
                    return false
                }
            }
        }
        return true
    }
    
    public mutating func addType(schema: AvroSchema) {
        if self.types == nil {
            self.types = [AvroSchema]()
            self.typeMap = [String: AvroSchema]()
        }
        for t in types! {
            if t == schema {
                return
            }
        }
        types!.append(schema)
        typeMap[schema.getName()!] = schema
    }
    
    public mutating func addMessage(name: String, message: Message) {
        if !message.validate(types: types!) {
            return
        }
        if self.messages == nil {
            self.messages = Dictionary<String, Message>()
        }
        self.messages![name] = message
    }
    
    public func getRequest(messageName: String) -> [AvroSchema]? {
        if let message = messages![messageName] {
            var msgs = [AvroSchema]()
            for r in message.request! {
                if let t = typeMap[r.type] {
                    msgs.append(t)
                }
            }
            return msgs
        }
        return nil
    }
    
    public func getResponse(messageName: String) -> AvroSchema? {
        if let message = messages![messageName] {
            return typeMap[message.response!]
        }
        return nil
    }
    
    public func getErrors(messageName: String) -> [String:AvroSchema]? {
        if let message = messages![messageName] {
            var errors = [String:AvroSchema]()
            for e in message.errors! {
                if let t = typeMap[e] {
                    errors[t.getName()!] = t
                }
            }
            return errors
        }
        return nil
    }
    
    struct StringCodingKey: CodingKey {
        var intValue: Int?
        
        let stringValue: String
        
        init?(stringValue: String) {
            self.stringValue = stringValue
            self.intValue = Int(stringValue)
        }
        
        init?(intValue: Int) {
            self.stringValue = "\(intValue)"
            self.intValue = intValue
        }
    }
}

public struct Message : Equatable, Codable {
    enum CodingKeys: String, CodingKey {
        case request, response, errors, oneway = "one-way", doc
    }
    let doc: String?
    var request: [RequestType]?
    let response: String?
    var errors: [String]?
    let oneway: Bool?
    var resolution: AvroSchema.ResolutionMethod = .useDefault
    
    mutating func addRequest(types: [AvroSchema], name: String, type: String) {
        if self.request == nil {
            self.request = [RequestType]()
        }
        for t in types {
            if t.getName()! == type {
                for r in request! {
                    if r.type == type && r.name == name {
                        return
                    }
                }
                self.request!.append(RequestType(name: name, type: type))
            }
        }
    }
    
    mutating func addError(types: [AvroSchema], errorName: String) {
        if self.errors == nil {
            self.errors = [String]()
        }
        for t in types {
            if t.getName()! == errorName {
                for err in errors! {
                    if err == errorName {
                        return
                    }
                }
                self.errors!.append(errorName)
            }
        }
    }
    
    public func validate(types: [AvroSchema]) ->Bool {
        var typeMap = [String: AvroSchema]()
        for t in types {
            typeMap[t.getName()!] = t
        }
        if let requests = request {
            for req in requests {
                if !typeMap.contains(where: { (key: String, value: AvroSchema) in
                    return key == req.type
                }) {
                    return false
                }
            }
        }
        if let res = response {
            if !typeMap.contains(where: { (key: String, value: AvroSchema) in
                return key == res
            }) {
                return false
            }
        }
        if let errs = errors {
            for err in errs {
                if !typeMap.contains(where: { (key: String, value: AvroSchema) in
                    return key == err
                }) {
                    return false
                }
            }
        }
        return true
    }
}

struct RequestType: Equatable, Codable {
    let name: String
    let type: String
}
