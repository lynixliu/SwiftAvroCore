//
//  Protocol.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 1/05/22.
//

import Foundation
// MARK: - AvroProtocol

public struct AvroProtocol: Equatable, Codable {
    // The JSON key for the protocol name is "protocol", not "name".
    public let type: String
    public var name: String
    public var namespace: String?
    public var types: [AvroSchema]?
    public var messages: [String: Message]?
    public var aliases: Set<String>?
    public let doc: String?
    var resolution: AvroSchema.ResolutionMethod

    private var typeMap: [String: AvroSchema]

    enum CodingKeys: String, CodingKey {
        case type, name = "protocol", namespace, types, messages, aliases, doc
    }

    public init(from decoder: Decoder) throws {
        resolution = .useDefault
        let container = try decoder.container(keyedBy: CodingKeys.self)

        guard let protocolName = try container.decodeIfPresent(String.self, forKey: .name) else {
            throw AvroSchemaDecodingError.unknownSchemaJsonFormat
        }
        name = protocolName
        // `type` is always "protocol" for AvroProtocol objects.
        type = "protocol"
        namespace = try container.decodeIfPresent(String.self, forKey: .namespace) ?? ""
        aliases = try container.decodeIfPresent(Set<String>.self, forKey: .aliases)
        doc = try container.decodeIfPresent(String.self, forKey: .doc) ?? ""

        if let decodedTypes = try container.decodeIfPresent([AvroSchema].self, forKey: .types) {
            types = decodedTypes
            typeMap = Dictionary(uniqueKeysWithValues: decodedTypes.compactMap { schema in
                schema.getName().map { ($0, schema) }
            })
        } else {
            types = []
            typeMap = [:]
        }

        let nestContainer = try container.nestedContainer(
            keyedBy: StringCodingKey.self, forKey: .messages
        )
        messages = try nestContainer.allKeys.reduce(into: [:]) { result, key in
            result[key.stringValue] = try nestContainer.decodeIfPresent(Message.self, forKey: key)
        }
    }

    public static func == (lhs: AvroProtocol, rhs: AvroProtocol) -> Bool {
        guard lhs.type == rhs.type, lhs.namespace == rhs.namespace else { return false }
        guard lhs.types?.count == rhs.types?.count else { return false }
        if let lhsTypes = lhs.types, let rhsTypes = rhs.types {
            return lhsTypes.allSatisfy { rhsTypes.contains($0) }
        }
        return true
    }

    public mutating func addType(schema: AvroSchema) {
        if types == nil {
            types = []
            typeMap = [:]
        }
        guard let typeName = schema.getName(), types?.contains(schema) == false else { return }
        types!.append(schema)
        typeMap[typeName] = schema
    }

    public mutating func addMessage(name: String, message: Message) {
        guard let existingTypes = types, message.validate(types: existingTypes) else { return }
        if messages == nil { messages = [:] }
        messages![name] = message
    }

    public func getRequest(messageName: String) -> [AvroSchema]? {
        guard let message = messages?[messageName],
              let requestFields = message.request else { return nil }
        return requestFields.compactMap { typeMap[$0.type] }
    }

    public func getResponse(messageName: String) -> AvroSchema? {
        guard let response = messages?[messageName]?.response else { return nil }
        return typeMap[response]
    }

    public func getErrors(messageName: String) -> [String: AvroSchema]? {
        guard let errorNames = messages?[messageName]?.errors else { return nil }
        return errorNames.reduce(into: [:]) { result, name in
            if let schema = typeMap[name] {
                result[schema.getName() ?? name] = schema
            }
        }
    }

    // MARK: Private helpers

    private struct StringCodingKey: CodingKey {
        let stringValue: String
        var intValue: Int?

        init?(stringValue: String) {
            self.stringValue = stringValue
            intValue = Int(stringValue)
        }

        init?(intValue: Int) {
            stringValue = "\(intValue)"
            self.intValue = intValue
        }
    }
}

// MARK: - Message

public struct Message: Equatable, Codable {
    enum CodingKeys: String, CodingKey {
        case request, response, errors, oneway = "one-way", doc
    }

    public let doc: String?
    public var request: [RequestType]?
    public let response: String?
    public var errors: [String]?
    public let oneway: Bool?
    var resolution: AvroSchema.ResolutionMethod = .useDefault

    public mutating func addRequest(types: [AvroSchema], name: String, type: String) {
        if request == nil { request = [] }
        let typeExists = types.contains { $0.getName() == type }
        let alreadyAdded = request?.contains { $0.type == type && $0.name == name } ?? false
        if typeExists && !alreadyAdded {
            request!.append(RequestType(name: name, type: type))
        }
    }

    public mutating func addError(types: [AvroSchema], errorName: String) {
        if errors == nil { errors = [] }
        let typeExists = types.contains { $0.getName() == errorName }
        let alreadyAdded = errors?.contains(errorName) ?? false
        if typeExists && !alreadyAdded {
            errors!.append(errorName)
        }
    }

    public func validate(types: [AvroSchema]) -> Bool {
        let typeNames = Set(types.compactMap { $0.getName() })
        if let reqs = request, reqs.contains(where: { !typeNames.contains($0.type) }) {
            return false
        }
        if let res = response, !typeNames.contains(res) { return false }
        if let errs = errors, errs.contains(where: { !typeNames.contains($0) }) {
            return false
        }
        return true
    }
}

// MARK: - RequestType

public struct RequestType: Equatable, Codable, Sendable {
    public let name: String
    public let type: String
}
