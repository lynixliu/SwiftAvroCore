//
//  AvroClient/Avro.swift
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

public class Avro {
    private var schema: AvroSchema? = nil
    private var schemaEncodingOption: AvroSchemaEncodingOption = .CanonicalForm
    private var encodingOption: AvroEncodingOption = .AvroBinary
    let infoKey = CodingUserInfoKey(rawValue: "encodeOption")!
    private var stream: Data = Data()
    public init() {}
    
    func setSchema(schema: AvroSchema) {
        self.schema = schema
    }
    
    public func getSchema() -> AvroSchema? {
        return self.schema
    }
    
    func defineSchema<T: Codable>(_ value: T) {
        let data = try! JSONEncoder().encode(value)
        print(String(bytes: data, encoding: .utf8)!)
        self.schema = try! AvroSchema(schema: data, decoder: JSONDecoder())
    }
    
    public func setSchemaFormat(option: AvroSchemaEncodingOption) {
        schemaEncodingOption = option
    }
    
    public func setAvroFormat(option: AvroEncodingOption) {
        encodingOption = option
    }
    
    public func decodeSchema(schema: String) -> AvroSchema? {
        let decoder = JSONDecoder()
        do {
            self.schema = try AvroSchema(schemaJson: schema, decoder: decoder)
            return self.schema
        } catch {
            fatalError(error.localizedDescription)
        }
    }
    
    public func decodeSchema(schema: Data) -> AvroSchema? {
        let decoder = JSONDecoder()
        do {
            self.schema = try AvroSchema(schema: schema, decoder: decoder)
            return self.schema
        } catch {
            fatalError(error.localizedDescription)
        }
    }
    
    public func encodeSchema() throws -> Data {
        if let schema = self.schema {
            return try encodeSchema(schema: schema)
        }
        return Data()
    }
    
    public func encodeSchema(schema: AvroSchema) throws -> Data {
        let encoder = JSONEncoder()
        switch schemaEncodingOption {
        case .PrettyPrintedForm:
            encoder.outputFormatting = .prettyPrinted
            encoder.userInfo[infoKey] = schemaEncodingOption
        case .FullForm:
            encoder.outputFormatting = JSONEncoder.OutputFormatting()
            encoder.userInfo[infoKey] = schemaEncodingOption
        case .CanonicalForm:
            encoder.outputFormatting = JSONEncoder.OutputFormatting()
        }
        do {
            if let data = try schema.encode(jsonEncoder: encoder) {
                return data
            } else {
                throw AvroSchemaEncodingError.invalidSchemaType
            }
        } catch {
            fatalError(error.localizedDescription)
        }
    }
    
    public func encode<T: Codable>(_ value: T) throws -> Data {
        if nil == self.schema {
            defineSchema(value)
        }
        do {
            let encoder = AvroEncoder()
            encoder.setUserInfo(userInfo: [infoKey : encodingOption])
            return try encoder.encode(value.self, schema: self.schema!)
        } catch {
            throw error
        }
    }
    
    public func decode<T: Codable>(from: Data) throws -> T {
        guard nil != self.schema else {
            throw BinaryEncodingError.noSchemaSpecified
        }
        do {
            let decoder = AvroDecoder(schema: self.schema!)
            return try decoder.decode(T.self, from: from)
        } catch {
            throw error
        }
    }
    
    public func decode(from: Data) throws -> Any? {
        guard nil != self.schema else {
            throw BinaryEncodingError.noSchemaSpecified
        }
        do {
            let decoder = AvroDecoder(schema: self.schema!)
            return try decoder.decode(from: from)
        } catch {
            throw error
        }
    }
    
    public func decodeFromContinue<T: Codable>(from: Data, schema: AvroSchema) throws -> (T,Int) {
        do {
            return try (from.withUnsafeBytes{ (pointer: UnsafePointer<UInt8>) in
                let decoder = try AvroBinaryDecoder(schema: schema, pointer: pointer, size: from.count)
                return try (decoder.decode(T.self), from.count - decoder.primitive.available)
            })
        } catch {
            throw error
        }
    }
    
    public func newSchema(schema: String) -> AvroSchema? {
        let decoder = JSONDecoder()
        do {
            return try AvroSchema(schemaJson: schema, decoder: decoder)
        } catch {
            fatalError(error.localizedDescription)
        }
    }
    
    public func newSchema(schema: Data) -> AvroSchema? {
        let decoder = JSONDecoder()
        do {
            return try AvroSchema(schema: schema, decoder: decoder)
        } catch {
            fatalError(error.localizedDescription)
        }
    }
    
    public func encodeFrom<T: Codable>(_ value: T, schema: AvroSchema) throws -> Data {
        do {
            let encoder = AvroEncoder()
            encoder.setUserInfo(userInfo: [infoKey : encodingOption])
            return try encoder.encode(value.self, schema: schema)
        } catch {
            throw error
        }
    }
    
    public func decodeFrom<T: Codable>(from: Data, schema: AvroSchema) throws -> T {
        do {
            let decoder = AvroDecoder(schema: schema)
            return try decoder.decode(T.self, from: from)
        } catch {
            throw error
        }
    }
    
    public func decodeFrom(from: Data, schema: AvroSchema) throws -> Any? {
        do {
            let decoder = AvroDecoder(schema: schema)
            return try decoder.decode(from: from)
        } catch {
            throw error
        }
    }
    
    
    public func makeFileObjectContainer(schema: String, codec: CodecProtocol) throws -> ObjectContainer {
        return try ObjectContainer(schema:schema, codec: codec)
    }
}

public enum AvroSchemaEncodingOption: Int {
    case CanonicalForm = 0, FullForm, PrettyPrintedForm
}

public enum AvroEncodingOption: Int {
    case AvroBinary = 0, AvroJson//, AvroSize
}

struct SwiftAvroCore {
    var text = "SwiftAvroCore"
}
