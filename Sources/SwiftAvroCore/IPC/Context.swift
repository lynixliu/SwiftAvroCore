//
//  Context.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 20/04/22.
//

import Foundation

struct MessageConstant {
    static let requestSchema:String = """
   {
     "type": "record",
     "name": "HandshakeRequest",
     "namespace":"org.apache.avro.ipc",
     "fields": [
       {"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}},
       {"name": "clientProtocol", "type": ["null", "string"]},
       {"name": "serverHash", "type": "MD5"},
       {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
     ]
   }
  """
    
    static let responseSchema:String = """
  {
    "type": "record",
    "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
    "fields": [
      {"name": "match",
       "type": {"type": "enum", "name": "HandshakeMatch",
                "symbols": ["BOTH", "CLIENT", "NONE"]}},
      {"name": "serverProtocol",
       "type": ["null", "string"]},
      {"name": "serverHash",
       "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
      {"name": "meta",
       "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
  }
  
  """
    static let metadataSchema:String = """
{"type": "map", "values": "bytes"}
"""
    
}
enum HandshakeMatch:String,Codable {
    case BOTH
    case CLIENT
    case NONE
}

struct HandshakeRequest:Codable {
    let clientHash: [UInt8]
    let clientProtocol: String?
    let serverHash: [UInt8]
    var meta: [String: [UInt8]]?
}

struct HandshakeResponse:Codable {
    let match: HandshakeMatch
    let serverProtocol: String?
    let serverHash: [UInt8]?
    var meta: [String: [UInt8]]?
}

struct RequestHeader:Codable {
    let meta: [String: [UInt8]]?
    let name: String
}

struct ResponseHeader:Codable {
    let meta: [String: [UInt8]]?
    let flag: Bool
}

class Context {
    let requestMeta:[String: [UInt8]]
    let responseMeta: [String: [UInt8]]
    let requestSchema: AvroSchema
    let responseSchema: AvroSchema
    let metaSchema: AvroSchema
    
    init(requestMeta:[String: [UInt8]], responseMeta:[String: [UInt8]]) {
        self.requestMeta = requestMeta
        self.responseMeta = responseMeta
        let avro = Avro()
        self.requestSchema = avro.decodeSchema(schema: MessageConstant.requestSchema)!
        self.responseSchema = avro.decodeSchema(schema: MessageConstant.responseSchema)!
        self.metaSchema = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
    }
    
    public func enFrames(data: inout Data, frameLength: Int32) -> Data {
        if data.count <= frameLength {
            let array = withUnsafeBytes(of: Int32(data.count).bigEndian, Array.init)
            data.insert(contentsOf: array, at: 0)
            let ending = withUnsafeBytes(of: Int32(0).bigEndian, Array.init)
            data.append(contentsOf: ending)
            return data
        }
        let frames = Int32(data.count)/frameLength
        let rest = Int32(data.count)%frameLength
        for i in 0...frames {
            let array = withUnsafeBytes(of: frameLength.bigEndian, Array.init)
            data.insert(contentsOf: array, at: Int(i))
        }
        let ending = withUnsafeBytes(of: rest.bigEndian, Array.init)
        data.append(contentsOf: ending)
        return data
    }
    
    public func deFrames(from data: inout Data) -> [Data] {
        guard data.count >= 4 else {
            return [data]
        }
        var frames = [Data]()
        let lengthBytes = [UInt8](data[0...4])
        let bigEndianValue = lengthBytes.withUnsafeBufferPointer {
            $0.baseAddress!.withMemoryRebound(to: UInt32.self, capacity: 1) { $0.pointee }
        }
        let frameCount = UInt32(data.count)/(bigEndianValue+4)
        for i in 0...frameCount {
            frames.append(data.subdata(in: (Int(i+4)..<Int(i+4+bigEndianValue))))
        }
        let rest = data.count%Int(bigEndianValue+4)
        if rest > 4 {
            frames.append(data.subdata(in: (data.count-rest)..<Int(data.count-4)))
        }
        return frames
    }
}
