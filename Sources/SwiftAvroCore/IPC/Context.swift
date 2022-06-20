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
}

extension Data {
    public mutating func framing(frameLength: Int32) {
        let ending = Swift.withUnsafeBytes(of: Int32(0).bigEndian, Array.init)
        if self.count == 0 {
            self.append(contentsOf: ending)
            return
        }
        if self.count <= frameLength {
            let array = Swift.withUnsafeBytes(of: Int32(self.count).bigEndian, Array.init)
            self.insert(contentsOf: array, at: 0)
            self.append(contentsOf: ending)
            return
        }
        let frames = Int32(self.count)/frameLength
        let frameLenArray = Swift.withUnsafeBytes(of: frameLength.bigEndian, Array.init)
        let rest = Int32(self.count)%frameLength
        let frameStep = frameLength + 4
        for i in 0..<frames {
            self.insert(contentsOf: frameLenArray, at: Int(i*frameStep))
        }
        if rest > 0 {
            let last = Swift.withUnsafeBytes(of: rest.bigEndian, Array.init)
            self.insert(contentsOf: last, at: self.count-Int(rest))
        }
        self.append(contentsOf: ending)
        return
    }
    
    public func deFraming() -> [Data] {
        guard self.count > 4 else {
            return []
        }
        let len = self.count - 4
        var frames = [Data]()
        let lengthBytes = [UInt8](self[0...3])
        let bigEndianValue = UInt32(bigEndian: lengthBytes.withUnsafeBufferPointer {
            $0.baseAddress!.withMemoryRebound(to: UInt32.self, capacity: 1) { $0.pointee }
        })
        let frameLen = Int(bigEndianValue+4)
        let frameCount = len/frameLen
        for i in 0..<frameCount {
            let loc = i * frameLen + 4
            frames.append(self.subdata(in: (loc..<(loc+Int(bigEndianValue)))))
        }
        let rest = len%Int(bigEndianValue+4) - 4
        if rest > 0 {
            frames.append(self.subdata(in: (self.count-4-rest)..<Int(self.count-4)))
        }
        return frames
    }
}
