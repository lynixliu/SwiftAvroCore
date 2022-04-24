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

class Context {
    let handshakeRequestMeta: [String: [UInt8]]
    let handshakeResponeMeta: [String: [UInt8]]
    let requestMeta:[String: [UInt8]]
    let responseMeta: [String: [UInt8]]
    let requestSchema: AvroSchema
    let responseSchema: AvroSchema
    
    init(handshakeRequestMeta: [String: [UInt8]], handshakeResponeMeta: [String: [UInt8]], requestMeta:[String: [UInt8]], responseMeta:[String: [UInt8]]) {
        self.handshakeRequestMeta = handshakeRequestMeta
        self.handshakeResponeMeta = handshakeResponeMeta
        self.requestMeta = requestMeta
        self.responseMeta = responseMeta
        let avro = Avro()
        self.requestSchema = avro.decodeSchema(schema: MessageConstant.requestSchema)!
        self.responseSchema = avro.decodeSchema(schema: MessageConstant.responseSchema)!
    }
}
