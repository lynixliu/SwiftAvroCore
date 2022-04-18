//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
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

struct HandshakeRequest:Codable {
    let clientHash: [UInt8]
    let clientProtocol: String?
    let serverHash: [UInt8]
    var meta: [String: [UInt8]]?
}

class MessageRequest {
    //.MapSchema(schema: "{\"type\": \"map\", \"values\": \"bytes\"}")
    let avro: Avro
    let longSchema = AvroSchema.IntSchema(isLong:true)//try avro.newSchema(schema: "long")
    var sessionCache: [[UInt8]: AvroSchema]
    var clientRequest: HandshakeRequest
    let responseSchema: AvroSchema
    public init(clientHash: [UInt8], clientProtocol: String) throws {
        self.avro = Avro()
        self.sessionCache = [[UInt8]:AvroSchema]()
        clientRequest = HandshakeRequest(clientHash: clientHash, clientProtocol: clientProtocol, serverHash: clientHash)
        _ = avro.decodeSchema(schema: MessageConstant.requestSchema)
        responseSchema = avro.newSchema(schema: MessageConstant.responseSchema)!
    }
    
    public func encodeHandshakeRequest(request: HandshakeRequest) throws -> Data {
        return try avro.encode(request)
    }
    
    /*
     avro handshake
     client --->
     HandshakeRequest protocl schema in json| clientHash|null client protocol| serverHash (same as clientHash)
     server <----
     HandshakeResponse protocl schema in json:
      * match=BOTH, serverProtocol=null, serverHash=null if the client sent the valid hash of the server's protocol and the server knows what protocol corresponds to the client's hash. In this case, the request is complete and the response data immediately follows the HandshakeResponse.
      * match=CLIENT, serverProtocol!=null, serverHash!=null if the server has previously seen the client's protocol, but the client sent an incorrect hash of the server's protocol. The request is complete and the response data immediately follows the HandshakeResponse. The client must use the returned protocol to process the response and should also cache that protocol and its hash for future interactions with this server.
      * match=NONE if the server has not previously seen the client's protocol. The serverHash and serverProtocol may also be non-null if the server's protocol hash was incorrect.
     In this case the client must then re-submit its request with its protocol text (clientHash!=null, clientProtocol!=null, serverHash!=null) and the server should respond with a successful match (match=BOTH, serverProtocol=null, serverHash=null) as above.
    */
    public func initHandshakeRequest() throws -> Data {
        return try encodeHandshakeRequest(request: HandshakeRequest(clientHash: clientRequest.clientHash, clientProtocol: nil, serverHash: clientRequest.clientHash))
    }
    
    public func decodeResponse(responseData: Data) throws -> (HandshakeResponse, Data) {
        let (response, next) = try avro.decodeFromContinue(from: responseData, schema: responseSchema) as (HandshakeResponse,Int)
        return (response,responseData.subdata(in: next..<responseData.count))
    }
    
    public func resolveHandshakeResponse(response: HandshakeResponse) throws -> Data? {
        switch response.match {
        case .NONE:
            return try encodeHandshakeRequest(request:HandshakeRequest(clientHash: clientRequest.clientHash, clientProtocol: clientRequest.clientProtocol, serverHash: response.serverHash!))
        case .CLIENT:
            sessionCache[response.serverHash!] = avro.newSchema(schema:response.serverProtocol!)!
            return nil
        default:
            return nil
        }
    }
    
    public func decodeResponseData<T: Codable>(header: HandshakeResponse, responseData: Data) throws -> T {
        guard let _ = header.serverHash else {
            throw AvroHandshakeError.noServerHash
        }
        if header.serverHash == clientRequest.clientHash {
            return try avro.decodeFrom(from: responseData, schema: responseSchema)
        }
        guard let sc = sessionCache[header.serverHash!] else {
            throw AvroHandshakeError.noServerHash
        }
        return try avro.decodeFrom(from: responseData, schema: sc)
    }
    
    public func outdateSession(header: HandshakeResponse) {
        sessionCache.removeValue(forKey: header.serverHash!)
    }
    
    public func clearSession() {
        sessionCache.removeAll()
    }
    
    /*
     The format of a call request is:
     * request metadata, a map with values of type bytes
     * the message name, an Avro string, followed by
     * the message parameters. Parameters are serialized according to
     the message's request declaration.
    */
    public func writeRequest<T:Codable>(meta: [String: [UInt8]]?, messageName: String?, parameters: [T]) throws -> Data {
        var data = Data()
        if let meta = meta {
            let metaSchema = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
            let d = try? avro.encodeFrom(meta, schema: metaSchema)
            data.append(d!)
        }
        if let name = messageName {
            let d = try? avro.encodeFrom(name, schema: AvroSchema.init(type: "string"))
            data.append(d!)
            if let serverProtocol = sessionCache[clientRequest.serverHash],
               let messages = serverProtocol.getProtocol()?.GetMessageSchemeMap(),
               let messageSchema = messages[name] {
                guard messageSchema.request?.count != parameters.count else {
                    throw AvroMessageError.requestParamterCountError
                }
                var i = 0
                for r in messageSchema.request! {
                    let d = try? avro.encodeFrom(parameters[i], schema: r)
                    data.append(d!)
                    i+=1
                }
            }
        } else {
            return data
        }
        return data
    }
    
    public func readRequest<T:Codable>(from: Data)throws -> ([String: [UInt8]]?, String?, [T]) {
        let metaSchema = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
        let (meta, nameIndex) = try! avro.decodeFromContinue(from: from, schema: metaSchema) as ([String: [UInt8]]?,Int)
        let (messageName, paramIndex) = try! avro.decodeFromContinue(from: from.advanced(by: nameIndex), schema: AvroSchema.init(type: "string")) as (String?,Int)
        if messageName == nil {
            return (meta, nil, [])
        }
        var param = [T]()
        if let name = messageName {
            if let serverProtocol = sessionCache[clientRequest.serverHash],
               let messages = serverProtocol.getProtocol()?.GetMessageSchemeMap(),
               let messageSchema = messages[name] {
                var index = paramIndex
                for r in messageSchema.request! {
                    let (p, nextIndex) = try! avro.decodeFromContinue(from: from.advanced(by: index), schema: r) as (T,Int)
                    param.append(p)
                    index = nextIndex
                }
            }
        }
        return (meta, messageName, param)
    }
}
