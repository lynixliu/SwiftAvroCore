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
}

struct Request:Codable {
    let clientHash: [UInt8]
    var clientProtocal: String?
    var serverHash: [UInt8]?
    var meta: [String: [UInt8]]?
}

enum HandshakeMatch:String,Codable {
    case BOTH
    case CLIENT
    case NONE
}

struct Response:Codable {
    let match: HandshakeMatch
    let serverProtocol: String?
    let serverHash: [UInt8]?
    var meta: [String: [UInt8]]?
}

class MessageRequest {
    //.MapSchema(schema: "{\"type\": \"map\", \"values\": \"bytes\"}")
    let avro: Avro
    let longSchema = AvroSchema.IntSchema(isLong:true)//try avro.newSchema(schema: "long")
    var sessionCache: [[UInt8]: AvroSchema]
    var clientRequest: Request
    let responseSchema: AvroSchema
    public init(clientHash: [UInt8], clientProtocol: String) throws {
        self.avro = Avro()
        self.sessionCache = [[UInt8]:AvroSchema]()
        clientRequest = Request(clientHash: clientHash, clientProtocal: clientProtocol, serverHash: nil)
        _ = avro.decodeSchema(schema: MessageConstant.requestSchema)
        responseSchema = avro.newSchema(schema: MessageConstant.responseSchema)!
    }
    
    public func encodeHandshakeRequest(request: Request) throws -> Data {
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
        return try encodeHandshakeRequest(request: Request(clientHash: clientRequest.clientHash, clientProtocal: nil, serverHash: clientRequest.clientHash))
    }
    
    public func decodeResponse(responseData: Data) throws -> (Response, Data) {
        let (response, next) = try avro.decodeFromContinue(from: responseData, schema: responseSchema) as (Response,Int)
        return (response,responseData.subdata(in: next..<responseData.count))
    }
    
    public func resolveHandshakeResponse(response: Response) throws -> Data? {
        switch response.match {
        case .NONE:
            return try encodeHandshakeRequest(request:Request(clientHash: clientRequest.clientHash, clientProtocal: clientRequest.clientProtocal, serverHash: response.serverHash))
        case .CLIENT:
            sessionCache[response.serverHash!] = avro.newSchema(schema:response.serverProtocol!)!
            return nil
        default:
            return nil
        }
    }
    
    public func decodeResponseData<T: Codable>(header: Response, responseData: Data) throws -> T {
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
    
    public func outdateSession(header: Response) {
        sessionCache.removeValue(forKey: header.serverHash!)
    }
    
    public func clearSession() {
        sessionCache.removeAll()
    }
    
}
