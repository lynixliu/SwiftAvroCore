//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//

import Foundation
class MessageResponse {
    let avro: Avro
    var sessionCache: [[uint8]: AvroSchema]
    var serverResponse: Response
    let requestSchema: AvroSchema
    init(avro: Avro, serverHash: [uint8], serverProtocol: String) throws {
        self.avro = avro
        self.sessionCache = [[uint8]:AvroSchema]()
        self.requestSchema = avro.newSchema(schema: MessageConstant.requestSchema)!
        self.serverResponse = Response(match: HandshakeMatch.NONE,serverProtocal: serverProtocol, serverHash: serverHash)
        _ = avro.decodeSchema(schema: MessageConstant.responseSchema)
    }
    
    func encodeHandshakeResponse(response: Response) throws -> Data {
        return try avro.encode(response)
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
    public func resolveHandshakeRequest(requestData: Data) throws -> Data {
        let request = try avro.decodeFrom(from:requestData, schema: requestSchema) as Request
        if sessionCache[request.clientHash] == nil && request.clientHash.count > 0 {
            sessionCache[request.clientHash] = avro.newSchema(schema: request.clientProtocal!)
        }
        if sessionCache[request.clientHash] != nil {
            if request.serverHash == serverResponse.serverHash {
                return try avro.encode(Response(match: HandshakeMatch.BOTH, serverProtocal: nil, serverHash: nil))
            }
            return try avro.encode(Response(match: HandshakeMatch.CLIENT, serverProtocal: serverResponse.serverProtocal, serverHash: serverResponse.serverHash))
        }
        return try avro.encode(Response(match: HandshakeMatch.NONE, serverProtocal: serverResponse.serverProtocal, serverHash: serverResponse.serverHash))
    }
}
