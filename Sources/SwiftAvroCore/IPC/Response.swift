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
    public init(serverHash: [uint8], serverProtocol: String) throws {
        self.avro = Avro()
        self.sessionCache = [[uint8]:AvroSchema]()
        self.requestSchema = avro.newSchema(schema: MessageConstant.requestSchema)!
        self.serverResponse = Response(match: HandshakeMatch.NONE,serverProtocol: serverProtocol, serverHash: serverHash)
        _ = avro.decodeSchema(schema: MessageConstant.responseSchema)
    }
    
    func encodeHandshakeResponse(response: Response) throws -> Data {
        return try avro.encode(response)
    }
    public func addSupportPotocol(protocolString: String, hash: [uint8]) throws {
        sessionCache[hash] = avro.newSchema(schema:protocolString)
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
        if request.clientHash.count != 16 {
            throw AvroHandshakeError.noClientHash
        }
        if let _ = sessionCache[request.clientHash] {
            if request.serverHash != serverResponse.serverHash {
                return try encodeHandshakeResponse(response: Response(match: HandshakeMatch.CLIENT, serverProtocol: serverResponse.serverProtocol, serverHash: serverResponse.serverHash))
            }
            return try encodeHandshakeResponse(response: Response(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil))
        }
        if let clientProtocol = request.clientProtocol,request.serverHash == serverResponse.serverHash {
            sessionCache[request.clientHash] = avro.newSchema(schema: clientProtocol)
            return try encodeHandshakeResponse(response: Response(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil))
        }
        // client use this response to retrieve the supported protocol from server
        return try encodeHandshakeResponse(response: Response(match: HandshakeMatch.NONE, serverProtocol: serverResponse.serverProtocol, serverHash: serverResponse.serverHash))
    }
    
    public func outdateSession(header: Request) {
        sessionCache.removeValue(forKey: header.clientHash)
    }
    
    public func clearSession() {
        sessionCache.removeAll()
    }
}
