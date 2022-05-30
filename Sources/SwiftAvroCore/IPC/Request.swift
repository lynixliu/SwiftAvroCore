//
//  Request.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 22/02/22.
//

import Foundation

class MessageRequest {
    let avro: Avro
    let context: Context
    var sessionCache: [[UInt8]: AvroProtocol]
    var clientRequest: HandshakeRequest
    public init(context: Context, clientHash: [UInt8], clientProtocol: String) throws {
        self.avro = Avro()
        self.context = context
        self.sessionCache = [[UInt8]:AvroProtocol]()
        self.clientRequest = HandshakeRequest(clientHash: clientHash, clientProtocol: clientProtocol, serverHash: clientHash,meta: context.handshakeRequestMeta)
        avro.setSchema(schema: context.requestSchema)
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
        let (response, next) = try avro.decodeFromContinue(from: responseData, schema: context.responseSchema) as (HandshakeResponse,Int)
        return (response,responseData.subdata(in: next..<responseData.count))
    }
    
    public func resolveHandshakeResponse(response: HandshakeResponse) throws -> Data? {
        switch response.match {
        case .NONE:
            return try encodeHandshakeRequest(request:HandshakeRequest(clientHash: clientRequest.clientHash, clientProtocol: clientRequest.clientProtocol, serverHash: response.serverHash!))
        case .CLIENT:
            try addSession(hash:response.serverHash!, protocolString: response.serverProtocol!)
            return nil
        default:
            return nil
        }
    }
    
    public func decodeResponseData<T: Codable>(header: HandshakeResponse, massageName:String, requestName:String, responseData: Data) throws -> T {
        guard let _ = header.serverHash else {
            throw AvroHandshakeError.noServerHash
        }
        if header.serverHash == clientRequest.clientHash {
            return try avro.decodeFrom(from: responseData, schema: context.responseSchema)
        }
        guard let p = sessionCache[header.serverHash!] else {
            throw AvroHandshakeError.noServerHash
        }
        let requestSchema = p.getResponse(messageName: massageName)
        return try avro.decodeFrom(from: responseData, schema: requestSchema!)
    }
    
    public func addSession(hash: [UInt8], protocolString: String) throws {
        sessionCache[hash] = try JSONDecoder().decode(AvroProtocol.self, from: protocolString.data(using: .utf8)!)
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
    public func writeRequest<T:Codable>(messageName: String?, parameters: [T]) throws -> Data {
        var data = Data()
        if let name = messageName {
            let d = try? avro.encodeFrom(context.requestMeta, schema: context.metaSchema)
            data.append(d!)
            let n = try? avro.encodeFrom(name, schema: AvroSchema.init(type: "string"))
            data.append(n!)
            if let serverProtocol = sessionCache[clientRequest.serverHash],
               let schemas = serverProtocol.getRequest(messageName: name) {
                var i = 0
                for r in schemas {
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
    
    // two ways message requires response
    public func readResponse<T:Codable>(header: HandshakeRequest, messageName: String, from: Data)throws -> ([String: [UInt8]]?, Bool, [T]) {
        let metaSchema = avro.decodeSchema(schema: MessageConstant.metadataSchema)!
        let (meta, nameIndex) = try! avro.decodeFromContinue(from: from, schema: metaSchema) as ([String: [UInt8]]?,Int)
        let (flag, paramIndex) = try! avro.decodeFromContinue(from: from.advanced(by: nameIndex), schema: AvroSchema.init(type: "boolean")) as (Bool,Int)
        var param = [T]()
        if flag {
            if let serverProtocol = sessionCache[header.serverHash],
               let errorSchemas = serverProtocol.getErrors(messageName: messageName) {
                var index = paramIndex
                for (_,e) in errorSchemas {
                    let (p, nextIndex) = try! avro.decodeFromContinue(from: from.advanced(by: index), schema: e) as (T,Int)
                    param.append(p)
                    index = nextIndex
                }
            }
            return (meta, flag, param)
        }
        if let serverProtocol = sessionCache[header.serverHash],
           let responeSchemas = serverProtocol.getResponse(messageName: messageName) {
             let p = try! avro.decodeFrom(from: from.advanced(by: paramIndex), schema: responeSchemas) as T
                param.append(p)
        }
        return (meta, flag, param)
    }
}
