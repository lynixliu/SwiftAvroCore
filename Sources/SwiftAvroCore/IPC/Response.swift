//
//  Response.swift
//  SwiftAvroCore
//
//  Created by Yang.Liu on 24/02/22.
//

import Foundation

class MessageResponse {
    let avro: Avro
    let context: Context
    var sessionCache: [[UInt8]: AvroProtocol]
    var serverResponse: HandshakeResponse

    public init(context:Context, serverHash: [UInt8], serverProtocol: String) throws {
        self.avro = Avro()
        self.context = context
        self.avro.setSchema(schema: context.responseSchema)
        self.sessionCache = [[UInt8]:AvroProtocol]()
        self.serverResponse = HandshakeResponse(match: HandshakeMatch.NONE,serverProtocol: serverProtocol, serverHash: serverHash, meta: context.responseMeta)
    }
    
    func encodeHandshakeResponse(response: HandshakeResponse) throws -> Data {
        return try avro.encode(response)
    }
    public func addSupportPotocol(protocolString: String, hash: [UInt8]) throws {
        sessionCache[hash] = try JSONDecoder().decode(AvroProtocol.self,from: protocolString.data(using: .utf8)!)
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
    public func resolveHandshakeRequest(requestData: Data) throws -> (HandshakeRequest, Data) {
        let request = try avro.decodeFrom(from:requestData, schema: context.requestSchema) as HandshakeRequest
        if request.clientHash.count != 16 {
            throw AvroHandshakeError.noClientHash
        }
        if let _ = sessionCache[request.clientHash] {
            if request.serverHash != serverResponse.serverHash {
                return (request, try encodeHandshakeResponse(response: HandshakeResponse(match: HandshakeMatch.CLIENT, serverProtocol: serverResponse.serverProtocol, serverHash: serverResponse.serverHash)))
            }
            return (request, try encodeHandshakeResponse(response: HandshakeResponse(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil)))
        }
        if let clientProtocol = request.clientProtocol,request.serverHash == serverResponse.serverHash {
            try addSupportPotocol(protocolString:clientProtocol ,hash:request.clientHash)
            return (request, try encodeHandshakeResponse(response: HandshakeResponse(match: HandshakeMatch.BOTH, serverProtocol: nil, serverHash: nil)))
        }
        // client use this response to retrieve the supported protocol from server
        return (request, try encodeHandshakeResponse(response: HandshakeResponse(match: HandshakeMatch.NONE, serverProtocol: serverResponse.serverProtocol, serverHash: serverResponse.serverHash)))
    }
    
    public func outdateSession(header: HandshakeRequest) {
        sessionCache.removeValue(forKey: header.clientHash)
    }
    
    public func clearSession() {
        sessionCache.removeAll()
    }
    
    /*
     The format of a call response is:
     * response metadata, a map with values of type bytes
     * a one-byte error flag boolean, followed by either:
     ** if the error flag is false, the message response, serialized per the message's response schema.
     ** if the error flag is true, the error, serialized per the message's effective error union schema.
    */
    public func writeResponse<T:Codable>(header: HandshakeRequest, messageName: String, parameter: T) throws -> Data {
        var data = Data()
        if let meta = header.meta {
            let d = try? avro.encodeFrom(meta, schema: context.metaSchema)
            data.append(d!)
        } else {
            data.append(contentsOf: [UInt8(0)])
        }
        
        if let serverProtocol = sessionCache[header.serverHash],
           let responseSchema = serverProtocol.getResponse(messageName: messageName) {
                let flag = try? avro.encodeFrom(false, schema: AvroSchema.init(type: "boolean"))
                data.append(flag!)
                let d = try? avro.encodeFrom(parameter, schema: responseSchema)
                data.append(d!)
        }
        return data
    }
    
    public func writeErrorResponse<T:Codable>(header: HandshakeRequest,messageName: String, errors: [String: T]) throws -> Data {
        var data = Data()
        if let meta = header.meta {
            let d = try? avro.encodeFrom(meta, schema: context.metaSchema)
            data.append(d!)
        } else {
            data.append(contentsOf: [UInt8(0)])
        }
       
        if let serverProtocol = sessionCache[header.serverHash],
           let errorSchemas = serverProtocol.getErrors(messageName: messageName) {
                let flag = try? avro.encodeFrom(true, schema: AvroSchema.init(type: "boolean"))
                data.append(flag!)
                for (k, v) in errors {
                    if let schema = errorSchemas[k] {
                        data.append(contentsOf: [UInt8(2)]) //union 1
                        let d = try? avro.encodeFrom(v, schema: schema)
                        data.append(d!)
                    } else {
                        data.append(contentsOf: [UInt8(0)]) //union 0
                        let d = try? avro.encodeFrom(v, schema: AvroSchema.init(type: "string"))
                        data.append(d!)
                    }
                }
        }
        return data
    }
    
    public func readRequest<T:Codable>(header: HandshakeRequest, from: Data)throws -> (RequestHeader, [T]) {
        let (hasMeta, metaIndex) = try! avro.decodeFromContinue(from: from, schema: AvroSchema.init(type: "int")) as (Int,Int)
        var meta: [String: [UInt8]]?
        var nameIndex = metaIndex
        if hasMeta == 0 {
            meta = nil
        } else {
            (meta, nameIndex) = try! avro.decodeFromContinue(from: from.advanced(by: metaIndex), schema: context.metaSchema) as ([String: [UInt8]]?,Int)
            nameIndex = nameIndex+metaIndex
        }
        let (messageName, paramIndex) = try! avro.decodeFromContinue(from: from.advanced(by: nameIndex), schema: AvroSchema.init(type: "string")) as (String?,Int)
        if let name = messageName {
            var param = [T]()
            if let serverProtocol = sessionCache[header.serverHash],
               let requestSchemas = serverProtocol.getRequest(messageName: name) {
                var index = nameIndex+paramIndex
                for r in requestSchemas {
                    let (p, nextIndex) = try! avro.decodeFromContinue(from: from.advanced(by: index), schema: r) as (T,Int)
                    param.append(p)
                    index = nextIndex
                }
            }
            return (RequestHeader(meta:meta, name:name), param)
        }
        return (RequestHeader(meta:meta, name:""),[])
    }
}
