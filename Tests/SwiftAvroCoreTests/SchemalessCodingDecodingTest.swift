//
//  SchemalessCodingDecodingTest.swift
//  
//
//  Created by standard on 3/17/23.
//

import Foundation
import XCTest
@testable import SwiftAvroCore

class SchemalessCodingDecodingTest: XCTestCase {

    override func setUp() {
    }

    override func tearDown() {
    }
    func testSchemalessKittensDoubleDecode() {
        let kitten = Kitty.random()
        let avro = Avro()
        let schemaReflecting = AvroSchema.reflecting(kitten)!
        let serializedSchema = try! String(decoding: avro.encodeSchema(schema: schemaReflecting), as: UTF8.self)

        let _ = avro.decodeSchema(schema: serializedSchema)

        
        let binaryValue = try!avro.encode(kitten)
        let kittenDecoded: SchemalessCodable = try! avro.decode(from: binaryValue)
        
        XCTAssertNotNil(kittenDecoded)
//        XCTAssertEqual(kitten, kittenDecoded)
    }
    
    
    func testSchemalessKittenActions() {
        let kittenAction = KittyAction.random()
        let avro = Avro()
        let schemaReflecting = AvroSchema.reflecting(kittenAction)!
        
        let schemaJson = try! String(decoding: avro.encodeSchema(schema: schemaReflecting), as: UTF8.self)

        
        let _ = avro.decodeSchema(schema: schemaJson)
//        XCTAssertEqual(decodedSchema, schemaReflecting)
        
        let binaryValue = try! avro.encode(kittenAction)
        let kittenActionDecoded: SchemalessCodable = try! avro.decode(from: binaryValue)
        
//        XCTAssertEqual(Int(kittenAction.timestamp.timeIntervalSinceReferenceDate), kittenActionDecoded["timestamp"] as! Int, accuracy: 1)
//        XCTAssertEqual(kittenAction.dataValue, kittenActionDecoded["dataValue"] as! [UInt8])
        XCTAssertEqual(kittenAction.label, kittenActionDecoded["label"] as! String)
        XCTAssertEqual(kittenAction.type.rawValue, kittenActionDecoded["type"] as! String)
        XCTAssertEqual(kittenAction.floatValue, kittenActionDecoded["floatValue"] as! Float)
        XCTAssertEqual(kittenAction.doubleValue, kittenActionDecoded["doubleValue"] as! Double)
//        XCTAssertEqual(kittenAction.kitty, kittenActionDecoded["kitty"] as! Kitty)
    }
    

    
    static var allTests = [
        ("testSchemalessKittensDoubleDecode", testSchemalessKittensDoubleDecode),
        ("testSchemalessKittenActions", testSchemalessKittenActions),
        ]
}
