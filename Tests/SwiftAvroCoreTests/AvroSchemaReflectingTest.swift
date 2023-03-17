//
//  File.swift
//  
//
//  Created by standard on 3/16/23.
//

import Foundation




import XCTest
@testable import SwiftAvroCore
class AvroSchemaReflectingTest: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    func testKittensDoubleDecode() {
        let kitten = Kitty.random()
        let avro = Avro()
        let schemaReflecting = AvroSchema.reflecting(kitten)!
        let serializedSchema = try! String(decoding: avro.encodeSchema(schema: schemaReflecting), as: UTF8.self)

        let _ = avro.decodeSchema(schema: serializedSchema)

        
        // encode to avro binray
        let binaryValue = try!avro.encode(kitten)
        // decode from avro binary
        let kittenDecoded: Kitty = try! avro.decode(from: binaryValue)
        
        XCTAssertEqual(kitten, kittenDecoded)
    }
    
    func testKittensDirectSet() {
        let kitten = Kitty.random()
        let avro = Avro()
        let schemaReflecting = AvroSchema.reflecting(kitten)!
        avro.setSchema(schema: schemaReflecting)
        
        // encode to avro binray
        let binaryValue = try!avro.encode(kitten)
        // decode from avro binary
        let kittenDecoded: Kitty = try! avro.decode(from: binaryValue)
        
        XCTAssertEqual(kitten, kittenDecoded)
    }
    
    func testKittenActions() {
        let kittenAction = KittyAction.random()
        let avro = Avro()
        let schemaReflecting = AvroSchema.reflecting(kittenAction)!
        
        let schemaJson = try! String(decoding: avro.encodeSchema(schema: schemaReflecting), as: UTF8.self)

        
        let _ = avro.decodeSchema(schema: schemaJson)
//        XCTAssertEqual(decodedSchema, schemaReflecting)
        
        // encode to avro binray
        let binaryValue = try!avro.encode(kittenAction)
        // decode from avro binary
        let kittenActionDecoded: KittyAction = try! avro.decode(from: binaryValue)
        
        XCTAssertEqual(kittenAction.dataValue, kittenActionDecoded.dataValue)
        XCTAssertEqual(kittenAction.timestamp.timeIntervalSinceReferenceDate, kittenActionDecoded.timestamp.timeIntervalSinceReferenceDate, accuracy: 1)
        XCTAssertEqual(kittenAction.dataValue, kittenActionDecoded.dataValue)
        XCTAssertEqual(kittenAction.label, kittenActionDecoded.label)
        XCTAssertEqual(kittenAction.type, kittenActionDecoded.type)
        XCTAssertEqual(kittenAction.floatValue, kittenActionDecoded.floatValue)
        XCTAssertEqual(kittenAction.doubleValue, kittenActionDecoded.doubleValue)
        XCTAssertEqual(kittenAction.kitty, kittenActionDecoded.kitty)
    }
    

    
    static var allTests = [
        ("testKittensDoubleDecode", testKittensDoubleDecode),
        ("testKittensDirectSet", testKittensDirectSet),
        ("testKittenActions", testKittenActions),
        ]
}
