//
//  File.swift
//  
//
//  Created by standard on 3/16/23.
//

import Foundation



extension Data {
    static func random(length: Int) throws -> Data {
        return Data((0 ..< length).map { _ in UInt8.random(in: UInt8.min ... UInt8.max) })
    }
}


struct Kitty: Codable, Equatable {
    enum KittyColor: String, Codable, CaseIterable {
        case Brown
        case White
        case Black
    }
    let name: String
    let color: KittyColor
    
    static func random() -> Self {
        Self(name:[
            "Whiskers",
            "Felix",
            "Oscar",
            "Smudge",
            "Fluffy",
            "Angel",
            "Lady",
            "Lucky"
        ].randomElement()!
             , color:KittyColor.allCases.randomElement()!
        )
    }
}


struct KittyAction: Codable, Equatable {
    enum KittyActionType:  String, Codable, CaseIterable {
        case meow
        case jump
        case bite
    }
    let label: String
    let type: KittyActionType
    let timestamp: Date
    let dataValue: [UInt8]
    let intValue: Int
    let floatValue: Float
    let doubleValue: Double
    let kitty: Kitty
    
    static func random() -> Self {
        Self(label: [
            "yah just kidding",
            "random text",
            "very very very very very very long label",
            "hahahhhahah"
        ].randomElement()!, type: KittyActionType.allCases.randomElement()!,
             timestamp: Date(),
             dataValue: [UInt8.random(in: 1...40), UInt8.random(in: 1...40), UInt8.random(in: 1...40)],
             intValue: Int.random(in: -100...4990),
             floatValue: Float.random(in: -1000...40),
             doubleValue: Double.random(in: -100...40),
             kitty: Kitty.random())
    }
}

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
        
        _ = try! String(decoding: avro.encodeSchema(schema: schemaReflecting), as: UTF8.self)

        
        avro.setSchema(schema: schemaReflecting)
        
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
