# SwiftAvroCore

The SwiftAvroCore framework implements the core coding functionalities that is required  in Apache Avro™ 1.8.2 Specification. It provides user friendly Codable interface introduced from Swift 4 to encode and decode Avro schema, binray data as well as the JSON format data.

It is designed with these goals in mind:

* Provide a small set of core functionalities defined in Avro specification.
* Make software development easier by introducing Codable interface.
* Provide a level of platform independence, and self contained framework to enhance portability.

This project, `SwiftAvroCore`, provides an implementation of the coding API for all swift platforms provide Foundation framework. The file IO and RPC functions defined in Avro specification will be privided in a seperate project `SwiftAvroRpc` which depmends on the swift-nio framework.

## Getting Started

SwiftAvroCore uses SwiftPM as its build tool. If you want to depend on SwiftAvroCore in your own project, it's as simple as adding a dependencies clause to your Package.swift:

dependencies: [
.package(url: "https://github.com/lynixliu/SwiftAvroCore")
]
and then adding the appropriate SwiftAvroCore module to your target dependencies.

To work on SwiftAvroCore itself, or to investigate some of the demonstration applications, you can clone the repository directly and use SwiftPM to help build it. For example, you can run the following commands to compile and run the example echo server:

swift build
swift test

To generate an Xcode project to work on SwiftAvroCore in Xcode:

swift package generate-xcodeproj
This generates an Xcode project using SwiftPM. You can open the project with:

open SwiftAvroCore.xcodeproj


## Using SwiftAvroCore

Here is a simple `main.swift` file which uses SwiftAvroCore. This guide assumes you have already installed a version of the latest [Swift binary distribution](https://swift.org/download/#latest-development-snapshots).

```swift
import Foundation
import SwiftAvroCore

// Define your model in Swift
struct Model: Encodable {
    let requestId: Int32 = 1
    let requestName: String = "hello"
    let parameter: [Int32] = []
}

// Make an Avro instance
let avro = Avro()
let myModel = Model(requestId: 42, requestName: "hello", parameter: [1,2])

```
Define your schema in JSON format like below:
```
// The JSON schema
let jsonSchema = """
{"type":"record",
"fields":[
{"name": "requestId", "type": "int"},
{"name": "requestName", "type": "string"},
{"name": "parameter", "type": {"type":"array", "items": "int"}}
]}
"""

// Decode schema
let schema = try avro.decodeSchema(schema: jsonSchema)

// encode to avro binray
let binaryValue = try! Avro.encode(myModel, schema: schema)

// decode from avro binary
let decodedValue = try! Avro.decode(Model.self, from: binaryValue)

// check result
print("\(encodedValue.requestId), \(encodedValue.requestName), \(encodedValue.parameter)")
```
Generate JSON schema for out side
```
let encodedSchema = try avro.encodeSchema(schema: schema)

print(String(bytes: encodedSchema!, encoding: .utf8)!)
```

## FAQ

### Why this framework provided neigther code generation nor dynamic type?
There are a lot of serialization systems like Thrift, Protocol Buffers, or CORBA use the interface description languages (IDLs) to generate the code for user. Avro also provides an IDL to do so for static language such as C/C++ or Java. But code generation is not flexiable for changing the message format frequently especially the project is in a cross team developing environment. Data in Avro is always stored with its corresponding schema, meaning we can always read a serialized item, regardless of whether we know the schema ahead of time. This allows us to perform serialization and deserialization without code generation. 
The most dynamic language implementation like Pyhon, ruby, javascript library do not support code generation but provide dynamic instance. But the dynamic instance itself is a blackbox for the user without checking the schema defination. There is no key name for the value, just value type. Set value to the dynamic instance looks more like assembly language which is hard to maintain without a lot of comments.  
This project provides neither of them because both of them are not elegent and simplicity. Thanks for the Codable feature introduced in Swift 4, allowing SwiftAvroCore provides a more easy to use and type safty interface for programmers. You can not only generate the schema from your Swift structure on the fly with Codable feature by JSONDecoder, but also encode/decode your data from Swift structure with Avro Codable feature by encoder/decoder. No need to write IDL or JSON schema, no need to generate code and no need to add extra comments or remenber the key name. Besides, the Codable interface is also type safe, so you can locate bugs easilly.
That is it, enjoy the simplicity :)

### Why there is no  file IO and  RPC?
Because the file IO and RPC feature such as deflate depend on some specific platform and library. While the encoding feature depend nearly nothing except for Foundation which also required for swift runtime. So wrap the core features as a standalong framework is more portable and useful than a combo one.  
Don't worry, File IO and RPC will be provided in another open source project `SwiftAvroRpc` licensed in Apache 2.0 which depmends on the swift-nio framework,  coming soon.




