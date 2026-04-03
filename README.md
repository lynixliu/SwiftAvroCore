[![Swift](https://github.com/STCData/SwiftAvroCore/actions/workflows/swift.yml/badge.svg)](https://github.com/STCData/SwiftAvroCore/actions/workflows/swift.yml)

# SwiftAvroCore

SwiftAvroCore implements the core coding functionality required by Apache Avro™. It supports the Avro 1.8.2 and later specification and provides a user-friendly `Codable` interface (introduced in Swift 4) for encoding and decoding Avro schemas, binary data, and JSON-format data.

It is designed to achieve the following goals:

* provide a small set of core functionalities defined in the Avro specification;
* make software development easier by using the Swift `Codable` interface;
* provide the Avro IPC specification using the Swift `Sendable` interface, which simplifies the client/server implementation;
* provide platform independence and a self-contained framework to enhance portability.

`SwiftAvroCore` provides the coding API for all Swift platforms that include Foundation. File I/O and RPC functions defined in the Avro specification are provided in the separate `SwiftAvroRpc` project, which depends on the swift-nio framework.

## Getting Started

SwiftAvroCore uses SwiftPM as its build tool. To depend on SwiftAvroCore in your own project, add it to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/lynixliu/SwiftAvroCore", branch: "develop")
]
```

Then add `SwiftAvroCore` to your target dependencies:

```swift
.target(
    name: "MyApp",
    dependencies: [
        .product(name: "SwiftAvroCore", package: "SwiftAvroCore")
    ]
)
```

To build and test SwiftAvroCore itself:

```bash
swift build
swift test
```

To generate an Xcode project:

```bash
swift package generate-xcodeproj
open SwiftAvroCore.xcodeproj
```

---

## Using SwiftAvroCore

### Domain model

All examples below share a common model and schema:

```swift
import Foundation
import SwiftAvroCore

struct SensorReading: Codable, Equatable {
    let deviceId:    String
    let timestamp:   Int64    // Unix epoch milliseconds
    let temperature: Double
    let humidity:    Float
    let active:      Bool
}

let sensorReadingSchemaJSON = """
{
  "type": "record",
  "name": "SensorReading",
  "namespace": "com.example",
  "fields": [
    { "name": "deviceId",    "type": "string"  },
    { "name": "timestamp",   "type": "long"    },
    { "name": "temperature", "type": "double"  },
    { "name": "humidity",    "type": "float"   },
    { "name": "active",      "type": "boolean" }
  ]
}
"""
```

---

### Encoding and decoding a record

```swift
let avro = Avro()
let schema = avro.decodeSchema(schema: sensorReadingSchemaJSON)!

let original = SensorReading(
    deviceId: "sensor-007",
    timestamp: 1_714_000_000_000,
    temperature: 23.5,
    humidity: 61.2,
    active: true
)

// Encode to Avro binary
let encoded: Data = try avro.encodeFrom(original, schema: schema)

// Decode back to the Swift type
let decoded: SensorReading = try avro.decodeFrom(from: encoded, schema: schema)
```

`encodeFrom(_:schema:)` and `decodeFrom(from:schema:)` work with any `Codable` type and an `AvroSchema` obtained from `decodeSchema(schema:)`.

---

### Primitive types

Each Avro primitive maps directly to a Swift type:

```swift
let avro = Avro()

// boolean → Bool
let boolSchema = avro.decodeSchema(schema: #"{"type":"boolean"}"#)!
let boolData  = try avro.encodeFrom(true, schema: boolSchema)
let boolBack: Bool = try avro.decodeFrom(from: boolData, schema: boolSchema)

// int → Int32 (zig-zag varint, 1–5 bytes)
let intSchema = avro.decodeSchema(schema: #"{"type":"int"}"#)!
let intData  = try avro.encodeFrom(Int32(-42), schema: intSchema)
let intBack: Int32 = try avro.decodeFrom(from: intData, schema: intSchema)

// long → Int64
let longSchema = avro.decodeSchema(schema: #"{"type":"long"}"#)!
let longData = try avro.encodeFrom(Int64(1_000_000_000_000), schema: longSchema)
let longBack: Int64 = try avro.decodeFrom(from: longData, schema: longSchema)

// string → String
let strSchema = avro.decodeSchema(schema: #"{"type":"string"}"#)!
let strData  = try avro.encodeFrom("Hello, Avro!", schema: strSchema)
let strBack: String = try avro.decodeFrom(from: strData, schema: strSchema)

// bytes → [UInt8]
let bytesSchema = avro.decodeSchema(schema: #"{"type":"bytes"}"#)!
let payload: [UInt8] = [0xDE, 0xAD, 0xBE, 0xEF]
let bytesData = try avro.encodeFrom(payload, schema: bytesSchema)
let bytesBack: [UInt8] = try avro.decodeFrom(from: bytesData, schema: bytesSchema)
```

---

### Nested records, maps, and nullable unions

```swift
struct SensorEvent: Codable, Equatable {
    let eventId: Int32
    let reading: SensorReading
    let tags:    [String: Int32]  // map<int>
    let notes:   String?          // union [null, string]
}

let sensorEventSchemaJSON = """
{
  "type": "record",
  "name": "SensorEvent",
  "namespace": "com.example",
  "fields": [
    { "name": "eventId", "type": "int" },
    {
      "name": "reading",
      "type": {
        "type": "record", "name": "SensorReading",
        "fields": [
          { "name": "deviceId",    "type": "string"  },
          { "name": "timestamp",   "type": "long"    },
          { "name": "temperature", "type": "double"  },
          { "name": "humidity",    "type": "float"   },
          { "name": "active",      "type": "boolean" }
        ]
      }
    },
    { "name": "tags",  "type": { "type": "map", "values": "int" } },
    { "name": "notes", "type": ["null", "string"], "default": null }
  ]
}
"""

let schema = avro.decodeSchema(schema: sensorEventSchemaJSON)!

let event = SensorEvent(
    eventId: 42,
    reading: SensorReading(deviceId: "sensor-042", timestamp: 1_714_100_000_000,
                           temperature: 19.8, humidity: 55.0, active: false),
    tags: ["floor": 3, "building": 1],
    notes: "Routine check"
)

let encoded = try avro.encodeFrom(event, schema: schema)
let decoded: SensorEvent = try avro.decodeFrom(from: encoded, schema: schema)
```

A `nil` optional encodes as the `null` branch of the union:

```swift
let eventNoNotes = SensorEvent(eventId: 99, reading: event.reading, tags: [:], notes: nil)
let encodedNil = try avro.encodeFrom(eventNoNotes, schema: schema)
let decodedNil: SensorEvent = try avro.decodeFrom(from: encodedNil, schema: schema)
// decodedNil.notes == nil
```

---

### Arrays and maps as top-level values

```swift
// Array of longs
let arraySchema = avro.decodeSchema(schema: #"{"type":"array","items":"long"}"#)!
let numbers: [Int64] = [10, 20, 30, 40, 50]
let arrayData   = try avro.encodeFrom(numbers, schema: arraySchema)
let numbersBack: [Int64] = try avro.decodeFrom(from: arrayData, schema: arraySchema)

// Map of string → double
let mapSchema = avro.decodeSchema(schema: #"{"type":"map","values":"double"}"#)!
let metrics: [String: Double] = ["cpu": 0.42, "mem": 0.71, "disk": 0.15]
let mapData     = try avro.encodeFrom(metrics, schema: mapSchema)
let metricsBack: [String: Double] = try avro.decodeFrom(from: mapData, schema: mapSchema)
```

---

### Enum

Avro enums are encoded as the zero-based index of their symbol. When decoded schemalessly (to `Any?`) the symbol string is returned.

```swift
let enumSchemaJSON = """
{
  "type": "enum",
  "name": "Severity",
  "symbols": ["INFO", "WARNING", "ERROR", "CRITICAL"]
}
"""
let schema  = avro.decodeSchema(schema: enumSchemaJSON)!
let encoded = try avro.encodeFrom("WARNING", schema: schema)
let decoded = try avro.decodeFrom(from: encoded, schema: schema) as Any?
// decoded == "WARNING"
```

---

### Fixed schema and logical types

```swift
// Plain fixed — 16-byte identifier
let fixedSchema = avro.decodeSchema(schema: #"{"type":"fixed","name":"UUID","size":16}"#)!
let uuid: [UInt8] = Array(repeating: 0xAB, count: 16)
let fixedData = try avro.encodeFrom(uuid, schema: fixedSchema)
let uuidBack: [UInt8] = try avro.decodeFrom(from: fixedData, schema: fixedSchema)

// date — stored as int (days since Unix epoch)
let dateSchema = avro.decodeSchema(schema: #"{"type":"int","logicalType":"date"}"#)!
let days: Int32 = 19_832   // 2024-04-11
let dateData = try avro.encodeFrom(days, schema: dateSchema)
let daysBack: Int32 = try avro.decodeFrom(from: dateData, schema: dateSchema)

// timestamp-millis — stored as long
let tsSchema  = avro.decodeSchema(schema: #"{"type":"long","logicalType":"timestamp-millis"}"#)!
let nowMillis = Int64(Date().timeIntervalSince1970 * 1000)
let tsData    = try avro.encodeFrom(nowMillis, schema: tsSchema)
let tsBack: Int64 = try avro.decodeFrom(from: tsData, schema: tsSchema)
```

---

### Schemaless (Any?) decoding

When the receiving type is unknown at compile time, decode to `Any?`. Records become `[String: Any]`, arrays become `[Any]`, maps become `[String: Any]`, and enums become their symbol string.

```swift
let raw: Any? = try avro.decodeFrom(from: encoded, schema: schema)
if let dict = raw as? [String: Any] {
    print(dict["deviceId"]    as? String ?? "")   // "sensor-007"
    print(dict["temperature"] as? Double ?? 0)    // 23.5
    print(dict["active"]      as? Bool   ?? false) // true
}
```

#### Type mapping for Any? decoding

| Avro type | Swift type |
|-----------|-----------|
| `null` | `nil` |
| `boolean` | `Bool` |
| `int` | `Int32` |
| `long` | `Int64` |
| `float` | `Float` |
| `double` | `Double` |
| `bytes` | `[UInt8]` |
| `string` | `String` |
| `fixed` | `[UInt8]` (or `[UInt32]` for `duration`) |
| `array` | `[Any]` |
| `record` | `[String: Any]` |
| `enum` | `String` (symbol value) |
| `map` | `[String: Any]` |
| `union` | `Any?` |

---

### JSON encoding mode

Switch between Avro binary (default) and JSON wire format with `setAvroFormat`:

```swift
let avro = Avro()
avro.setAvroFormat(option: .AvroJson)

let schema  = avro.decodeSchema(schema: sensorReadingSchemaJSON)!
let jsonData = try avro.encodeFrom(original, schema: schema)
print(String(data: jsonData, encoding: .utf8)!)

let back: SensorReading = try avro.decodeFrom(from: jsonData, schema: schema)
```

---

### Schema serialisation

Encode a parsed `AvroSchema` back to JSON in three forms:

```swift
let schema = avro.decodeSchema(schema: sensorReadingSchemaJSON)!

// Canonical form — compact, deterministic (default)
avro.setSchemaFormat(option: .CanonicalForm)
let canonical = try avro.encodeSchema(schema: schema)

// Full form — preserves all declared fields
avro.setSchemaFormat(option: .FullForm)
let full = try avro.encodeSchema(schema: schema)

// Pretty-printed form — human-readable indented JSON
avro.setSchemaFormat(option: .PrettyPrintedForm)
let pretty = try avro.encodeSchema(schema: schema)
print(String(data: pretty, encoding: .utf8)!)
```

---

### Object Container File (Avro data files)

`ObjectContainer` implements the Avro Object Container File format: a file header containing the schema and codec, followed by data blocks.

```swift
let codec = NullCodec()

// --- Writing ---
var writer = try ObjectContainer(schema: sensorReadingSchemaJSON, codec: codec)

let readings = (0..<5).map { i in
    SensorReading(deviceId: "sensor-\(i)",
                  timestamp: 1_714_000_000_000 + Int64(i) * 1000,
                  temperature: 20.0 + Double(i),
                  humidity: 50.0 + Float(i),
                  active: i % 2 == 0)
}

// All records in one block
try writer.addObjects(readings)

// Or split into blocks of N records each
try writer.addObjectsToBlocks(readings, objectsInBlock: 2)

let fileBytes = try writer.encodeObject()
try fileBytes.write(to: URL(fileURLWithPath: "/path/to/output.avro"))

// --- Reading ---
var reader = try ObjectContainer(codec: codec)
let data = try Data(contentsOf: URL(fileURLWithPath: "/path/to/output.avro"))
try reader.decodeFromData(from: data)

let recovered: [SensorReading] = try reader.decodeObjects()
```

To decode without a known model type:

```swift
let anyObjects: [Any?] = try reader.decodeObjects()
```

---

### Schema fingerprinting

SwiftAvroCore provides a 64-bit Rabin fingerprint. This is used as the schema identity hash in Avro IPC handshakes.

```swift
let fp = AvroFingerPrint()
let schemaBytes = Array(sensorReadingSchemaJSON.utf8)
let hash: Int64 = fp.fingerPrint64(schemaBytes)
```

---

### RPC (IPC framing and handshake)

SwiftAvroCore implements the Avro IPC protocol, including handshake negotiation and message framing. For complete working examples see [`Tests/SwiftAvroCoreTests/AvroRequestResponseTest.swift`](Tests/SwiftAvroCoreTests/AvroRequestResponseTest.swift).

A minimal sketch of the client/server flow:

```swift
let helloProtocol = """
{
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "types": [
    {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
    {"name": "Curse",    "type": "error",  "fields": [{"name": "message", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request":  [{"name": "greeting", "type": "Greeting"}],
      "response": "Greeting",
      "errors":   ["Curse"]
    }
  }
}
"""

let clientHash: MD5Hash = Array(repeating: 0x01, count: 16)
let serverHash: MD5Hash = Array(repeating: 0x02, count: 16)
let context = Context(requestMeta: [:], responseMeta: [:])

let server = try MessageResponse(context: context, serverHash: serverHash, serverProtocol: helloProtocol)
let client = try MessageRequest(context: context, clientHash: clientHash, clientProtocol: helloProtocol)

// Handshake
let handshakeReq = try client.encodeHandshakeRequest(
    HandshakeRequest(clientHash: serverHash, clientProtocol: helloProtocol, serverHash: serverHash)
)
try client.addSession(hash: serverHash, protocolString: helloProtocol)
let (requestHandshake, _) = try server.resolveHandshakeRequest(from: handshakeReq)

// Client sends a request message
struct Greeting: Codable { var message: String }
let msgData = try client.writeRequest(messageName: "hello", parameters: [Greeting(message: "hi")])

// Server reads request and writes response
let (header, _) = try server.readRequest(header: requestHandshake, from: msgData) as (RequestHeader, [Greeting])
let resData = try server.writeResponse(header: requestHandshake, messageName: header.name,
                                        parameter: Greeting(message: "hello back"))

// Client reads response
let (_, response) = try client.readResponse(header: requestHandshake, messageName: "hello",
                                             from: resData) as (ResponseHeader, [Greeting])
print(response.first?.message ?? "")  // "hello back"

// IPC framing
var raw = Data([1, 2, 3, 4, 5])
raw.framing(frameLength: 4)       // wraps into length-prefixed frames
let frames = raw.deFraming()      // [Data([1,2,3,4]), Data([5])]
```

---

## License

This software is licensed under the Apache 2.0 License and the Anti-996 License.

- https://github.com/lynixliu/SwiftAvroCore/blob/master/LICENSE.txt
- https://github.com/996icu/996.ICU/blob/master/LICENSE

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

---

## FAQ

### Why does this framework provide neither code generation nor dynamic types?

Many serialisation systems (Thrift, Protocol Buffers, CORBA) use IDLs to generate code. Avro also provides an IDL for static languages. However, code generation is inflexible when message formats change frequently, especially in cross-team environments. Avro stores data alongside its schema, so serialisation and deserialisation are possible without code generation.

Dynamic instances (as used by Python, Ruby, and JavaScript Avro libraries) are opaque without checking the schema definition — setting values resembles assembly language and is hard to maintain.

This project uses neither approach. Thanks to the `Codable` feature introduced in Swift 4, SwiftAvroCore provides a type-safe, easy-to-use interface. You can derive schemas from Swift structs on the fly, encode and decode without writing IDL or JSON schemas, and catch type mismatches at compile time. No code generation, no dynamic black boxes — just Swift structs.

### Why is there no file I/O or RPC in the core package?

File I/O and RPC features such as deflate compression depend on platform-specific libraries, while the encoding features depend only on Foundation (required by the Swift runtime). Keeping the core features in a standalone framework makes it more portable. File I/O and RPC are provided in the separate `SwiftAvroRpc` project, which depends on the swift-nio framework.
