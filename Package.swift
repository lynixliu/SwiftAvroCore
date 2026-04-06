// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "SwiftAvroCore",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
    ],
    products: [
        .library(
            name: "SwiftAvroCore",
            targets: ["SwiftAvroCore"]),
    ],
    targets: [
        .target(name: "SwiftAvroCore"),
        .testTarget(
            name: "SwiftAvroCoreTests",
            dependencies: ["SwiftAvroCore"]),
    ]
)
