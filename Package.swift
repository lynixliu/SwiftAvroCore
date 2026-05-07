// swift-tools-version:6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftAvroCore",
    platforms: [
        .macOS(.v10_13),
        .iOS(.v15)
    ],
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
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
