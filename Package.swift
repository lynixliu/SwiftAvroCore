// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "SwiftAvroCore",
    platforms: [
        .macOS(.v15),
        .iOS(.v18)
    ],
    products: [
        .library(name: "SwiftAvroCore",    targets: ["SwiftAvroCore"]),
        .library(name: "SwiftAvroRpc",     targets: ["SwiftAvroRpc"]),
        .library(name: "SwiftAvroCluster", targets: ["SwiftAvroCluster"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git",               from: "2.82.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git",           from: "2.28.0"),
        .package(url: "https://github.com/apple/swift-crypto.git",            from: "3.0.0"),
        .package(url: "https://github.com/apple/swift-distributed-actors.git",branch: "main"),
    ],
    targets: [
        // MARK: - SwiftAvroCore
        .target(name: "SwiftAvroCore"),
        .testTarget(
            name: "SwiftAvroCoreTests",
            dependencies: ["SwiftAvroCore"]),

        // MARK: - SwiftAvroRpc
        .target(
            name: "SwiftAvroRpc",
            dependencies: [
                "SwiftAvroCore",
                .product(name: "NIO",     package: "swift-nio"),
                .product(name: "NIOHTTP1",package: "swift-nio"),
                .product(name: "NIOSSL",  package: "swift-nio-ssl"),
                .product(name: "Crypto",  package: "swift-crypto",
                         condition: .when(platforms: [.linux])),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "SwiftAvroRpcTests",
            dependencies: ["SwiftAvroRpc"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),

        // MARK: - SwiftAvroCluster
        .target(
            name: "SwiftAvroCluster",
            dependencies: [
                "SwiftAvroRpc",
                .product(name: "DistributedCluster", package: "swift-distributed-actors"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "SwiftAvroClusterTests",
            dependencies: ["SwiftAvroCluster"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
