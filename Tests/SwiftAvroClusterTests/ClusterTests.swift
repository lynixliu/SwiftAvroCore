import Testing
import Foundation
import SwiftAvroRpc
@testable import SwiftAvroCluster

// MARK: - ServiceRegistry

@Suite("ServiceRegistry")
struct ServiceRegistryTests {

    func makeSystem(port: Int) async throws -> ClusterNode {
        try await ClusterNode(host: "127.0.0.1", port: port)
    }

    @Test("Register and discover a service")
    func registerAndDiscover() async throws {
        let node = try await makeSystem(port: 9710)
        defer { Task { try await node.shutdown() } }

        let registry = ServiceRegistry(actorSystem: await node.actorSystem)
        let info = ServiceInfo(
            name: "greeter", version: "1.0",
            endpoint: .tcp(host: "127.0.0.1", port: 9090), nodeID: "127.0.0.1:9710"
        )
        try await registry.register(info)
        let found = try await registry.discover(serviceName: "greeter")
        #expect(found == [info])
    }

    @Test("Discover returns empty for unknown service")
    func discoverUnknown() async throws {
        let node = try await makeSystem(port: 9711)
        defer { Task { try await node.shutdown() } }

        let registry = ServiceRegistry(actorSystem: await node.actorSystem)
        let found = try await registry.discover(serviceName: "unknown")
        #expect(found.isEmpty)
    }

    @Test("Deregister removes all endpoints for a node")
    func deregister() async throws {
        let node = try await makeSystem(port: 9712)
        defer { Task { try await node.shutdown() } }

        let registry = ServiceRegistry(actorSystem: await node.actorSystem)
        let i1 = ServiceInfo(name: "svc", version: "1.0",
                             endpoint: .tcp(host: "a", port: 1), nodeID: "n1")
        let i2 = ServiceInfo(name: "svc", version: "1.0",
                             endpoint: .tcp(host: "b", port: 2), nodeID: "n2")
        try await registry.register(i1)
        try await registry.register(i2)
        try await registry.deregister(nodeID: "n1")
        let found = try await registry.discover(serviceName: "svc")
        #expect(found == [i2])
    }

    @Test("ServiceRegistry conforms to ServiceCatalog")
    func conformsToServiceCatalog() async throws {
        let node = try await makeSystem(port: 9713)
        defer { Task { try await node.shutdown() } }

        let registry = ServiceRegistry(actorSystem: await node.actorSystem)
        // Type-check: ServiceRegistry should be usable as any ServiceCatalog
        let catalog: any ServiceCatalog = registry
        let info = ServiceInfo(name: "auth", version: "2.0",
                               endpoint: .unix(path: "/tmp/auth.sock"), nodeID: "n1")
        try await catalog.register(info)
        let found = try await catalog.discover(serviceName: "auth")
        #expect(found.count == 1)
    }
}

// MARK: - ClusterNode

@Suite("ClusterNode")
struct ClusterNodeTests {

    @Test("nodeID is formatted as host:port")
    func nodeIDFormat() async throws {
        let node = try await ClusterNode(host: "127.0.0.1", port: 9720)
        defer { Task { try await node.shutdown() } }
        let id = await node.nodeID
        #expect(id == "127.0.0.1:9720")
    }

    @Test("actorSystem is accessible")
    func actorSystemAccessible() async throws {
        let node = try await ClusterNode(host: "127.0.0.1", port: 9721)
        defer { Task { try await node.shutdown() } }
        let system = await node.actorSystem
        #expect(system.name == "SwiftAvroCluster")
    }

    @Test("shutdown completes without error")
    func shutdownClean() async throws {
        let node = try await ClusterNode(host: "127.0.0.1", port: 9722)
        #expect(throws: Never.self) {
            try await node.shutdown()
        }
    }
}

// MARK: - HealthMonitor

@Suite("HealthMonitor")
struct HealthMonitorTests {

    @Test("HealthMonitor initialises with a cluster system and catalogue")
    func initSucceeds() async throws {
        let node = try await ClusterNode(host: "127.0.0.1", port: 9730)
        defer { Task { try await node.shutdown() } }

        let registry = ServiceRegistry(actorSystem: await node.actorSystem)
        let monitor  = HealthMonitor(system: await node.actorSystem, catalogue: registry)
        // If we got here, init succeeded — watch() would suspend indefinitely so we don't call it
        _ = monitor
    }
}
