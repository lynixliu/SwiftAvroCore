import Testing
import Foundation
@testable import SwiftAvroRpc

// MARK: - In-memory ServiceCatalog for testing

actor InMemoryCatalog: ServiceCatalog {
    private var services: [String: [ServiceInfo]] = [:]

    func register(_ info: ServiceInfo) {
        services[info.name, default: []].append(info)
    }

    func discover(serviceName: String) -> [ServiceInfo] {
        services[serviceName] ?? []
    }

    func deregister(nodeID: String) {
        for key in services.keys {
            services[key]?.removeAll { $0.nodeID == nodeID }
        }
    }
}

// MARK: - Fixtures

private let greeterProtocol = """
{
  "protocol": "Greeter",
  "namespace": "test",
  "messages": {
    "hello": {
      "request": [{ "name": "greeting", "type": "string" }],
      "response": "string"
    }
  }
}
"""

private struct GreeterMessage: Codable, Sendable { var greeting: String }

private struct ServiceEchoHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data { requestData }
}

private struct GreeterService: AvroService {
    let avroProtocol = greeterProtocol
    let serviceName  = "greeter"
    let serviceVersion = "1.0.0"
    var handler: any AvroIPCHandler { ServiceEchoHandler() }
}

// MARK: - LoadBalancer

@Suite("LoadBalancer")
struct LoadBalancerTests {

    @Test("Returns nil for empty candidates")
    func emptyReturnsNil() async {
        let lb = LoadBalancer()
        let result = await lb.select(serviceName: "svc", from: [])
        #expect(result == nil)
    }

    @Test("Returns the single candidate when only one exists")
    func singleCandidate() async {
        let lb   = LoadBalancer()
        let info = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "h", port: 1), nodeID: "n1")
        let result = await lb.select(serviceName: "svc", from: [info])
        #expect(result == info)
    }

    @Test("Round-robins across multiple candidates")
    func roundRobin() async {
        let lb = LoadBalancer()
        let a  = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "a", port: 1), nodeID: "n1")
        let b  = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "b", port: 2), nodeID: "n2")
        let candidates = [a, b]
        let r1 = await lb.select(serviceName: "svc", from: candidates)
        let r2 = await lb.select(serviceName: "svc", from: candidates)
        let r3 = await lb.select(serviceName: "svc", from: candidates)
        #expect(r1 == a)
        #expect(r2 == b)
        #expect(r3 == a)
    }

    @Test("Independent cursors per service name")
    func independentCursors() async {
        let lb = LoadBalancer()
        let a  = ServiceInfo(name: "s1", version: "1.0", endpoint: .tcp(host: "a", port: 1), nodeID: "n1")
        let b  = ServiceInfo(name: "s2", version: "1.0", endpoint: .tcp(host: "b", port: 2), nodeID: "n2")
        let r1 = await lb.select(serviceName: "s1", from: [a])
        let r2 = await lb.select(serviceName: "s2", from: [b])
        #expect(r1 == a)
        #expect(r2 == b)
    }
}

// MARK: - InMemoryCatalog

@Suite("InMemoryCatalog")
struct InMemoryCatalogTests {

    @Test("Register and discover a service")
    func registerAndDiscover() async {
        let catalog = InMemoryCatalog()
        let info = ServiceInfo(name: "greeter", version: "1.0", endpoint: .tcp(host: "h", port: 1), nodeID: "n1")
        await catalog.register(info)
        let found = await catalog.discover(serviceName: "greeter")
        #expect(found == [info])
    }

    @Test("Discover returns empty for unknown service")
    func discoverUnknown() async {
        let catalog = InMemoryCatalog()
        let found = await catalog.discover(serviceName: "unknown")
        #expect(found.isEmpty)
    }

    @Test("Deregister removes all endpoints for a node")
    func deregister() async {
        let catalog = InMemoryCatalog()
        let i1 = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "h", port: 1), nodeID: "n1")
        let i2 = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "h", port: 2), nodeID: "n2")
        await catalog.register(i1)
        await catalog.register(i2)
        await catalog.deregister(nodeID: "n1")
        let found = await catalog.discover(serviceName: "svc")
        #expect(found == [i2])
    }
}

// MARK: - InProcessServiceProvider

@Suite("InProcessServiceProvider")
struct InProcessServiceProviderTests {

    @Test("Host registers service in catalogue with inProcess endpoint")
    func hostRegisters() async throws {
        let catalog  = InMemoryCatalog()
        let provider = InProcessServiceProvider(nodeID: "node1")
        try await provider.host(service: GreeterService(), catalogue: catalog)

        let found = await catalog.discover(serviceName: "greeter")
        #expect(found.count == 1)
        if case .inProcess(let id) = found[0].endpoint {
            #expect(id == "node1/greeter")
        } else {
            Issue.record("Expected inProcess endpoint, got \(found[0].endpoint)")
        }
    }

    @Test("callRaw returns data from handler")
    func callRawReturnsData() async throws {
        let catalog  = InMemoryCatalog()
        let provider = InProcessServiceProvider(nodeID: "node1")
        try await provider.host(service: GreeterService(), catalogue: catalog)

        let request  = Data("hello".utf8)
        let response = try await provider.callRaw(
            serviceName: "greeter", messageName: "hello", requestData: request
        )
        #expect(response == request)
    }

    @Test("callRaw throws serviceNotFound for unknown service")
    func callRawUnknownService() async throws {
        let provider = InProcessServiceProvider(nodeID: "node1")
        await #expect(throws: InProcessServiceError.serviceNotFound("missing")) {
            _ = try await provider.callRaw(serviceName: "missing", messageName: "m", requestData: Data())
        }
    }

    @Test("handler(for:) returns nil for unregistered service")
    func handlerForUnknown() async {
        let provider = InProcessServiceProvider(nodeID: "node1")
        let h = await provider.handler(for: "nonexistent")
        #expect(h == nil)
    }

    @Test("handler(for:) returns handler for registered service")
    func handlerForRegistered() async throws {
        let catalog  = InMemoryCatalog()
        let provider = InProcessServiceProvider(nodeID: "node1")
        try await provider.host(service: GreeterService(), catalogue: catalog)
        let h = await provider.handler(for: "greeter")
        #expect(h != nil)
    }
}

// MARK: - ServiceProvider (inProcess endpoint rejection)

@Suite("ServiceProvider")
struct ServiceProviderTests {

    @Test("host throws unsupportedEndpoint for inProcess")
    func inProcessEndpointRejected() async throws {
        let catalog  = InMemoryCatalog()
        let provider = ServiceProvider(nodeID: "node1")
        await #expect(throws: ServiceProviderError.unsupportedEndpoint(.inProcess(id: "x"))) {
            try await provider.host(
                service:   GreeterService(),
                endpoint:  .inProcess(id: "x"),
                catalogue: catalog
            )
        }
    }
}

// MARK: - ServiceClient

@Suite("ServiceClient")
struct ServiceClientTests {

    @Test("call throws noEndpointAvailable when catalogue is empty")
    func noEndpointAvailable() async throws {
        let catalog = InMemoryCatalog()
        let client  = ServiceClient(catalogue: catalog)
        await #expect(throws: ServiceClientError.noEndpointAvailable("greeter")) {
            _ = try await client.call(
                serviceName:    "greeter",
                clientProtocol: greeterProtocol,
                messageName:    "hello",
                parameters:     [GreeterMessage(greeting: "hi")],
                as:             GreeterMessage.self
            )
        }
        try await client.shutdown()
    }
}

// MARK: - Endpoint

@Suite("Endpoint")
struct EndpointTests {

    @Test("Endpoint round-trips through Codable")
    func codableRoundTrip() throws {
        let cases: [Endpoint] = [
            .tcp(host: "localhost", port: 9090),
            .unix(path: "/var/run/svc.sock"),
            .inProcess(id: "node1/greeter"),
        ]
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        for endpoint in cases {
            let data    = try encoder.encode(endpoint)
            let decoded = try decoder.decode(Endpoint.self, from: data)
            #expect(decoded == endpoint)
        }
    }

    @Test("Endpoint is Hashable and can be used as dictionary key")
    func hashable() {
        var dict: [Endpoint: String] = [:]
        dict[.tcp(host: "h", port: 1)] = "a"
        dict[.unix(path: "/sock")]     = "b"
        #expect(dict[.tcp(host: "h", port: 1)] == "a")
        #expect(dict[.unix(path: "/sock")]     == "b")
    }
}

// MARK: - ServiceInfo

@Suite("ServiceInfo")
struct ServiceInfoTests {

    @Test("ServiceInfo round-trips through Codable")
    func codableRoundTrip() throws {
        let info = ServiceInfo(
            name: "auth", version: "2.0",
            endpoint: .tcp(host: "10.0.0.1", port: 8080), nodeID: "n1"
        )
        let data    = try JSONEncoder().encode(info)
        let decoded = try JSONDecoder().decode(ServiceInfo.self, from: data)
        #expect(decoded == info)
    }

    @Test("ServiceInfo is Hashable")
    func hashable() {
        let a = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "h", port: 1), nodeID: "n1")
        let b = ServiceInfo(name: "svc", version: "1.0", endpoint: .tcp(host: "h", port: 1), nodeID: "n1")
        #expect(a == b)
        var set = Set<ServiceInfo>()
        set.insert(a)
        set.insert(b)
        #expect(set.count == 1)
    }
}

// MARK: - AvroServiceDescriptor

@Suite("AvroServiceDescriptor")
struct AvroServiceDescriptorTests {

    @Test("init throws resourceNotFound for missing resource")
    func missingResource() {
        #expect(throws: AvroServiceDescriptorError.resourceNotFound("missing.avpr")) {
            _ = try AvroServiceDescriptor(resource: "missing", in: Bundle.main)
        }
    }

    @Test("resourceNotFound description contains file name")
    func errorDescription() {
        let error = AvroServiceDescriptorError.resourceNotFound("greeter.avpr")
        #expect(error.description.contains("greeter.avpr"))
    }
}
