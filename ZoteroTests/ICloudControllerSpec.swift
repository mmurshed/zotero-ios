//
//  ICloudControllerSpec.swift
//  ZoteroTests
//
//  Created by Claude on 31.05.2026.
//  Copyright © 2026 Corporation for Digital Scholarship. All rights reserved.
//

import Foundation

@testable import Zotero

import Nimble
import RealmSwift
import RxSwift
import Quick

/// In-memory `ICloudTransport` double. Models the container as a `[name: Data]` map plus a set of "materialized" names so eviction,
/// slow/failed materialization and coordination outcomes can be simulated without touching real iCloud.
final class MockICloudTransport: ICloudTransport {
    enum MaterializeBehavior {
        case immediate                 // contents already present
        case progressThenComplete      // emit a couple of progress ticks, then complete
        case fail(Error)               // never materializes
    }

    enum TestError: Swift.Error {
        case missingItem
        case forced
    }

    var accountAvailable = true
    var containerAvailable = true
    var materializeBehavior: MaterializeBehavior = .progressThenComplete

    private(set) var store: [String: Data] = [:]
    private var downloaded: Set<String> = []
    private let baseDir: URL

    init() {
        baseDir = FileManager.default.temporaryDirectory.appendingPathComponent("MockICloud-" + UUID().uuidString)
        try? FileManager.default.createDirectory(at: baseDir, withIntermediateDirectories: true)
    }

    var isAccountAvailable: Bool { return accountAvailable }

    func storageDirectory() throws -> URL {
        guard accountAvailable else { throw ICloudTransportController.Error.accountUnavailable }
        guard containerAvailable else { throw ICloudTransportController.Error.containerUnavailable }
        return baseDir
    }

    func url(forItemNamed name: String) throws -> URL {
        return try storageDirectory().appendingPathComponent(name)
    }

    func isDownloaded(name: String) -> Bool { return downloaded.contains(name) }

    func write(data: Data, toItemNamed name: String) -> Single<()> {
        return Single.create { [weak self] subscriber in
            self?.store[name] = data
            self?.downloaded.insert(name)
            subscriber(.success(()))
            return Disposables.create()
        }
    }

    func readData(fromItemNamed name: String) -> Single<Data?> {
        return Single.just(store[name])
    }

    func copy(localFile: URL, toItemNamed name: String) -> Single<()> {
        return Single.create { [weak self] subscriber in
            do {
                let data = try Data(contentsOf: localFile)
                self?.store[name] = data
                self?.downloaded.insert(name)
                subscriber(.success(()))
            } catch let error {
                subscriber(.failure(error))
            }
            return Disposables.create()
        }
    }

    func copyItem(named name: String, to localFile: URL) -> Single<()> {
        return Single.create { [weak self] subscriber in
            guard let data = self?.store[name] else {
                subscriber(.failure(TestError.missingItem))
                return Disposables.create()
            }
            do {
                if FileManager.default.fileExists(atPath: localFile.path) {
                    try FileManager.default.removeItem(at: localFile)
                }
                try FileManager.default.createDirectory(at: localFile.deletingLastPathComponent(), withIntermediateDirectories: true)
                try data.write(to: localFile)
                subscriber(.success(()))
            } catch let error {
                subscriber(.failure(error))
            }
            return Disposables.create()
        }
    }

    func remove(itemNamed name: String) -> Single<Bool> {
        return Single.create { [weak self] subscriber in
            let existed = self?.store[name] != nil
            self?.store[name] = nil
            self?.downloaded.remove(name)
            subscriber(.success(existed))
            return Disposables.create()
        }
    }

    func listItemNames() -> Single<[String]> {
        return Single.just(Array(store.keys))
    }

    func materialize(name: String, queue: DispatchQueue) -> Observable<Progress> {
        switch materializeBehavior {
        case .immediate:
            downloaded.insert(name)
            let progress = Progress(totalUnitCount: 100)
            progress.completedUnitCount = 100
            return Observable.just(progress)

        case .progressThenComplete:
            return Observable.create { [weak self] subscriber in
                let mid = Progress(totalUnitCount: 100)
                mid.completedUnitCount = 50
                subscriber.onNext(mid)
                self?.downloaded.insert(name)
                let done = Progress(totalUnitCount: 100)
                done.completedUnitCount = 100
                subscriber.onNext(done)
                subscriber.onCompleted()
                return Disposables.create()
            }

        case .fail(let error):
            return Observable.error(error)
        }
    }
}

final class ICloudControllerSpec: QuickSpec {
    override class func spec() {
        describe("an iCloud controller") {
            let myLibrary = LibraryIdentifier.custom(.myLibrary)
            var realm: Realm!
            var dbStorage: DbStorage!
            var transport: MockICloudTransport!
            var controller: ICloudController!
            var disposeBag: DisposeBag!

            // Creates a real attachment file on disk so `prepareForUpload` can zip it.
            func makeAttachmentFile(key: String, contents: String) -> File {
                let file = Files.attachmentFile(in: myLibrary, key: key, filename: "doc.txt", contentType: "text/plain")
                try? FileManager.default.createDirectory(at: file.createUrl().deletingLastPathComponent(), withIntermediateDirectories: true)
                try? contents.data(using: .utf8)!.write(to: file.createUrl())
                return file
            }

            beforeEach {
                let config = Realm.Configuration(inMemoryIdentifier: UUID().uuidString)
                realm = try! Realm(configuration: config)
                _ = realm // retain
                dbStorage = RealmDbStorage(config: config)
                transport = MockICloudTransport()
                controller = ICloudController(dbStorage: dbStorage, fileStorage: TestControllers.fileStorage, transport: transport)
                disposeBag = DisposeBag()
                Defaults.shared.iCloudVerified = false
            }

            describe("verification") {
                it("fails when no iCloud account is signed in") {
                    transport.accountAvailable = false
                    waitUntil(timeout: .seconds(10)) { done in
                        controller.verify(queue: .main)
                            .subscribe(onSuccess: { _ in fail("verify should not succeed without an account"); done() },
                                       onFailure: { error in
                                           expect(error as? ICloudError.Verification).to(equal(.accountUnavailable))
                                           expect(controller.isVerified).to(beFalse())
                                           done()
                                       })
                            .disposed(by: disposeBag)
                    }
                }

                it("succeeds and marks verified when the container is writable") {
                    waitUntil(timeout: .seconds(10)) { done in
                        controller.verify(queue: .main)
                            .subscribe(onSuccess: { _ in
                                expect(controller.isVerified).to(beTrue())
                                done()
                            }, onFailure: { error in fail("verify failed: \(error)"); done() })
                            .disposed(by: disposeBag)
                    }
                }
            }

            describe("prepareForUpload") {
                it("returns .new when no remote metadata exists") {
                    let key = "AAAAAAAA"
                    let file = makeAttachmentFile(key: key, contents: "hello")
                    waitUntil(timeout: .seconds(10)) { done in
                        controller.prepareForUpload(key: key, mtime: 100, hash: "abc", file: file, queue: .main)
                            .subscribe(onSuccess: { result in
                                switch result {
                                case .new: break
                                case .exists: fail("expected .new for a fresh key")
                                }
                                done()
                            }, onFailure: { error in fail("\(error)"); done() })
                            .disposed(by: disposeBag)
                    }
                }

                it("round-trips a zip and reports .exists on the second pass (no-op)") {
                    let key = "BBBBBBBB"
                    let file = makeAttachmentFile(key: key, contents: "round-trip")

                    waitUntil(timeout: .seconds(15)) { done in
                        controller.prepareForUpload(key: key, mtime: 200, hash: "hash200", file: file, queue: .main)
                            .flatMap { result -> Single<File> in
                                guard case .new(let zip) = result else { return .error(MockICloudTransport.TestError.forced) }
                                return controller.upload(key: key, file: zip, mtime: 200, hash: "hash200", queue: .main).map { _ in zip }
                            }
                            .flatMap { zip in
                                controller.finishUpload(key: key, result: .success((200, "hash200")), file: zip, queue: .main)
                            }
                            .flatMap { _ in
                                // Second pass with identical mtime+hash must be a no-op.
                                controller.prepareForUpload(key: key, mtime: 200, hash: "hash200", file: file, queue: .main)
                            }
                            .subscribe(onSuccess: { result in
                                switch result {
                                case .exists:
                                    expect(transport.store[key + ".zip"]).toNot(beNil())
                                    expect(transport.store[key + ".prop"]).toNot(beNil())
                                case .new:
                                    fail("second pass with identical metadata should be .exists")
                                }
                                done()
                            }, onFailure: { error in fail("\(error)"); done() })
                            .disposed(by: disposeBag)
                    }
                }

                it("returns .new when the remote hash differs") {
                    let key = "CCCCCCCC"
                    let file = makeAttachmentFile(key: key, contents: "changed")

                    waitUntil(timeout: .seconds(15)) { done in
                        controller.prepareForUpload(key: key, mtime: 1, hash: "old", file: file, queue: .main)
                            .flatMap { result -> Single<File> in
                                guard case .new(let zip) = result else { return .error(MockICloudTransport.TestError.forced) }
                                return controller.upload(key: key, file: zip, mtime: 1, hash: "old", queue: .main).map { _ in zip }
                            }
                            .flatMap { zip in controller.finishUpload(key: key, result: .success((1, "old")), file: zip, queue: .main) }
                            .flatMap { _ in
                                // Same key, different hash => content changed => .new again.
                                controller.prepareForUpload(key: key, mtime: 2, hash: "new", file: file, queue: .main)
                            }
                            .subscribe(onSuccess: { result in
                                switch result {
                                case .new: break
                                case .exists: fail("a changed hash should produce .new")
                                }
                                done()
                            }, onFailure: { error in fail("\(error)"); done() })
                            .disposed(by: disposeBag)
                    }
                }
            }

            describe("download") {
                it("materializes the zip into the local <key>.zip file") {
                    let key = "DDDDDDDD"
                    let file = Files.attachmentFile(in: myLibrary, key: key, filename: "doc.txt", contentType: "text/plain")
                    let localZip = file.copy(withExt: "zip")
                    try? FileManager.default.removeItem(at: localZip.createUrl())
                    // Seed the container with zip bytes and simulate an evicted placeholder.
                    transport.store[key + ".zip"] = "zipbytes".data(using: .utf8)!
                    transport.materializeBehavior = .progressThenComplete

                    var progressSeen = false
                    waitUntil(timeout: .seconds(10)) { done in
                        controller.download(key: key, file: file, queue: .main)
                            .subscribe(onNext: { _ in progressSeen = true },
                                       onError: { error in fail("\(error)"); done() },
                                       onCompleted: {
                                           expect(progressSeen).to(beTrue())
                                           expect(FileManager.default.fileExists(atPath: localZip.createUrl().path)).to(beTrue())
                                           let data = try? Data(contentsOf: localZip.createUrl())
                                           expect(data).to(equal("zipbytes".data(using: .utf8)))
                                           done()
                                       })
                            .disposed(by: disposeBag)
                    }
                }

                it("errors when materialization fails") {
                    let key = "EEEEEEEE"
                    let file = Files.attachmentFile(in: myLibrary, key: key, filename: "doc.txt", contentType: "text/plain")
                    transport.store[key + ".zip"] = "x".data(using: .utf8)!
                    transport.materializeBehavior = .fail(MockICloudTransport.TestError.forced)

                    waitUntil(timeout: .seconds(10)) { done in
                        controller.download(key: key, file: file, queue: .main)
                            .subscribe(onNext: { _ in },
                                       onError: { _ in done() },
                                       onCompleted: { fail("download should have errored"); done() })
                            .disposed(by: disposeBag)
                    }
                }
            }

            describe("delete") {
                it("removes zip and prop and reports succeeded vs missing") {
                    transport.store["FFFFFFFF.zip"] = Data()
                    transport.store["FFFFFFFF.prop"] = Data()
                    // GGGGGGGG has nothing in the container.

                    waitUntil(timeout: .seconds(10)) { done in
                        controller.delete(keys: ["FFFFFFFF", "GGGGGGGG"], queue: .main)
                            .subscribe(onSuccess: { result in
                                expect(result.succeeded).to(contain("FFFFFFFF"))
                                expect(result.missing).to(contain("GGGGGGGG"))
                                expect(transport.store["FFFFFFFF.zip"]).to(beNil())
                                expect(transport.store["FFFFFFFF.prop"]).to(beNil())
                                done()
                            }, onFailure: { error in fail("\(error)"); done() })
                            .disposed(by: disposeBag)
                    }
                }
            }
        }
    }
}
