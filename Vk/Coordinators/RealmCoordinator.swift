//
//  RealmCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 16.08.2022.
//

import Foundation
import RealmSwift

class RealmCoordinator {
    private let backgroundQueue = DispatchQueue(label: "RealmContextQueue", qos: .background)
    private let mainQueue = DispatchQueue.main

    private func safeWrite(in realm: Realm, _ block: (() throws -> Void)) throws {
        realm.isInWriteTransaction
        ? try block()
        : try realm.write(block)
    }


    func create<T>(_ model: T.Type, keyedValues: [[String: Any]], completion: @escaping (Result<[T], DatabaseError>) -> Void) where T : Object {
        self.backgroundQueue.async {
            do {
                let realm = try Realm()

                try self.safeWrite(in: realm) {
                    guard let model = model as? Object.Type else {
                        self.mainQueue.async { completion(.failure(.wrongModel)) }
                        return
                    }

                    var objectsRefs: [ThreadSafeReference<Object>] = []
                    keyedValues.forEach {
                        let newObject = realm.create(model, value: $0, update: .all)
                        let objectRef = ThreadSafeReference(to: newObject)
                        objectsRefs.append(objectRef)
                    }

                    self.mainQueue.async {
                        do {
                            let realmOnMainThread = try Realm()
                            realmOnMainThread.refresh()
                            let objects = objectsRefs.compactMap { realmOnMainThread.resolve($0) }

                            guard let result = objects as? [T] else {
                                completion(.failure(.wrongModel))
                                return
                            }

                            completion(.success(result))
                        } catch {
                            completion(.failure(.error(desription: "Fail to fetch all objects")))
                        }
                    }
                }
            } catch {
                self.mainQueue.async { completion(.failure(.error(desription: "Fail to create object in storage"))) }
            }
        }
    }



    func fetch<T>(_ model: T.Type, predicate: NSPredicate?, completion: @escaping (Result<[T], DatabaseError>) -> Void) where T : Object {
        self.backgroundQueue.async {
            do {
                let realm = try Realm()

                guard let model = model as? Object.Type else {
                    self.mainQueue.async { completion(.failure(.wrongModel)) }
                    return
                }

                var objects = realm.objects(model)
                if let predicate = predicate {
                    objects = objects.filter(predicate)
                }

                let objectsRef = Array(objects).map { ThreadSafeReference(to: $0) }

                self.mainQueue.async {
                    do {
                        let realmOnMainThread = try Realm()
                        realmOnMainThread.refresh()
                        let objects = objectsRef.compactMap { realmOnMainThread.resolve($0) }

                        guard let result = objects as? [T] else {
                            completion(.failure(.wrongModel))
                            return
                        }

                        completion(.success(result))
                    } catch {
                        completion(.failure(.error(desription: "Fail to fetch all objects")))
                    }
                }
            } catch {
                self.mainQueue.async { completion(.failure(.error(desription: "Fail to fetch objects"))) }
            }
        }
    }
}

enum DatabaseError: Error {
    /// Невозможно добавить хранилище.
    case store(model: String)
    /// Не найден momd файл.
    case find(model: String, bundle: Bundle?)
    /// Не найдена модель объекта.
    case wrongModel
    /// Кастомная ошибка.
    case error(desription: String)
    /// Неизвестная ошибка.
    case unknown(error: Error)
}
