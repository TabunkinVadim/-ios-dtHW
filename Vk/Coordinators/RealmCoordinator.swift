//
//  RealmCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 26.08.2022.
//

import Foundation
import RealmSwift

class RealmCoordinator {
    func create(password: String, email: String) {
        let realm = try! Realm()
        let accaunt = AuthorizationRealmModel()
        accaunt.email = email
        accaunt.password = password
        accaunt.logIn = true
        try! realm.write({
            realm.add(accaunt)
        })
    }

    func get() -> AuthorizationRealmModel? {
        let realm = try! Realm()
        var items: Results<AuthorizationRealmModel>!
        items = realm.objects(AuthorizationRealmModel.self)
        return items.last

    }

    func delete() {
        let realm = try! Realm()
        var items: Results<AuthorizationRealmModel>!
        items = realm.objects(AuthorizationRealmModel.self)
        try! realm.write {
            realm.delete(items)
        }
    }

    func edit(item: AuthorizationRealmModel, isLogIn: Bool) {
        let realm = try! Realm()
        try! realm.write {
            item.logIn = isLogIn
        }
    }

    func getCount() -> Int {
        let realm = try! Realm()
        var items: Results<AuthorizationRealmModel>?
        items = realm.objects(AuthorizationRealmModel.self)
        guard let items = items else {return 0}
        return items.count
    }
}
