//
//  authorizationModel.swift
//  Vk
//
//  Created by Табункин Вадим on 15.08.2022.
//

import Foundation
import RealmSwift

final class AuthorizationRealmModel: Object {
    @objc dynamic var email: String = ""
    @objc dynamic var password: String = ""
    @objc dynamic var logIn : Bool = false
}
