//
//  TestUserService.swift
//  StorageService
//
//  Created by Табункин Вадим on 05.06.2022.
//

import Foundation
import UIKit

class TestUserService: UserService {
    public let currentUser = User(fullName: "Пётр", avatar: UIImage(named: "Petr_1") ?? UIImage(), status: "Русский тот, кто любит Россию и ей служит")
    func setUser(fullName: String) -> User? {
        if fullName == currentUser.fullName {
            return currentUser
        } else {
            return nil
        }
    }
}
