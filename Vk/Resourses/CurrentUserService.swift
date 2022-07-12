//
//  CurrentUserService.swift
//  Vk
//
//  Created by Табункин Вадим on 31.05.2022.
//

import Foundation
import UIKit

class CurrentUserService: UserService {
    public let currentUser = User(fullName: "Иван", avatar: UIImage(named: "Ivan_Kalita") ?? UIImage(), status: "Век живи, век учись")
    func setUser(fullName: String) -> User? {
        if fullName == currentUser.fullName {
            return currentUser
        } else {
            return nil
        }
    }
}
