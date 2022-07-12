//
//  LoginInspector.swift
//  Vk
//
//  Created by Табункин Вадим on 07.06.2022.
//

import Foundation
class LoginInspector: LoginViewControllerDelegate {

    func chek(login: String, pswd: String) -> Bool {
        return Checker.shared.chek(verifiableLogin: login, verifiablePassword: pswd)
    }
}
