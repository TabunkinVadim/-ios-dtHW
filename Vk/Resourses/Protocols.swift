//
//  Protocols.swift
//  Vk
//
//  Created by Табункин Вадим on 31.05.2022.
//

import Foundation

protocol UserService {
    func setUser(fullName:String) -> User?
}
protocol LoginViewControllerDelegate: AnyObject {
    func chek(login: String, pswd: String) -> Bool
    }
protocol LoginFactory{
    func getLoginChek() -> LoginInspector
}
