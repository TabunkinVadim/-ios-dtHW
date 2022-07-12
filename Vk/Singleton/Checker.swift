//
//  Checker.swift
//  Vk
//
//  Created by Табункин Вадим on 07.06.2022.
//

import Foundation

final class Checker {
    
    static let shared = Checker()
    #if DEBUG
    private let login = "Пётр"
    #else
    private let login = "Иван"
    #endif
    private var pswd = "1"
    
    func chek(verifiableLogin: String, verifiablePassword: String) -> Bool {
        if verifiableLogin.hash == login.hash && verifiablePassword.hash == pswd.hash {
            return true
        } else {
            return false
        }
    }

    func randomString(length: Int) -> String {
        let letters = String().printable
        pswd = String((0..<length).map{ _ in letters.randomElement()! })
      return pswd
    }
    
}


