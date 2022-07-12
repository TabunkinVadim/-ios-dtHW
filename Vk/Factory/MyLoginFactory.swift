//
//  MyLoginFactory.swift
//  Vk
//
//  Created by Табункин Вадим on 13.06.2022.
//

import Foundation

class MyLoginFactory: LoginFactory {
    func getLoginChek() -> LoginInspector {
        return LoginInspector()
    }
    
}
