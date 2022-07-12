//
//  CheckModel.swift
//  Vk
//
//  Created by Табункин Вадим on 20.06.2022.
//

import Foundation
class CheckModel {
    private let word = "Пётр1"
    
    func check(word: String){
        if self.word == word {
            NotificationCenter.default.post(name: NSNotification.Name.greenLable, object: nil)
        } else {
            NotificationCenter.default.post(name: NSNotification.Name.redLable, object: nil)
        }
    }
}

