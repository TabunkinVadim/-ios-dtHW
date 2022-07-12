//
//  FeedModel.swift
//  Vk
//
//  Created by Табункин Вадим on 28.06.2022.
//
//import StorageService
import Foundation
import UIKit

class FeedModel {

    weak var coordinator: FeedCoordinator?

    var chengeActiv: ((Action) -> Void)?

    enum Action {
        case tapButton (checkModel: CheckModel ,textField: UITextField)
        case postDetail
    }
    
    func chenge(_ action: Action) {
        switch action {
        case .postDetail:
            coordinator?.postVC()
        case .tapButton(let checkModel,let textField):
            if textField.text != ""{
                checkModel.check(word: textField.text!)
            } else {
                textField.placeholder = "Введите слово"
            }
        }
    }
}

