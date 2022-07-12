//
//  User.swift
//  Vk
//
//  Created by Табункин Вадим on 31.05.2022.
//

import Foundation
import UIKit

class User {
    let fullName: String
    let avatar: UIImage
    let status: String
    
    init(fullName: String, avatar: UIImage, status: String) {
        self.fullName = fullName
        self.avatar = avatar
        self.status = status
    }
}
