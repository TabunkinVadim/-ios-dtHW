//
//  Coordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 26.06.2022.
//

import Foundation
import UIKit


protocol Coordinator: AnyObject {
    var childCoordinators: [Coordinator] {get set}
    var navigationController: UINavigationController {get set}
    
    func start()
}
