//
//  MainCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 23.06.2022.
//

import Foundation
import UIKit


final class MainCoordinator: Coordinator {
    static let shared = MainCoordinator(navigationController: UINavigationController())
    
    var childCoordinators =  [Coordinator]()
    var navigationController: UINavigationController

    init(navigationController: UINavigationController) {
        self.navigationController = navigationController
    }

    func start() {
        let loginFactory = MyLoginFactory()
        let checkModel = CheckModel()
        let vc = MainTabBarController(loginCheker: loginFactory.getLoginChek(), checkModel: checkModel)
        vc.coordinator = self
        navigationController.navigationBar.isHidden = true
        navigationController.pushViewController(vc, animated: false)
    }

    func profile(navigationController: UINavigationController, loginCheker: LoginInspector)-> ProfileCoordinator {
        let child = ProfileCoordinator(navigationController: navigationController, loginCheker: loginCheker)
        childCoordinators.append(child)
        child.parentCoordinator = self
        child.start()
        return child
    }

    func feed(navigationController: UINavigationController, checkModel: CheckModel) -> FeedCoordinator{
        let child = FeedCoordinator(navigationController: navigationController, checkModel: checkModel)
        childCoordinators.append(child)
        child.start()
        return child
    }
    func favorite(navigationController: UINavigationController) -> FavoritePostsCoordinator {
        let child = FavoritePostsCoordinator(navigationController: navigationController)
        childCoordinators.append(child)
        child.start()
        return child
    }

    func childDidFinish(_ child:Coordinator?) {
        for (index, coordinator) in childCoordinators.enumerated() {
            if coordinator === child {
                childCoordinators.remove(at: index)
                break
            }
        }
    }
}
