//
//  ProfileCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 23.06.2022.
//

import Foundation
import UIKit

final class ProfileCoordinator: Coordinator{
    weak var parentCoordinator: MainCoordinator?
    var childCoordinators = [Coordinator]()
    var navigationController: UINavigationController
    let loginCheker: LoginInspector

    init(navigationController: UINavigationController, loginCheker: LoginInspector) {
        self.navigationController = navigationController
        self.loginCheker = loginCheker
    }

    func start() {
        logInVC()
    }
    
    func logInVC() {
        let vc = LogInViewController(loginCheker: loginCheker)
        vc.coordinator = self
        vc.view.backgroundColor = .white
        vc.tabBarItem = UITabBarItem(title: "Profile", image: UIImage(systemName: "person")?.withAlignmentRectInsets(.init(top: 0, left: 0, bottom: 0, right: 0)), tag: 0 )
        navigationController.navigationBar.isHidden = true
        navigationController.pushViewController(vc, animated: false)
    }

    func profileVC(user: UserService, name: String) {
        let vc = ProfileViewController(user: user, name: name)
        vc.coordinator = self
        vc.view.backgroundColor = .systemGray6
        navigationController.pushViewController(vc, animated: true)
    }

    func photoVC() {
        let vc = PhotosViewController()
        vc.coordinator = self
        vc.view.backgroundColor = .systemGray6
        navigationController.navigationBar.isHidden = false
        navigationController.pushViewController(vc, animated: true)
    }

    func didfinish() {
        parentCoordinator?.childDidFinish(self)
    }

}
