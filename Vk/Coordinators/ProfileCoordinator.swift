//
//  ProfileCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 23.06.2022.
//

import Foundation
import UIKit
import RealmSwift
import Firebase

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
        guard let item = RealmCoordinator().get() else {
            logInVC()
            return}

        if item.logIn{
#if DEBUG
        self.profileVC(user: TestUserService(), name: "Пётр")
#else
        self.profileVC(user: CurrentUserService(), name: "Иван" )
#endif
        } else {
            logInVC()
        }
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
        vc.tabBarItem = UITabBarItem(title: "Profile", image: UIImage(systemName: "person")?.withAlignmentRectInsets(.init(top: 0, left: 0, bottom: 0, right: 0)), tag: 0 )
        navigationController.pushViewController(vc, animated: true)
    }

    func photoVC() {
        let vc = PhotosViewController()
        vc.coordinator = self
        vc.view.backgroundColor = .systemGray6
        navigationController.navigationBar.isHidden = false
        navigationController.pushViewController(vc, animated: true)
    }
    func singUpAlert(yesAction:((UIAlertAction) -> Void)?, cancelAction:((UIAlertAction) -> Void)?) {
        let alert: UIAlertController = {
            $0.title = "Создание аккаунта"
            $0.message = "Вы хотите создать аккаунт?"
            return $0
        }(UIAlertController())
        alert.addAction(UIAlertAction(title: "Да", style: .default, handler: yesAction))
        alert.addAction(UIAlertAction(title: "Отмена", style: .cancel, handler: cancelAction))
        navigationController.present(alert, animated: true)
    }

    func errorAlert (error: Error?, cancelAction:((UIAlertAction) -> Void)?) {
        let alert: UIAlertController = {
            $0.title = "Ошибка"
            if let error = error {
                $0.message = error.localizedDescription
            } else { $0.message = "Неизвесная ошибка" }
            return $0
        }(UIAlertController())
        alert.addAction(UIAlertAction(title: "Отмена", style: .cancel, handler: cancelAction))
        navigationController.present(alert, animated: true)
    }
        func didfinish() {
            parentCoordinator?.childDidFinish(self)
        }

}
