//
//  FeedCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 24.06.2022.
//

import Foundation
import UIKit

public final class FeedCoordinator: Coordinator{

    var childCoordinators = [Coordinator]()
    var navigationController: UINavigationController
    let checkModel: CheckModel

    
    init(navigationController: UINavigationController, checkModel: CheckModel) {
        self.navigationController = navigationController
        self.checkModel = checkModel
    }

    func start() {
        let vc = FeedViewController(checkModel: checkModel)
        vc.feedModel.coordinator = self
        vc.tabBarItem = UITabBarItem(title: "Newsline", image: UIImage(systemName: "newspaper")?.withAlignmentRectInsets(.init(top: 0, left: 0, bottom: 0, right: 0)), tag: 0 )
        vc.view.backgroundColor = .systemBackground
        vc.title = "Feed"
        navigationController.navigationBar.isHidden = true
        navigationController.pushViewController(vc, animated: false)
    }

    func postVC() {
        let vc = PostViewController()
        vc.coordinator = self
        vc.view.backgroundColor = .systemGray6
        navigationController.navigationBar.isHidden = false
        navigationController.pushViewController(vc, animated: true)
    }

    func infoVC() {
        let nc = UINavigationController(rootViewController: InfoViewController())
        navigationController.present(nc, animated: true, completion: .none)
    }
    
    func pop (){
        navigationController.popViewController(animated: true)
    }

}
