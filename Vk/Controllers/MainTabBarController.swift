//
//  MainTabBarController.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.02.2022.
//

import UIKit

class MainTabBarController: UITabBarController {

    weak var coordinator: MainCoordinator?
        var profileCoordinator: ProfileCoordinator
        var feedCoordinator: FeedCoordinator

    init (loginCheker: LoginInspector,checkModel: CheckModel ){
        profileCoordinator = MainCoordinator.shared.profile(navigationController: UINavigationController(), loginCheker: loginCheker)
        feedCoordinator = MainCoordinator.shared.feed(navigationController: UINavigationController(), checkModel: checkModel)
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        viewControllers = [profileCoordinator.navigationController, feedCoordinator.navigationController]
    }
}

