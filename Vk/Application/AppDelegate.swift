//
//  AppDelegate.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.02.2022.
//

import UIKit

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var coordinator: MainCoordinator?
    
    var window: UIWindow?
    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        MainCoordinator.shared.start()
        window = UIWindow(frame: UIScreen.main.bounds)
        window?.rootViewController = MainCoordinator.shared.navigationController
        window?.makeKeyAndVisible()

        let navBarAppearance = UINavigationBarAppearance()
        navBarAppearance.configureWithOpaqueBackground()
        UINavigationBar.appearance().standardAppearance = navBarAppearance
        UINavigationBar.appearance().scrollEdgeAppearance = navBarAppearance
        
        return true
    }
}

