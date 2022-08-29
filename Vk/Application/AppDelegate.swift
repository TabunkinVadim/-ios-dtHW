//
//  AppDelegate.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.02.2022.
//

import UIKit
import Firebase
import RealmSwift

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var coordinator: MainCoordinator?


    var window: UIWindow?
//    func applicationDidFinishLaunching(_ application: UIApplication) {
//        let appConfiguration = AppConfiguration.randomElement()
//        NetworkService.request(for: appConfiguration)
//    }

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        FirebaseApp.configure()
        migration()
        MainCoordinator.shared.start()
        window = UIWindow(frame: UIScreen.main.bounds)
        window?.rootViewController = MainCoordinator.shared.navigationController
        window?.makeKeyAndVisible()
        let navBarAppearance = UINavigationBarAppearance()
        navBarAppearance.configureWithOpaqueBackground()
        UINavigationBar.appearance().standardAppearance = navBarAppearance
        UINavigationBar.appearance().scrollEdgeAppearance = navBarAppearance
//        UserJson.request()
        return true
    }

    func applicationWillTerminate(_ application: UIApplication) {
        do {
            try Firebase.Auth.auth().signOut()
        } catch {
            print("Произошла ошибка")
        }
    }


    func migration() {
        let config = Realm.Configuration(
            // Новая версия базы
            schemaVersion: 2,
            migrationBlock: { migration, oldSchemaVersion in
                // Последняя версия базы
                if (oldSchemaVersion < 1) {
                    // Вносимые изменения
                    migration.enumerateObjects(ofType: AuthorizationRealmModel.className()){ oldObject, newObject in
                        newObject!["logIn"] = false
                    }
                }
            })
        Realm.Configuration.defaultConfiguration = config
    }

}

