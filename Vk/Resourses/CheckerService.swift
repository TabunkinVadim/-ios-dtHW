//
//  CheckerService.swift
//  Vk
//
//  Created by Табункин Вадим on 26.07.2022.
//

import Foundation
import Firebase

protocol CheckerServiceProtocol: AnyObject{
    func checkCredentials(email: String, password: String, completion: @escaping (AuthResult) -> Void)
    func signUp(email: String, password: String, completion: @escaping (AuthResult) -> Void)
}

enum AuthResult {
    case success
    case failure(Error)
}

class CheckerService: CheckerServiceProtocol {

    weak var coordinator: ProfileCoordinator?
    func checkCredentials(email: String, password: String, completion: @escaping (AuthResult) -> Void)  {
            FirebaseAuth.Auth.auth().signIn(withEmail: email, password: password) { result, error in
                    guard let _ = result else {
                        completion(.failure(error!))
                        return
                    }
                completion(.success)
                    }
                }

    func signUp(email: String, password: String, completion: @escaping (AuthResult) -> Void) {
        Firebase.Auth.auth().createUser(withEmail: email, password: password) { result, error in
            guard let _ = result else {
                completion(.failure(error!))
                return
            }
                completion(.success)
        }
    }
}
