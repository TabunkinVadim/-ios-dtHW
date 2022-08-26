//
//  LoginInspector.swift
//  Vk
//
//  Created by Табункин Вадим on 07.06.2022.
//

import Foundation
class LoginInspector: LoginViewControllerDelegate {

    weak var delegateChecker: CheckerServiceProtocol?
    private let checker = CheckerService()

    func chek(login: String, pswd: String, completion: @escaping (AuthResult) -> Void) {
        self.delegateChecker = self.checker
        self.delegateChecker?.checkCredentials(email: login, password: pswd) { (result) in
            switch result {
            case .success:
                completion(.success)
            case .failure(let error):
                completion(.failure(error))
            }
        }
    }

    func signUp(login: String, pswd: String, completion: @escaping (AuthResult) -> Void) {
        self.delegateChecker = self.checker
        self.delegateChecker?.signUp(email: login, password: pswd, completion: { (result) in
            switch result {
            case .success:
                completion(.success)
            case .failure(let error):
                completion(.failure(error))
            }
        })
    }
}
