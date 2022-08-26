//
//  LogInViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 20.03.2022.
//

import UIKit
import Firebase
import RealmSwift


class LogInViewController: UIViewController {

    weak var coordinator: ProfileCoordinator?
    weak var delegate: LoginViewControllerDelegate?
    weak var delegateChecker: CheckerServiceProtocol?

    private var isCheck = false
    private var loginCheck = ""
    private var passwordCheck = ""

    private let realmCoordinator = RealmCoordinator()
    private let contentView: UIView = {
        $0.translatesAutoresizingMaskIntoConstraints = false
        return $0
    }(UIView())
    
    private let loginScrollView: UIScrollView = {
        $0.translatesAutoresizingMaskIntoConstraints = false
        return $0
    }(UIScrollView())
    
    private let logo: UIImageView = {
        $0.translatesAutoresizingMaskIntoConstraints = false
        $0.image = UIImage(named: "logo")
        return $0
    }(UIImageView())
    
    private lazy var loginSet: UITextField = {
        $0.translatesAutoresizingMaskIntoConstraints = false
        $0.leftView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.rightView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.leftViewMode = .always
        $0.rightViewMode = .always
        $0.backgroundColor = .systemGray6
        $0.layer.borderColor = UIColor.lightGray.cgColor
        $0.layer.borderWidth = 0.5
        $0.layer.cornerRadius = 10
        $0.textColor = .black
        $0.font = UIFont.systemFont(ofSize: 16)
        $0.tintColor = UIColor(named: "MainColor")
        $0.autocapitalizationType = .none
        $0.layer.maskedCorners = [.layerMinXMinYCorner, .layerMaxXMinYCorner]
        $0.placeholder = "Email of phone"
        $0.delegate = self
        $0.addTarget(self, action: #selector(loginsSeting(_:)), for: .editingChanged)
        $0.addTarget(self, action: #selector(buttonActivate(_:)), for: .editingChanged)
        return $0
    }(UITextField())

    @objc private func buttonActivate(_ textField: UITextField){
        if self.loginSet.hasText && self.passwordSet.hasText{
            loginButtom?.backgroundColor = UIColor(named: "MainColor") ?? .blue
            loginButtom?.setTitleColor(.white, for: .normal)
            loginButtom?.layer.borderWidth = 0
        } else {
            loginButtom?.backgroundColor = .white
            loginButtom?.setTitleColor(UIColor(named: "MainColor") ?? .blue, for: .normal)
            loginButtom?.layer.borderWidth = 1
        }
    }

    private lazy var passwordSet: UITextField = {
        $0.translatesAutoresizingMaskIntoConstraints = false
        $0.leftView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.rightView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.leftViewMode = .always
        $0.rightViewMode = .always
        $0.backgroundColor = .systemGray6
        $0.layer.borderColor = UIColor.lightGray.cgColor
        $0.layer.borderWidth = 0.5
        $0.layer.cornerRadius = 10
        $0.textColor = .black
        $0.font = UIFont.systemFont(ofSize: 16)
        $0.tintColor = UIColor(named: "MainColor")
        $0.autocapitalizationType = .none
        $0.layer.maskedCorners = [.layerMinXMaxYCorner , .layerMaxXMaxYCorner]
        $0.placeholder = "Password"
        $0.delegate = self
        $0.isSecureTextEntry = true
        $0.addTarget(self, action: #selector(passwordSeting), for: .editingChanged)
        $0.addTarget(self, action: #selector(buttonActivate(_:)), for: .editingChanged)
        return $0
    }(UITextField())

    enum AuthResult {
        case success
        case failure(Error)
    }

    private let checker = CheckerService()
    private let loginCheker: LoginInspector

    private lazy var delBottom = CustomButton(title: "удалить", color: .red, colorTitle: .white, borderWith: 1, cornerRadius: 10) {
        self.realmCoordinator.delete()
        self.setButtomLogin()
    }

    private lazy var loginButtom = CustomButton(title: "Login", color: .systemGray6, colorTitle: UIColor(named: "MainColor") ?? .blue , borderWith: 1, cornerRadius: 10) {
        guard let email = self.loginSet.text, !email.isEmpty, let password = self.passwordSet.text, !password.isEmpty else {
            self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
            self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
            return
        }
        if self.realmCoordinator.getCount() != 0 {
            guard let item = self.realmCoordinator.get() else {return}
            if item.password == password, item.email == email {
                self.realmCoordinator.edit(item: item, isLogIn: true)
                self.openProfile()
            } else {
                self.showAlert(title: "Не верный ввод", massege: "Повторите") { _ in
                    self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                    self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                    self.loginSet.text = ""
                    self.passwordSet.text = ""
                    self.setButtomLogin()
                }
            }
        } else {
            if self.isCheck {
                if self.passwordCheck == password, self.loginCheck == email {
                    self.realmCoordinator.create(password: password, email: email)
                    self.openProfile()
                } else {
                    self.showAlert(title: "Не верный ввод", massege: "Повторите") { _ in
                        self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                        self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                        self.passwordCheck = ""
                        self.loginCheck = ""
                        self.loginSet.text = ""
                        self.passwordSet.text = ""
                        self.isCheck = false
                        self.setButtomLogin()
                    }
                }
            } else {
                self.loginCheck = email
                self.passwordCheck = password
                self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.gray])
                self.loginSet.text = ""
                self.passwordSet.text = ""
                self.isCheck = true
                self.setButtomLogin()
            }

        }
    }

    private func showAlert (title: String, massege: String, action:@escaping (UIAlertAction)-> Void) {
        let alert = UIAlertController(title: title, message: massege, preferredStyle: .alert)
        let ok = UIAlertAction(title: "Хорошо", style: .cancel, handler: action)
        alert.addAction(ok)
        self.present(alert, animated: true, completion: nil)
    }

    private func openProfile() {
#if DEBUG
        self.coordinator?.profileVC(user: TestUserService(), name: "Пётр")
#else
        self.coordinator?.profileVC(user: CurrentUserService(), name: "Иван" )
#endif
        self.dismiss(animated: true)
    }

    init (loginCheker: LoginInspector){
        self.loginCheker = loginCheker
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        setButtomLogin()
        layout()
    }
    private func setButtomLogin() {
        if isCheck {
            self.loginButtom?.setTitle("Повторите ввод", for: .normal)
        } else{
            let realm = try! Realm()
            var items: Results<AuthorizationRealmModel>?
            items = realm.objects(AuthorizationRealmModel.self)
            guard let items = items else { return }
            if items.count != 0 {
                self.loginButtom?.setTitle("Вход", for: .normal)
            } else {
                self.loginButtom?.setTitle("Создать аккаунт", for: .normal)
            }
        }
    }

    @objc func loginsSeting(_ textField: UITextField){
        loginSet.text = textField.text ?? ""
    }

    @objc func passwordSeting (_ textField: UITextField){
        passwordSet.text = textField.text ?? ""
    }

    private func layout() {

        view.addSubview(loginScrollView)

        NSLayoutConstraint.activate([
            loginScrollView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            loginScrollView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            loginScrollView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            loginScrollView.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor),
            loginScrollView.widthAnchor.constraint(equalTo: view.widthAnchor)
        ])

        loginScrollView.addSubview(contentView)

        NSLayoutConstraint.activate([
            contentView.topAnchor.constraint(equalTo: loginScrollView.topAnchor),
            contentView.leadingAnchor.constraint(equalTo: loginScrollView.leadingAnchor),
            contentView.trailingAnchor.constraint(equalTo: loginScrollView.trailingAnchor),
            contentView.bottomAnchor.constraint(equalTo: loginScrollView.bottomAnchor),
            contentView.widthAnchor.constraint(equalTo: loginScrollView.widthAnchor)
        ])
        loginButtom!.translatesAutoresizingMaskIntoConstraints = false

        contentView.addSubviews(logo, loginSet, passwordSet, loginButtom!, delBottom!/*, guessingButtom!*/)

        NSLayoutConstraint.activate([
            logo.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 120),
            logo.widthAnchor.constraint(equalToConstant: 100),
            logo.heightAnchor.constraint(equalToConstant: 100),
            logo.centerXAnchor.constraint(equalTo: contentView.centerXAnchor),
        ])

        NSLayoutConstraint.activate([
            loginSet.topAnchor.constraint(equalTo: logo.bottomAnchor, constant: 120),
            loginSet.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            loginSet.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            loginSet.heightAnchor.constraint(equalToConstant: 50)
        ])

        NSLayoutConstraint.activate([
            passwordSet.topAnchor.constraint(equalTo: loginSet.bottomAnchor),
            passwordSet.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            passwordSet.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            passwordSet.heightAnchor.constraint(equalToConstant: 50)
        ])

        NSLayoutConstraint.activate([
            loginButtom!.topAnchor.constraint(equalTo: passwordSet.bottomAnchor, constant: 16),
            loginButtom!.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            loginButtom!.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            loginButtom!.heightAnchor.constraint(equalToConstant: 50)
        ])

        NSLayoutConstraint.activate([
            delBottom!.topAnchor.constraint(equalTo: loginButtom!.bottomAnchor, constant: 16),
            delBottom!.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            delBottom!.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            delBottom!.heightAnchor.constraint(equalToConstant: 50),
            delBottom!.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: 10)
        ])
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)

        // подписаться на уведомления
        let nc = NotificationCenter.default
        nc.addObserver(self, selector: #selector(kbdShow), name: UIResponder.keyboardWillShowNotification, object: nil)
        nc.addObserver(self, selector: #selector(kbdHide), name: UIResponder.keyboardWillHideNotification, object: nil)
    }

    override func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        // отписаться от уведомлений
        let nc = NotificationCenter.default
        nc.removeObserver(self, name: UIResponder.keyboardWillShowNotification, object: nil)
        nc.removeObserver(self, name: UIResponder.keyboardWillHideNotification, object: nil)
    }

    // Изменение отступов при появлении клавиатуры
    @objc private func kbdShow(notification: NSNotification) {
        if let kbdSize = (notification.userInfo?[UIResponder.keyboardFrameEndUserInfoKey] as? NSValue)?.cgRectValue {
            loginScrollView.contentInset.bottom = kbdSize.height
            loginScrollView.verticalScrollIndicatorInsets = UIEdgeInsets(top: 0, left: 0, bottom: kbdSize.height, right: 0) }
    }

    @objc private func kbdHide(notification: NSNotification) {
        loginScrollView.contentInset.bottom = .zero
        loginScrollView.verticalScrollIndicatorInsets = .zero
    }
}

extension LogInViewController: UITextFieldDelegate{
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        view.endEditing(true)
        return true
    }
}
