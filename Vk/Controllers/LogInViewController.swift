//
//  LogInViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 20.03.2022.
//

import UIKit 

class LogInViewController: UIViewController {
    weak var coordinator: ProfileCoordinator?
    
    weak var delegate: LoginViewControllerDelegate?

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
        return $0
    }(UITextField())
    
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
        return $0
    }(UITextField())

    private lazy var loginButtom = CustomButton(title: "Login", color: UIColor(named: "MainColor") ?? .blue, colorTitle: .white, borderWith: 0, cornerRadius: 10) {
        self.delegate = self.loginCheker
        if let bool = self.delegate?.chek(login: self.loginSet.text ?? "", pswd: self.passwordSet.text ?? "") {
            #if DEBUG
            if bool{
                self.coordinator?.profileVC(user: TestUserService(), name: self.loginSet.text ?? "")
            }else {
                self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
                self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
                self.loginSet.textColor = .red
                self.passwordSet.textColor = .red
            }
            #else
            if bool{
                self.coordinator?.profileVC(user: CurrentUserService(), name: self.loginSet.text ?? "")
            }else {
                self.loginSet.attributedPlaceholder = NSAttributedString.init(string: "Email of phone", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
                self.passwordSet.attributedPlaceholder = NSAttributedString.init(string: "Password", attributes: [NSAttributedString.Key.foregroundColor: UIColor.red])
                self.loginSet.textColor = .red
                self.passwordSet.textColor = .red
            }
            #endif
        }
    }
    private lazy var guessingButtom = CustomButton(title: "bruteForce", color: UIColor(named: "MainColor") ?? .blue, colorTitle: .white, borderWith: 0, cornerRadius: 10) {
        var pass = ""{
            didSet{
                DispatchQueue.main.async{
                    self.passwordSet.text = pass
                    #if DEBUG
                    self.loginSet.text = "Пётр"
                    #else
                    self.loginSet.text = "Иван"
                    #endif
                    self.passwordSet.isSecureTextEntry = false
                    self.indicator.stopAnimating()
                    self.indicator.hidesWhenStopped = true
                }
            }
        }
        DispatchQueue.global().async {
            let randomPass = Checker.shared.randomString(length: 4)
            pass = PassGuessing().bruteForce(passwordToUnlock: randomPass)
        }

        DispatchQueue.main.async{
            self.passwordSet.addSubview(self.indicator)
            NSLayoutConstraint.activate([
                self.indicator.centerXAnchor.constraint(equalTo: self.passwordSet.centerXAnchor),
                self.indicator.centerYAnchor.constraint(equalTo: self.passwordSet.centerYAnchor),
                self.indicator.heightAnchor.constraint(equalToConstant: 40),
                self.indicator.widthAnchor.constraint(equalToConstant: 40)
            ])
            self.indicator.startAnimating()
        }
    }

    var indicator: UIActivityIndicatorView = {
        $0.toAutoLayout()
        $0.style = UIActivityIndicatorView.Style.medium
        return $0
    }(UIActivityIndicatorView())

    private let loginCheker: LoginInspector

    init (loginCheker: LoginInspector){
        self.loginCheker = loginCheker
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    
    override func viewDidLoad() {
        super.viewDidLoad()
        layout()
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

        contentView.addSubviews(logo, loginSet, passwordSet, loginButtom!, guessingButtom!)

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
            loginButtom!.heightAnchor.constraint(equalToConstant: 50),
//            loginButtom!.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: 10)
        ])

        NSLayoutConstraint.activate([
            guessingButtom!.topAnchor.constraint(equalTo: loginButtom!.bottomAnchor, constant: 16),
            guessingButtom!.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            guessingButtom!.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            guessingButtom!.heightAnchor.constraint(equalToConstant: 50),
            guessingButtom!.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: 10)
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
