//
//  FeedViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.03.2022.
//
import StorageService
import UIKit

class FeedViewController: UIViewController {
    let feedModel = FeedModel()

    private let contentView: UIView = {
        $0.toAutoLayout()
        return $0
    }(UIView())
    
    private let feedScrollView: UIScrollView = {
        $0.toAutoLayout()
        return $0
    }(UIScrollView())

    private lazy var word: UITextField = {
        $0.toAutoLayout()
        $0.leftView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.rightView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: $0.frame.height))
        $0.leftViewMode = .always
        $0.rightViewMode = .always
        $0.backgroundColor = .white
        $0.layer.borderColor = UIColor.lightGray.cgColor
        $0.layer.borderWidth = 0.5
        $0.layer.cornerRadius = 10
        $0.textColor = .black
        $0.font = UIFont.systemFont(ofSize: 16)
        $0.tintColor = UIColor(named: "MainColor")
        $0.autocapitalizationType = .none
        $0.placeholder = "any word"
        $0.delegate = self
        return $0
    }(UITextField())

    private lazy var wordCheck = CustomButton(title: "Chech word", color: UIColor(named: "MainColor") ?? .systemBlue, colorTitle: .white, borderWith: 0, cornerRadius: 10) {
        self.feedModel.chenge(.tapButton(checkModel: self.checkModel, textField: self.word))
    }

    let postsStack: UIStackView = {
        let stack = UIStackView()
        stack.toAutoLayout()
        stack.axis = .vertical
        stack.distribution = .fillEqually
        stack.spacing = 10
        return stack
    }()

    private lazy var postButton1 = CustomButton(title: newPost.title + "1", color: .systemGray4, colorTitle: .black, borderWith: 1, cornerRadius: 10) {
        self.feedModel.chenge(.postDetail)
    }
    private lazy var postButton2 = CustomButton(title: newPost.title + "2", color: .systemGray4,colorTitle: .black, borderWith: 1, cornerRadius: 10) {
        self.feedModel.chenge(.postDetail)
    }
    private let checkModel: CheckModel

    init (checkModel: CheckModel) {
        self.checkModel = checkModel
        super.init(nibName: nil, bundle: nil)
    }
    
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    override func loadView() {
        super.loadView()

    }
    override func viewDidLoad() {
        super.viewDidLoad()
        NotificationCenter.default.addObserver(self, selector: #selector(redLable), name: Notification.Name.redLable, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(greenLable), name: Notification.Name.greenLable, object: nil)
        layoutSubviews()
    }

    @objc func redLable() {
        word.backgroundColor = .red
    }

    @objc func greenLable() {
        word.backgroundColor = .green
    }

    @objc func pressFeedButtom(_ sender:Any) {
        navigationController?.pushViewController( PostViewController(), animated: true)
    }

    private func layoutSubviews(){
        view.addSubview(feedScrollView)

        NSLayoutConstraint.activate([
            feedScrollView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            feedScrollView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            feedScrollView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            feedScrollView.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor),
            feedScrollView.widthAnchor.constraint(equalTo: view.widthAnchor)
        ])
        feedScrollView.addSubview(contentView)

        NSLayoutConstraint.activate([
            contentView.topAnchor.constraint(equalTo: feedScrollView.topAnchor),
            contentView.leadingAnchor.constraint(equalTo: feedScrollView.leadingAnchor),
            contentView.trailingAnchor.constraint(equalTo: feedScrollView.trailingAnchor),
            contentView.bottomAnchor.constraint(equalTo: feedScrollView.bottomAnchor),
            contentView.widthAnchor.constraint(equalTo: feedScrollView.widthAnchor)
        ])

        feedScrollView .addSubviews(word, wordCheck!, postsStack)

        NSLayoutConstraint.activate([
            word.topAnchor.constraint(equalTo: feedScrollView.safeAreaLayoutGuide.topAnchor, constant: 30),
            word.leadingAnchor.constraint(equalTo: feedScrollView.leadingAnchor, constant: 16),
            word.trailingAnchor.constraint(equalTo: feedScrollView.trailingAnchor, constant: -16),
            word.heightAnchor.constraint(equalToConstant: 50)
        ])

        NSLayoutConstraint.activate([
            wordCheck!.topAnchor.constraint(equalTo: word.bottomAnchor, constant: 30),
            wordCheck!.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 16),
            wordCheck!.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -16),
            wordCheck!.heightAnchor.constraint(equalToConstant: 50)
        ])

        NSLayoutConstraint.activate([
            postsStack.topAnchor.constraint(equalTo: wordCheck!.bottomAnchor, constant: 50),
            postsStack.bottomAnchor.constraint(equalTo: feedScrollView.safeAreaLayoutGuide.bottomAnchor, constant: -50),
            postsStack.leadingAnchor.constraint(equalTo: feedScrollView.leadingAnchor, constant: 16),
            postsStack.trailingAnchor.constraint(equalTo: feedScrollView.trailingAnchor, constant: -16)
        ])
        postsStack.addArrangedSubview(postButton1!)
        postsStack.addArrangedSubview(postButton2!)
    }
    
    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)

        self.navigationController?.navigationBar.isHidden = true

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

    //     Изменение отступов при появлении клавиатуры
    @objc private func kbdShow(notification: NSNotification) {
        if let kbdSize = (notification.userInfo?[UIResponder.keyboardFrameEndUserInfoKey] as? NSValue)?.cgRectValue {
            feedScrollView.contentInset.bottom = kbdSize.height
            feedScrollView.verticalScrollIndicatorInsets = UIEdgeInsets(top: 0, left: 0, bottom: kbdSize.height, right: 0) }
    }

    @objc private func kbdHide(notification: NSNotification) {
        feedScrollView.contentInset.bottom = .zero
        feedScrollView.verticalScrollIndicatorInsets = .zero
    }
}

extension FeedViewController: UITextFieldDelegate {
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        view.endEditing(true)
        feedModel.chenge(.tapButton(checkModel: checkModel, textField: word))
        return true
    }
}

public extension NSNotification.Name {
    static let redLable = NSNotification.Name("redLable")
    static let greenLable = NSNotification.Name("greenLable")
}

let newPost = Post(author: "News", image: UIImage(), description: "", likes: 10, views: 10, title: "Новости")


