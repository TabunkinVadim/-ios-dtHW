//
//  PostViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.03.2022.
//
import StorageService
import UIKit

class PostViewController: UIViewController {
    weak var coordinator: FeedCoordinator?
    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .lightGray
        title = newPost.title
        self.navigationItem.rightBarButtonItem = UIBarButtonItem(title: "Info", style: .plain, target: self, action: #selector(infoButtom(_:)))
        self.navigationItem.leftBarButtonItem = UIBarButtonItem(title: "Feed", style: .plain, target: self, action: #selector(pop(_:)))
    }
    @objc func infoButtom(_ sender:Any) {
        coordinator?.infoVC()
    }
    @objc func pop(_ sender:Any) {
        coordinator?.pop()
    }
}
