//
//  FavoritePostsController.swift
//  Vk
//
//  Created by Табункин Вадим on 28.08.2022.
//

import Foundation
import UIKit
import StorageService

class FavoritePostsController: UIViewController {
    @objc func reloadFavoritPost() {
        tableView.reloadData()
    }
    private var index: Int = 0
    private var coreDataCoordinator = CoreDataCoordinator()
    private lazy var tableView: UITableView = {
        $0.toAutoLayout()
        $0.dataSource = self
        $0.delegate = self
        $0.register(PostTableViewCell.self, forCellReuseIdentifier: PostTableViewCell.identifier)
        return $0
    }(UITableView(frame: .zero, style: .grouped))

    override func viewDidLoad() {
        super.viewDidLoad()
        NotificationCenter.default.addObserver(self, selector: #selector(reloadFavoritPost), name: Notification.Name.reloadFavoritPost, object: nil)
        layout()
    }
    private func layout() {
        view.addSubview(tableView)
        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            tableView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            tableView.widthAnchor.constraint(equalTo: view.widthAnchor)
        ])
    }
}

extension FavoritePostsController : UITableViewDataSource {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        coreDataCoordinator.getPostCount()
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        var cell: PostTableViewCell
        cell = (tableView.dequeueReusableCell(withIdentifier: PostTableViewCell.identifier , for: indexPath) as! PostTableViewCell)
        let favoritPost = coreDataCoordinator.getPost(postIndex: indexPath.row)
        guard  let favoritPost = favoritPost else {return UITableViewCell()}
        cell.postAutor.text = favoritPost.autor
        cell.postDescription.text = favoritPost.descriptionPost
        cell.postViews.text = String(favoritPost.postViews)
        cell.postLike.text = String(favoritPost.likes)
        cell.postImage.image = UIImage(data: favoritPost.image!)
        let tap = UITapGestureRecognizer(target: self, action: #selector(doubleTapped))
        tap.numberOfTapsRequired = 2
        cell.addGestureRecognizer(tap)
        return cell
    }
    @objc private func doubleTapped (){
        coreDataCoordinator.deletePosts(index: index)
        NotificationCenter.default.post(name: NSNotification.Name.reloadFavoritPost, object: nil)
    }
}

extension FavoritePostsController : UITableViewDelegate {
    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        index = indexPath.row
    }
}
