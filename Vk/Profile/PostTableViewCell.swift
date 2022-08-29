//
//  PostTableViewCell.swift
//  Navigation
//
//  Created by Табункин Вадим on 23.03.2022.
//

import UIKit
import iOSIntPackage

import StorageService

class PostTableViewCell: UITableViewCell {
    var index: Int = 0
    let postCell: UIView = {
        $0.toAutoLayout()
        $0.backgroundColor = .white
        return $0
    }(UIView())

    let postAutor :UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.boldSystemFont(ofSize: 20)
        $0.textColor = .black
        $0.numberOfLines = 2
        return $0
    } (UILabel())

    let postImage:UIImageView = {
        $0.toAutoLayout()
        $0.contentMode = .scaleAspectFit
        $0.backgroundColor = .black
        return $0
    }(UIImageView())

    let postDescription : UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.systemFont(ofSize: 14)
        $0.textColor = .systemGray
        $0.numberOfLines = 0
        return $0
    } (UILabel())

    let postLike : UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.systemFont(ofSize: 16)
        $0.textColor = .black
        return $0
    } (UILabel())

    let postViews : UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.systemFont(ofSize: 16)
        $0.textColor = .black
        return $0
    } (UILabel())

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
//        self.index = index
        super .init(style: style, reuseIdentifier: reuseIdentifier)
//        self.index = index
        layout()
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func setupCell (model:Post, set: Int) {
        postAutor.text = model.author

        let filter: ColorFilter
        switch set {
        case 0:
            filter = .chrome
        case 1:
            filter = .fade
        case 2:
            filter = .bloom(intensity: 0.4)
        default:
            filter = .colorInvert
        }
        ImageProcessor().processImage(sourceImage: model.image, filter: filter ) { imageFilter in
            postImage.image = imageFilter
        }
        postDescription.text = model.description
        postLike.text = "Likes: \(model.likes)"
        postViews.text = "Views: \(model.views)"
    }

    private func layout() {
        
        contentView.addSubviews(postCell,postAutor, postImage, postDescription, postLike, postViews)

        NSLayoutConstraint.activate([
            postCell.topAnchor.constraint(equalTo:  contentView.topAnchor),
            postCell.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            postCell.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            postCell.bottomAnchor.constraint(equalTo: contentView.bottomAnchor),
            postCell.widthAnchor.constraint(equalToConstant: UIScreen.main.bounds.width)
        ])

        NSLayoutConstraint.activate([
            postAutor.topAnchor.constraint(equalTo: postCell.topAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postAutor.leadingAnchor.constraint(equalTo: postCell.leadingAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postAutor.trailingAnchor.constraint(equalTo: postCell.trailingAnchor, constant: -ConstrantsPostTableViewCell.viewIndent)
        ])

        NSLayoutConstraint.activate([
            postImage.topAnchor.constraint(equalTo: postAutor.bottomAnchor, constant: ConstrantsPostTableViewCell.imageIndent),
            postImage.leadingAnchor.constraint(equalTo: postCell.leadingAnchor),
            postImage.trailingAnchor.constraint(equalTo: postCell.trailingAnchor),
            postImage.heightAnchor.constraint(equalTo: postCell.widthAnchor)
        ])

        NSLayoutConstraint.activate([
            postDescription.topAnchor.constraint(equalTo: postImage.bottomAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postDescription.leadingAnchor.constraint(equalTo: postCell.leadingAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postDescription.trailingAnchor.constraint(equalTo: postCell.trailingAnchor, constant: -ConstrantsPostTableViewCell.viewIndent)
        ])

        NSLayoutConstraint.activate([
            postLike.topAnchor.constraint(equalTo: postDescription.bottomAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postLike.leadingAnchor.constraint(equalTo: postCell.leadingAnchor, constant: ConstrantsPostTableViewCell.viewIndent)
        ])

        NSLayoutConstraint.activate([
            postViews.topAnchor.constraint(equalTo: postDescription.bottomAnchor, constant: ConstrantsPostTableViewCell.viewIndent),
            postViews.trailingAnchor.constraint(equalTo: postCell.trailingAnchor, constant: -ConstrantsPostTableViewCell.viewIndent),
            postViews.bottomAnchor.constraint(equalTo: postCell.bottomAnchor, constant: -ConstrantsPostTableViewCell.viewIndent)
        ])
    }
}

