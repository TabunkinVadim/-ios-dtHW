//
//  ProfilePhotoCollectionViewCell.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.03.2022.
//

import UIKit

class ProfilePhotoCollectionViewCell: UICollectionViewCell {
    weak var coordinator: ProfileCoordinator?
    var imageView: UIImageView =  {
        $0.toAutoLayout()
        $0.backgroundColor = .black
        $0.clipsToBounds = true
        $0.contentMode = .scaleAspectFill
        return $0
    }(UIImageView())

    override init(frame: CGRect) {
        super .init(frame: frame)
        contentView.backgroundColor = .blue
        contentView.layer.cornerRadius = 6
        contentView.clipsToBounds = true
        layout()
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func layout() {
        addSubview(imageView)
        NSLayoutConstraint.activate([
            imageView.topAnchor.constraint(equalTo: topAnchor),
            imageView.leadingAnchor.constraint(equalTo: leadingAnchor),
            imageView.trailingAnchor.constraint(equalTo: trailingAnchor),
            imageView.bottomAnchor.constraint(equalTo: bottomAnchor)
        ])
    }
} 
