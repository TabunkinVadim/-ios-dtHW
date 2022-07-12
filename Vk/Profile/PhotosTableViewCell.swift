//
//  PhotosTableViewCell.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.03.2022.
//

import UIKit
import iOSIntPackage

class PhotosTableViewCell: UITableViewCell {
    weak var coordinator: ProfileCoordinator?
    private lazy var collectionView: UICollectionView = {
        let layout = UICollectionViewFlowLayout()
        let collection = UICollectionView(frame:.zero, collectionViewLayout: layout)
        layout.scrollDirection = .horizontal
        collection.dataSource = self
        collection.delegate = self
        collection.backgroundColor = .none
        collection.register(ProfilePhotoCollectionViewCell.self, forCellWithReuseIdentifier: ProfilePhotoCollectionViewCell.identifier)
        collection.toAutoLayout()
        return collection
    }()

    private let arrowRight : UIImageView = {
        $0.toAutoLayout()
        $0.image = UIImage(systemName: "arrow.right")
        $0.contentMode = .scaleAspectFit
        $0.tintColor = .black
        return $0
    }(UIImageView ())

    private let title: UILabel = {
        $0.toAutoLayout()
        $0.text = "Photos"
        $0.font = UIFont.boldSystemFont(ofSize: 24)
        return $0
    }(UILabel())

    override init(style: UITableViewCell.CellStyle, reuseIdentifier: String?) {
        super .init(style: style, reuseIdentifier: reuseIdentifier)
        layout()
        imageFasade.subscribe(self)
        setFasade
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    private func layout() {

        contentView.addSubviews(title, collectionView, arrowRight)

        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: contentView.topAnchor, constant: 12),
            title.leadingAnchor.constraint(equalTo: contentView.leadingAnchor, constant: 12)
        ])

        NSLayoutConstraint.activate([
            collectionView.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 12),
            collectionView.leadingAnchor.constraint(equalTo: contentView.leadingAnchor),
            collectionView.trailingAnchor.constraint(equalTo: contentView.trailingAnchor),
            collectionView.bottomAnchor.constraint(equalTo: contentView.bottomAnchor, constant: -12),
            collectionView.heightAnchor.constraint(equalToConstant: (UIScreen.main.bounds.width - 48) / 4)
        ])

        NSLayoutConstraint.activate([
            arrowRight.centerYAnchor.constraint(equalTo: title.centerYAnchor),
            arrowRight.trailingAnchor.constraint(equalTo: contentView.trailingAnchor, constant: -12),
            arrowRight.heightAnchor.constraint(equalToConstant: 24),
            arrowRight.widthAnchor.constraint(equalToConstant: 24)
        ])
    }
}

extension PhotosTableViewCell: UICollectionViewDataSource, UICollectionViewDelegateFlowLayout{

    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        incominginImages.count
    }
    
    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        let cell = collectionView.dequeueReusableCell(withReuseIdentifier: ProfilePhotoCollectionViewCell.identifier, for: indexPath) as! ProfilePhotoCollectionViewCell

        cell.imageView.image = incominginImages[indexPath.item]
        return cell
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, sizeForItemAt indexPath: IndexPath) -> CGSize {
        let width = (collectionView.bounds.width - 48) / 4
        return CGSize(width: width, height: width)
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, minimumInteritemSpacingForSectionAt section: Int) -> CGFloat {
        CGFloat(8)
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, insetForSectionAt section: Int) -> UIEdgeInsets {
        UIEdgeInsets(top: 0, left: 12, bottom: 0, right: 12)
    }
}

extension PhotosTableViewCell: ImageLibrarySubscriber{
    func receive(images: [UIImage]) {
        incominginImages = images
        collectionView.reloadData()
    }
}
