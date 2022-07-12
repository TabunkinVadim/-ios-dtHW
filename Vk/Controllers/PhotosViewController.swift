//
//  PhotosViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.04.2022.
//

import UIKit
import iOSIntPackage

class PhotosViewController: UIViewController {

    weak var coordinator: ProfileCoordinator?

    private lazy var collectionView: UICollectionView = {
        let layout = UICollectionViewFlowLayout()
        let collection = UICollectionView(frame:.zero, collectionViewLayout: layout)
        collection.dataSource = self
        collection.delegate = self
        collection.backgroundColor = .none
        collection.register(ProfilePhotoCollectionViewCell.self, forCellWithReuseIdentifier: ProfilePhotoCollectionViewCell.identifier)
        collection.toAutoLayout()
        return collection
    }()

    var imageThreads: [UIImage] = []

    override func viewDidLoad() {
        self.navigationController?.navigationBar.isHidden = false
        title = "Photo Gallery"
        view.backgroundColor = .white
        super.viewDidLoad()
        threadImages()
        layout()
    }

    /* Измерения времени срабатывания метода processImagesOnThread с параметром:
     qos: .default
     Измерение 1: 6.113 сек.
     Измерение 2: 5.367 сек.
     Измерение 3: 5.182 сек.

     qos: .background
     Измерение 1: 5.020 сек.
     Измерение 2: 5.008 сек.
     Измерение 3: 5.032 сек.

     qos: .userInitiated
     Измерение 1: 4.911 сек.
     Измерение 2: 5.736 сек.
     Измерение 3: 5.728 сек.

     qos: .userInteractive
     Измерение 1: 5.528 сек.
     Измерение 2: 5.121 сек.
     Измерение 3: 5.386 сек.

     qos: .utility
     Измерение 1: 5.446 сек.
     Измерение 2: 10.947 сек.
     Измерение 3: 5.973 сек.
     */
    func threadImages() {
        let startTime = Date()
        ImageProcessor().processImagesOnThread(sourceImages: photoGallery2, filter: .chrome, qos: .utility) { imageThread in
            var images: [UIImage] = []
            for image in imageThread {
                guard  let image = image else { return }
                images.append(UIImage(cgImage: image))
            }
            self.imageThreads = images
            DispatchQueue.main.async{
                self.collectionView.reloadData()
                let time = Date().timeIntervalSince(startTime)
                print(time)
            }
        }
    }

    func layout() {
        view.addSubviews(collectionView)
        NSLayoutConstraint.activate([
            collectionView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            collectionView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            collectionView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            collectionView.bottomAnchor.constraint(equalTo: view.bottomAnchor)
        ])
    }
}

extension PhotosViewController : UICollectionViewDataSource, UICollectionViewDelegateFlowLayout {
    static var indent:CGFloat  {
        8
    }

    func collectionView(_ collectionView: UICollectionView, numberOfItemsInSection section: Int) -> Int {
        imageThreads.count
    }

    func collectionView(_ collectionView: UICollectionView, cellForItemAt indexPath: IndexPath) -> UICollectionViewCell {
        let cell = collectionView.dequeueReusableCell(withReuseIdentifier: ProfilePhotoCollectionViewCell.identifier, for: indexPath) as! ProfilePhotoCollectionViewCell
        cell.imageView.image = imageThreads[indexPath.item]
        return cell
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, sizeForItemAt indexPath: IndexPath) -> CGSize {
        let width = (collectionView.bounds.width - 4 * PhotosViewController.indent) / 3
        return CGSize(width: width, height: width)
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, minimumInteritemSpacingForSectionAt section: Int) -> CGFloat {
        PhotosViewController.indent
    }

    func collectionView(_ collectionView: UICollectionView, layout collectionViewLayout: UICollectionViewLayout, insetForSectionAt section: Int) -> UIEdgeInsets {
        UIEdgeInsets(top: PhotosViewController.indent, left: PhotosViewController.indent, bottom: PhotosViewController.indent, right: PhotosViewController.indent)
    }

}
