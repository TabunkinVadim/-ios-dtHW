
//
//  Post.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.03.2022.
//

import Foundation
import UIKit
import iOSIntPackage

public struct Post {
    public let author: String
    public let image: UIImage
    public let description: String
    public var likes: Int
    public var views: Int
    public let title: String

    public init(author: String, image: UIImage, description: String,likes: Int,views: Int, title:String) {
        self .author = author
        self .image = image
        self .description = description
        self .likes = likes
        self .views = views
        self .title = title
    }
}

var images = [UIImage(imageLiteralResourceName: "img_1"), UIImage(imageLiteralResourceName: "img_2"), UIImage(imageLiteralResourceName: "img_3"), UIImage(imageLiteralResourceName: "img_4")]

public var post1 = Post(author: "ZinovkinArt", image: images[0], description: "Здесь опять простая символика, о её значении пусть каждый решает сам.", likes: 230, views: 900, title: "")

public var post2 = Post(author: "Книги", image: images[1], description: "Уникальный арт-объект «Потерянная книга»", likes: 120, views: 700, title: "")

public var post3 = Post(author: "Books", image: images[2], description: "Прoклятущая лень! Кaкой я был бы cлавный чeловек, кoли бы oна мнe нe мeшaлa.\n— Л. Н. Тoлстoй", likes: 300, views: 2500, title: "")

public var post4 = Post(author: "Мульт", image: images[3], description: "Ежик в тумане", likes: 800, views: 43000, title: "")

public var posts = [post1, post2, post3, post4]

public let newPost = Post(author: "News", image: UIImage(), description: "", likes: 10, views: 10, title: "Новости")
