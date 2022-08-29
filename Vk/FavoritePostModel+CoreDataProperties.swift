//
//  FavoritePostModel+CoreDataProperties.swift
//  Vk
//
//  Created by Табункин Вадим on 28.08.2022.
//
//

import Foundation
import CoreData


extension FavoritePostModel {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<FavoritePostModel> {
        return NSFetchRequest<FavoritePostModel>(entityName: "FavoritePostModel")
    }

    @NSManaged public var autor: String?
    @NSManaged public var image: Data?
    @NSManaged public var descriptionPost: String?
    @NSManaged public var likes: Int16
    @NSManaged public var postViews: Int16

}

extension FavoritePostModel : Identifiable {

}
