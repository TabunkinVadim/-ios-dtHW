//
//  CoreDataCoordinator.swift
//  Vk
//
//  Created by Табункин Вадим on 28.08.2022.
//

import Foundation
import CoreData
import StorageService
import UIKit

final class CoreDataCoordinator {

    lazy var persistentContainer: NSPersistentContainer = {
        let container = NSPersistentContainer(name: "CoreData")
        container.loadPersistentStores { description, error in
            if let error = error {
                fatalError("Unable to load persistent stores: \(error)")
            }
        }
        return container
    }()

    lazy var viewContext: NSManagedObjectContext = persistentContainer.viewContext

//    func seveContext() {
//        let context = persistentContainer.viewContext
//        if context.hasChanges {
//            do{
//                try context.save()
//            } catch {
//                let nserror = error as NSError
//                fatalError("Unresolved error \(nserror), \(nserror.userInfo)")
//            }
//        }
//    }

    func sevePost (post: Post) {
        let favoritePost = FavoritePostModel(context: viewContext)
        favoritePost.autor = post.author
        favoritePost.descriptionPost = post.description
        favoritePost.image = post.image.jpegData(compressionQuality: 1)
        favoritePost.likes = Int16(post.likes)
        favoritePost.postViews = Int16(post.views)
        do {
            try viewContext.save()
        } catch let error {
            print("ERRORE: \(error)")
        }
    }
    
    func getPost (postIndex: Int) -> FavoritePostModel? {
        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "FavoritePostModel")
        if let favoritPost = try? viewContext.fetch(fetchRequest) as? [FavoritePostModel], !favoritPost.isEmpty {
            return favoritPost[postIndex]
        } else {
            return nil
        }
    }

    func getPostCount() -> Int {
        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "FavoritePostModel")
        if let favoritPost = try? viewContext.fetch(fetchRequest) as? [FavoritePostModel], !favoritPost.isEmpty {
            return favoritPost.count
        } else {
            return 0
        }
    }

    func deletePosts(index: Int) {
        let favoritePost = self.getPost(postIndex: index)
        guard let favoritePost = favoritePost else { return }
        viewContext.delete(favoritePost)
        do{
            try viewContext.save()
        } catch {
            let nserror = error as NSError
            fatalError("Unresolved error \(nserror), \(nserror.userInfo)")
        }
    }
}
