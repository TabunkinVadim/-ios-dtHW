//
//  PhotoGallery.swift
//  Navigation
//
//  Created by Табункин Вадим on 27.03.2022.
//

import Foundation
import UIKit
import iOSIntPackage

let photoGallery2: [UIImage] = [UIImage(named: "Photo_1") ?? UIImage(),
                     UIImage(named: "Photo_2") ?? UIImage(),
                     UIImage(named: "Photo_3") ?? UIImage(),
                     UIImage(named: "Photo_4") ?? UIImage(),
                     UIImage(named: "Photo_5") ?? UIImage(),
                     UIImage(named: "Photo_6") ?? UIImage(),
                     UIImage(named: "Photo_7") ?? UIImage(),
                     UIImage(named: "Photo_8") ?? UIImage(),
                     UIImage(named: "Photo_9") ?? UIImage(),
                     UIImage(named: "Photo_10") ?? UIImage(),
                     UIImage(named: "Photo_11") ?? UIImage(),
                     UIImage(named: "Photo_12") ?? UIImage(),
                     UIImage(named: "Photo_13") ?? UIImage(),
                     UIImage(named: "Photo_14") ?? UIImage(),
                     UIImage(named: "Photo_15") ?? UIImage(),
                     UIImage(named: "Photo_16") ?? UIImage(),
                     UIImage(named: "Photo_17") ?? UIImage(),
                     UIImage(named: "Photo_18") ?? UIImage(),
                     UIImage(named: "Photo_19") ?? UIImage(),
                     UIImage(named: "Photo_20") ?? UIImage()]

let imageFasade = ImagePublisherFacade()
let setFasade: () = imageFasade.addImagesWithTimer(time: 1, repeat: 20, userImages: photoGallery2)
var incominginImages: [UIImage] = []
