//
//  UIViewExtension.swift
//  Navigation
//
//  Created by Табункин Вадим on 26.03.2022.
//

import UIKit

extension UIView {
    static var identifier:String {String(describing: self)}

    func  toAutoLayout () {
        translatesAutoresizingMaskIntoConstraints = false
    }

    func addSubviews(_ subviews: UIView...) {
        subviews.forEach {addSubview($0)}
    }
}
