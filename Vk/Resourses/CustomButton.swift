//
//  CustomButton.swift
//  Vk
//
//  Created by Табункин Вадим on 15.06.2022.
//

import Foundation
import UIKit

class CustomButton: UIButton {
    let action: (() -> Void)

    init?(title: String, color: UIColor, colorTitle: UIColor, borderWith: CGFloat,  cornerRadius: CGFloat,  action: @escaping () -> Void){
        self.action = action
        super.init(frame: CGRect())
        setTitle(title, for: .normal)
        backgroundColor = color
        setTitleColor(colorTitle, for: .normal)
        layer.cornerRadius = cornerRadius
        layer.borderWidth = borderWith
        layer.masksToBounds = true
        addTarget(self, action: #selector(buttonTapped), for: .touchUpInside)
        toAutoLayout()
    }

    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    @objc private func buttonTapped() -> Void{
            action()
    }
}
