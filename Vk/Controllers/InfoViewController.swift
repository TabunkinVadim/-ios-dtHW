//
//  InfoViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.03.2022.
//

import UIKit

class InfoViewController: UIViewController {
    weak var coordinator: FeedCoordinator?

    let screenWidth = UIScreen.main.bounds.width
    let screenHeight = UIScreen.main.bounds.height

    private lazy var alertButton = CustomButton(title: "Alert", color: .white, colorTitle: .blue, borderWith: 1, cornerRadius: 10) {

        let pressAlertButtom = UIAlertController(title: "Alert", message: "Внимание", preferredStyle: .alert)
        let cancelAction = UIAlertAction(title: "Отмена", style: .cancel, handler: {_ in print("Отмена")})
        pressAlertButtom.addAction(cancelAction)
        let deleteAction = UIAlertAction(title: "Удалить", style: .destructive, handler: {_ in print("Удалить")})
        pressAlertButtom.addAction(deleteAction)
        self.present(pressAlertButtom, animated: true, completion: nil)
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .systemBackground
        title = "Info"
        layout()
    }

    func layout () {
        alertButton!.toAutoLayout()
        view.addSubview(alertButton!)
        NSLayoutConstraint.activate([
            alertButton!.bottomAnchor.constraint(equalTo: view.bottomAnchor, constant: -150),
            alertButton!.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 40),
            alertButton!.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -40),
            alertButton!.heightAnchor.constraint(equalToConstant: screenHeight / 9)
        ])

    }
}
