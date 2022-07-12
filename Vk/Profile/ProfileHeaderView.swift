//
//  ProfileHeaderView.swift
//  Navigation
//
//  Created by Табункин Вадим on 13.03.2022.
//

import UIKit
import SnapKit

class ProfileHeaderView:UITableViewHeaderFooterView  {
    weak var coordinator: ProfileCoordinator?
    private var statusText: String = ""
    
    let avatarView: UIView = {
        $0.layer.cornerRadius = 50
        $0.layer.borderWidth = 3
        $0.layer.borderColor = UIColor.white.cgColor
        $0.layer.masksToBounds = true
        $0.backgroundColor = .blue
        return $0
    }(UIView())
    
    var avatar: UIImageView = {
        let image = UIImageView()
        image.layer.cornerRadius = 50
        image.layer.borderWidth = 3
        image.layer.borderColor = UIColor.white.cgColor
        image.layer.masksToBounds = true
        image.image = UIImage(named: "Avatar")
        return image
    }()
    
    var name: UILabel = {
        let lable = UILabel()
        lable.font = UIFont.boldSystemFont(ofSize: 18)
        lable.textColor = .black
        lable.text = "Иван Иванович"
        return lable
    }()

    private lazy var statusButtom = CustomButton(title: "Show status", color: .blue, colorTitle: .white, borderWith: 0, cornerRadius: 4) {
        self.coordinator?.photoVC()
        self.status.text = self.statusText
        print(self.status.text ?? "Статус отсутствует")
    }
    
    var status: UILabel = {
        let lable = UILabel(frame: CGRect(x: 100, y: 400, width: 200, height: 40))
        lable.font = UIFont.systemFont(ofSize: 14)
        lable.text = "Я в шоке..."
        lable.textColor = .gray
        return lable
    }()

    lazy var statusSet: UITextField = {
        let field = UITextField()
        field.font = UIFont.systemFont(ofSize: 15)
        field.textColor = .black
        field.backgroundColor = .white
        field.layer.cornerRadius = 12
        field.layer.borderWidth = 1
        field.layer.borderColor = UIColor.black.cgColor
        field.leftView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: field.frame.height))
        field.rightView = UIView(frame: CGRect(x: 0, y: 0, width: 15, height: field.frame.height))
        field.leftViewMode = .always
        field.rightViewMode = .always
        field.delegate = self
        field.addTarget(self, action: #selector(statusTextChanged), for: .editingChanged)
        return field
    }()
    
    override init(reuseIdentifier: String?) {
        super .init(reuseIdentifier: reuseIdentifier)
    }
    
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    
    @objc func pressStatusButtom(_ sender:Any) {
        status.text = statusText
        print(status.text ?? "Статус отсутствует")
    }
    
    @objc func statusTextChanged(_ textField: UITextField){
        statusText = textField.text ?? ""
    }
    
    override func layoutSubviews() {
        super .layoutSubviews()
        
        contentView.addSubviews(avatarView, name, statusButtom!, status, statusSet)
        avatarView.snp.makeConstraints { maker in
            maker.top.equalToSuperview().inset(16)
            maker.leading.equalToSuperview().inset(16)
            maker.height.equalTo(100)
            maker.width.equalTo(100)
        }
        
        avatarView.addSubviews(avatar)
        avatar.snp.makeConstraints { maker in
            maker.top.equalToSuperview()
            maker.leading.equalToSuperview()
            maker.trailing.equalToSuperview()
            maker.bottom.equalToSuperview()
        }
        name.snp.makeConstraints { marker in
            marker.top.equalToSuperview().inset(27)
            marker.leading.equalTo(avatar.snp.trailing).offset(16)
            marker.trailing.equalToSuperview().inset(16)
        }
        statusButtom!.snp.makeConstraints { marker in
            marker.top.equalTo(avatar.snp.bottom).offset(16)
            marker.leading.equalToSuperview().offset(16)
            marker.trailing.equalToSuperview().inset(16)
        }
        status.snp.makeConstraints { marker in
            marker.bottom.equalTo(statusSet.snp.top).offset(-10)
            marker.leading.equalTo(avatar.snp.trailing).offset(16)
            marker.trailing.equalToSuperview().inset(16)
        }
        statusSet.snp.makeConstraints { marker in
            marker.bottom.equalTo(statusButtom!.snp.top).offset(-8)
            marker.leading.equalTo(avatar.snp.trailing).offset(16)
            marker.trailing.equalToSuperview().inset(16)
            marker.height.equalTo(40)
        }
    }
}

extension ProfileHeaderView: UITextFieldDelegate{
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        endEditing(true)
        pressStatusButtom(endEditing(true))
        return true
    }
}
