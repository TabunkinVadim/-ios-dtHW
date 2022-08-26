//
//  InfoViewController.swift
//  Navigation
//
//  Created by Табункин Вадим on 03.03.2022.
//

import UIKit

class InfoViewController: UIViewController {
    weak var coordinator: FeedCoordinator?
    let url1 = "https://jsonplaceholder.typicode.com/todos/"
    let url2 = "https://swapi.dev/api/planets/1"
    let screenWidth = UIScreen.main.bounds.width
    let screenHeight = UIScreen.main.bounds.height
    var residents: [Resident] = []
    let lableURL1 :UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.boldSystemFont(ofSize: 20)
        $0.textColor = .black
        $0.numberOfLines = 2
        return $0
    } (UILabel())

    let lableURL2 :UILabel = {
        $0.toAutoLayout()
        $0.font = UIFont.boldSystemFont(ofSize: 20)
        $0.textColor = .black
        $0.numberOfLines = 2
        return $0
    } (UILabel())

    private lazy var tableView: UITableView = {
        $0.toAutoLayout()
        $0.dataSource = self
        $0.delegate = self
        $0.backgroundColor = .white
        return $0
    }(UITableView(frame: .zero, style: .plain))

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
        requestUrl1()
        requestUrl2()
    }

    func requestUrl1() {
        guard let url = URL(string: url1 ) else { return}
        let sessions = URLSession.shared
        let task = sessions.dataTask(with: url ){data, response, error in
            guard let data = data else { return }
            do {
                let json = try JSONSerialization.jsonObject(with: data, options: []) as? [[String: Any]]
                DispatchQueue.main.async {
                    self.lableURL1.text = (json![1]["title"]) as? String
                }
            } catch let error  {
                print(error)
            }
        }
        task.resume()
    }

    func requestUrl2() {
        guard let url = URL(string: url2 ) else { return}
        let sessions = URLSession.shared
        let task = sessions.dataTask(with: url ){data, response, error in
            guard let data = data else { return }
            do {
                let planet = try JSONDecoder().decode(Planet.self, from: data)
                DispatchQueue.main.async {
                    self.lableURL2.text = planet.orbitalPeriod
                }
                let residentsURL = planet.residents
                self.addResidents(residentsURL: residentsURL)
            } catch let error  {
                print(error)
            }
        }
        task.resume()
    }

    func addResidents(residentsURL:[String]){
        for url in residentsURL{
            guard let url = URL(string: url ) else { return}
            let sessions = URLSession.shared
            let task = sessions.dataTask(with: url) { data, response, error in
                guard let data = data else { return }
                do {
                    let resident = try JSONDecoder().decode(Resident.self, from: data)
                    self.residents.append(resident)
                    DispatchQueue.main.async {
                        self.tableView.reloadData()
                    }
                } catch let error {
                    print(error)
                }
            }
            task.resume()
        }
    }

    func layout () {
        alertButton!.toAutoLayout()
        view.addSubviews(alertButton!, lableURL1, lableURL2, tableView)
        NSLayoutConstraint.activate([
            alertButton!.bottomAnchor.constraint(equalTo: view.bottomAnchor, constant: -150),
            alertButton!.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 40),
            alertButton!.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -40),
            alertButton!.heightAnchor.constraint(equalToConstant: screenHeight / 9)
        ])

        NSLayoutConstraint.activate([
            lableURL1.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 20),
            lableURL1.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 40),
            lableURL1.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -40)
        ])
        NSLayoutConstraint.activate([
            lableURL2.topAnchor.constraint(equalTo:lableURL1.bottomAnchor , constant: 20),
            lableURL2.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 40),
            lableURL2.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -40)
        ])

        NSLayoutConstraint.activate([
            tableView.topAnchor.constraint(equalTo:lableURL2.bottomAnchor , constant: 20),
            tableView.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 40),
            tableView.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -40),
            tableView.bottomAnchor.constraint(equalTo: alertButton!.topAnchor, constant: -40)
        ])
    }
}
extension InfoViewController: UITableViewDataSource {
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        residents.count
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = UITableViewCell()
        cell.textLabel?.text = "\(indexPath.row + 1). \(residents[indexPath.row].name)"
        return cell
    }
    func tableView(_ tableView: UITableView, titleForHeaderInSection section: Int) -> String? {
        "Жители планеты Таурин"
    }
}

extension InfoViewController: UITableViewDelegate{
    
}
