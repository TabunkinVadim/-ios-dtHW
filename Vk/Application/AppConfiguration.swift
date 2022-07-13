//
//  AppConfiguration.swift
//  Vk
//
//  Created by Табункин Вадим on 12.07.2022.
//

import Foundation

enum AppConfiguration: CaseIterable {
    static var allCases: [AppConfiguration] = [people(URL(string: "https://swapi.dev/api/people/")!), planets(URL(string: "https://swapi.dev/api/planets/")!), species(URL(string: "https://swapi.dev/api/species/")!)]

    case people (URL)
    case planets (URL)
    case species (URL)
}
extension CaseIterable {
    static func randomElement() -> AllCases.Element {
        guard Self.allCases.count > 0 else {
            fatalError("There must be at least one case in the enum")
        }
        return Self.allCases.randomElement()!
    }
}
