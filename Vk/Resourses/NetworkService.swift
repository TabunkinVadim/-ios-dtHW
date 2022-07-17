//
//  NetworkService.swift
//  Vk
//
//  Created by Табункин Вадим on 12.07.2022.
//

import Foundation
import UIKit

struct NetworkService{
    static func request(for configuration: AppConfiguration) {
        let randomURL:URL
        switch configuration {
        case .people(let peopleURL):
            randomURL = peopleURL
        case .planets(let planetsURL):
        randomURL = planetsURL
        case .species(let speciesURL):
        randomURL = speciesURL
        }
        let session = URLSession.shared
        let task = session.dataTask(with: randomURL ) { data, response, error in
            let dateObj = try? JSONSerialization.jsonObject(with: data!) as! [String: Any]
            let responseObj = try? (response! as! HTTPURLResponse)
            print("________________________")
            print("Data:\n\n",dateObj)
            print("________________________")
            print("AllHeaderFields:\n\n",responseObj?.allHeaderFields)
            print("________________________")
            print("Status code:\n\n", responseObj?.statusCode)
        }
        task.resume()
    }
}
/*
Ошибка выпадающая без подключения к интернету
 2022-07-13 23:05:03.567272+0400 Vk[65016:13622470] Connection 1: received failure notification
 2022-07-13 23:05:03.568904+0400 Vk[65016:13622470] Connection 1: failed to connect 1:50, reason -1
 2022-07-13 23:05:03.573680+0400 Vk[65016:13622470] Connection 1: encountered error(1:50)
 2022-07-13 23:05:03.600919+0400 Vk[65016:13622470] Task <DFFA5478-CA0D-4E5A-BEA7-06A78D707D55>.<1> HTTP load failed, 0/0 bytes (error code: -1009 [1:50])
 2022-07-13 23:05:03.617544+0400 Vk[65016:13622474] Task <DFFA5478-CA0D-4E5A-BEA7-06A78D707D55>.<1> finished with error [-1009] Error Domain=NSURLErrorDomain Code=-1009 "The Internet connection appears to be offline." UserInfo={_kCFStreamErrorCodeKey=50, NSUnderlyingError=0x6000017ef690 {Error Domain=kCFErrorDomainCFNetwork Code=-1009 "(null)" UserInfo={_kCFStreamErrorCodeKey=50, _kCFStreamErrorDomainKey=1}}, _NSURLErrorFailingURLSessionTaskErrorKey=LocalDataTask <DFFA5478-CA0D-4E5A-BEA7-06A78D707D55>.<1>, _NSURLErrorRelatedURLSessionTaskErrorKey=(
     "LocalDataTask <DFFA5478-CA0D-4E5A-BEA7-06A78D707D55>.<1>"
 ), NSLocalizedDescription=The Internet connection appears to be offline., NSErrorFailingURLStringKey=https://swapi.dev/api/species/, NSErrorFailingURLKey=https://swapi.dev/api/species/, _kCFStreamErrorDomainKey=1}
 Vk/NetworkService.swift:25: Fatal error: Unexpectedly found nil while unwrapping an Optional value
 2022-07-13 23:05:03.619569+0400 Vk[65016:13622474] Vk/NetworkService.swift:25: Fatal error: Unexpectedly found nil while unwrapping an Optional value */
