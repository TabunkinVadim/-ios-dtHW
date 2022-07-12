# Uncomment the next line to define a global platform for your project
  platform :ios, '14.5'

target 'StorageService' do
  # Comment the next line if you don't want to use dynamic frameworks
  use_frameworks!

  # Pods for StorageService
  pod 'SnapKit'

  target 'StorageServiceTests' do
    # Pods for testing
  end

end

target 'Vk' do
  # Comment the next line if you don't want to use dynamic frameworks
  use_frameworks!

  # Pods for Vk
  pod 'SnapKit'

end

post_install do |installer_representation|
    installer_representation.pods_project.targets.each do |target|
        target.build_configurations.each do |config|
            config.build_settings['ALWAYS_EMBED_SWIFT_STANDARD_LIBRARIES'] = '$(inherited)'
        end
    end
end
