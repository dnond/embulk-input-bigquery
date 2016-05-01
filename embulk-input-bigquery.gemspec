
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-bigquery"
  spec.version       = "0.1.0"
  spec.authors       = ["dnond"]
  spec.summary       = "Bigquery input plugin for Embulk"
  spec.description   = "Loads records from Bigquery."
  spec.email         = ["tarosuk@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/tarosuk/embulk-input-bigquery"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'embulk', ['>= 0.8.5']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']

  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'bigquery-client'
end
