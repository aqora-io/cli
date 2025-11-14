group "default" {
  targets = ["kubimo"]
}

target "docker-metadata-kubimo" {}

target "kubimo" {
  inherits = ["docker-metadata-kubimo"]
  context = "."
  dockerfile = "docker/Dockerfile.kubimo"
}
