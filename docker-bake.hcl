group "default" {
  targets = ["kubimo"]
}

# Pin for the aqora PyPI package. Empty installs the latest published release;
# set from the env var of the same name (see the Release workflow, which only
# populates it for semver tag builds).
variable "AQORA_VERSION" {
  default = ""
}

target "docker-metadata-kubimo" {}

target "kubimo" {
  inherits = ["docker-metadata-kubimo"]
  context = "."
  dockerfile = "docker/Dockerfile.kubimo"
  args = {
    AQORA_VERSION = AQORA_VERSION
  }
}
