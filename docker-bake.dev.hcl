variable "TAG" {
  default = "dev"
}

target "docker-metadata-kubimo" {
  tags = ["ghcr.io/aqora-io/cli-kubimo:${TAG}"]
  args = {
    KUBIMO_MARIMO_IMAGE = "ghcr.io/aqora-io/kubimo-marimo:${TAG}"
  }
}

