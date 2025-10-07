variable "TAG" {
  default = "dev"
}

target "docker-metadata-kubimo" {
  tags = ["ghcr.io/aqora-io/cli-kubimo:${TAG}"]
}

