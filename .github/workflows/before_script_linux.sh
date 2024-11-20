#!/bin/bash

case "$1" in
"aarch64" | "armv7" | "s390x" | "ppc64le")
  # NOTE: pypa/manylinux docker images are Debian based
  sudo apt-get update
  sudo apt-get install -y libgit2-dev
  ;;
"x86" | "x86_64")
  # NOTE: rust-cross/manylinux docker images are CentOS based
  yum update -y
  yum install -y libgit2-devel perl-IPC-Cmd
  ;;
*)
  echo "Unsupported target: $1"
  exit 1
  ;;
esac
