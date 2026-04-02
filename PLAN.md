# Aqora Qiskit Provider Plan

(you can look at <https://github.com/scaleway/qiskit-scaleway> for inspiration. A slight difference is that sessions are automatically handled by the aqora backend. You can look at ~/Development/aqora/platform/backend for information)

- Add qiskit as optional dependency group
  - This will add qio and qiskit as dependencies
- Add a qiskit/ module to python/aqora_cli
- Add a backend that implements BackendV2 from qiskit.
  - This backend will be based on what is returned from list_platforms in the graphql schema
- Add a job that implements JobV1 from qiskit
  - This will first create a model, and then submit and poll for results via graphql
  - Payloads can be found in qio
- Add estimator and sampler primitives to the module as well
