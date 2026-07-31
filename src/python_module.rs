use std::{borrow::Cow, ffi::OsString, sync::Arc};

use aqora_client::{s3::S3Range, Client, ClientOptions};
use aqora_runner::pipeline::{LayerEvaluation, PipelineConfig};
use pyo3::{
    exceptions::PyValueError,
    import_exception,
    prelude::*,
    types::{PyBytes, PyDict, PyString},
};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::RwLock;
use url::Url;

use crate::{
    dirs::config_home,
    graphql_client::{authenticate_client, unauthenticated_client},
    workspace::download_workspace_notebook,
};

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();
    let sys = py.import("sys")?;
    let argv = sys.getattr("argv")?.extract::<Vec<OsString>>()?;
    let exit_code = py.detach(|| crate::run(argv));
    sys.getattr("exit")?.call1((exit_code,))?;
    Ok(())
}

import_exception!(aqora, ClientError);

fn program_format(value: u16) -> PyResult<qio::QuantumProgramSerializationFormat> {
    serde_json::from_value(value.into()).map_err(|_| {
        PyValueError::new_err(format!(
            "unknown quantum program serialization format: {value}"
        ))
    })
}

/// Build a qio `QuantumComputationModel` JSON payload from `(serialization,
/// serialization_format)` pairs, optionally zlib+base64 compressing each
/// program.
#[pyfunction]
#[pyo3(signature = (programs, *, user_agent, backend_name, compress))]
fn qio_build_model_payload(
    programs: Vec<(String, u16)>,
    user_agent: String,
    backend_name: String,
    compress: bool,
) -> PyResult<String> {
    let programs = programs
        .into_iter()
        .map(|(serialization, format)| {
            let program = qio::QuantumProgram::new(serialization, program_format(format)?);
            if compress {
                program
                    .to_compression(qio::CompressionFormat::ZlibBase64V1)
                    .map_err(|error| PyValueError::new_err(error.to_string()))
            } else {
                Ok(program)
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    qio::QuantumComputationModel {
        programs,
        noise_model: None,
        client: Some(qio::ClientData { user_agent }),
        backend: Some(qio::BackendData {
            name: backend_name,
            version: None,
            options: None,
        }),
    }
    .to_json_str()
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

/// Parse a qio result payload into `(serialization_format, serialization)`,
/// decompressing as needed. The serialization format is kept as a raw integer
/// so formats newer than this build still pass through.
#[pyfunction]
fn qio_parse_result_payload(payload: &str) -> PyResult<(u16, String)> {
    let result: qio::Serialization<u16> = qio::parse_json_str(payload)
        .map_err(|error| PyValueError::new_err(format!("invalid result payload: {error}")))?;
    let result = result
        .to_compression(qio::CompressionFormat::None)
        .map_err(|error| PyValueError::new_err(format!("invalid result compression: {error}")))?;
    Ok((result.serialization_format, result.serialization))
}

#[pyclass(frozen, name = "Client", module = "aqora")]
struct PyClient {
    inner: Arc<RwLock<PyClientInner>>,
}

struct PyClientInner {
    client: Client,
    authenticated: bool,
}

fn get_environ<'py>(py: Python<'py>, key: &str) -> PyResult<Option<String>> {
    let os = py.import(pyo3::intern!(py, "os"))?;
    let value = os
        .getattr("environ")?
        .call_method1(pyo3::intern!(py, "get"), (key,))?;
    if value.is_none() {
        Ok(None)
    } else {
        Ok(Some(value.extract()?))
    }
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (url=None, *, allow_insecure_host=None))]
    fn new<'py>(
        py: Python<'py>,
        url: Option<&str>,
        allow_insecure_host: Option<bool>,
    ) -> PyResult<Self> {
        let url = url
            .map_or_else(
                || {
                    PyResult::Ok(
                        get_environ(py, "AQORA_URL")?
                            .map_or(Cow::Borrowed("https://aqora.io"), Cow::Owned),
                    )
                },
                |url| Ok(Cow::Borrowed(url)),
            )?
            .parse::<Url>()
            .map_err(|error| PyValueError::new_err((error.to_string(),)))?;
        let allow_insecure_host = allow_insecure_host.map_or_else(
            || {
                PyResult::Ok(
                    get_environ(py, "AQORA_ALLOW_INSECURE_HOST")?
                        .map(|value| value.parse::<bool>())
                        .transpose()
                        .map_err(|error| PyValueError::new_err(error.to_string()))?
                        .unwrap_or(false),
                )
            },
            |allow_insecure_host| Ok(allow_insecure_host),
        )?;
        let client = unauthenticated_client(
            url,
            ClientOptions {
                allow_insecure_host,
            },
        )
        .map_err(|error| ClientError::new_err((error.message(),)))?;
        Ok(Self {
            inner: Arc::new(RwLock::new(PyClientInner {
                client,
                authenticated: false,
            })),
        })
    }

    fn authenticate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let config_home =
            config_home().map_err(|error| ClientError::new_err((error.message(),)))?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut inner = inner.write().await;
            let client = authenticate_client(config_home, inner.client.clone())
                .await
                .map_err(|error| ClientError::new_err((error.message(),)))?;
            *inner = PyClientInner {
                client,
                authenticated: true,
            };
            Ok(())
        })
    }

    #[pyo3(signature = (query, **variables))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'py, PyString>,
        variables: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py_json = py.import(pyo3::intern!(py, "json"))?;
        let body = PyDict::new(py);
        body.set_item("query", query)?;
        if let Some(variables) = variables {
            body.set_item("variables", variables)?;
        }
        let body: String = py_json
            .call_method1(pyo3::intern!(py, "dumps"), (body,))?
            .extract()?;

        let inner = Arc::clone(&self.inner);
        let py_json = py_json.unbind();
        future_into_py(py, async move {
            let inner = inner.read().await;
            let response = inner
                .client
                .send_raw(body)
                .await
                .map_err(|error| ClientError::new_err((error.to_string(),)))?;

            Python::attach(move |py| {
                let py_json = py_json.bind(py);

                let response = PyBytes::new(py, &response);
                let response = py_json.call_method1(pyo3::intern!(py, "loads"), (response,))?;
                let Ok(response) = response.cast::<PyDict>() else {
                    let error = format!(
                        "GraphQL returned unexpected value of type {}",
                        response.get_type()
                    );
                    return Err(ClientError::new_err(error));
                };

                if let Some(errors) = response.get_item(pyo3::intern!(py, "errors"))? {
                    Err(ClientError::new_err((
                        "GraphQL had errors",
                        errors.unbind(),
                    )))
                } else if let Some(data) = response.get_item(pyo3::intern!(py, "data"))? {
                    Ok(data.unbind())
                } else {
                    Err(ClientError::new_err("GraphQL returned an empty response"))
                }
            })
        })
    }

    #[pyo3(signature = (url, *, range=None))]
    fn s3_get<'py>(
        &self,
        py: Python<'py>,
        url: &str,
        range: Option<(Option<usize>, Option<usize>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = url
            .parse::<Url>()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let range = range.map_or(S3Range::FULL, |(lo, hi)| S3Range { lo, hi });
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let inner = inner.read().await;
            let response = inner
                .client
                .s3_get_range(url, range)
                .await
                .map_err(|error| ClientError::new_err(error.to_string()))?;
            let body = response
                .body
                .bytes()
                .await
                .map_err(|error| ClientError::new_err(error.to_string()))?;
            Python::attach(|py| {
                let body = PyBytes::new(py, &body);
                Ok(body.unbind())
            })
        })
    }

    #[pyo3(signature = (url, body, *, content_type=None))]
    fn s3_put<'py>(
        &self,
        py: Python<'py>,
        url: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = url
            .parse::<Url>()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let inner = inner.read().await;
            let response = inner
                .client
                .s3_put_with_content_type(url, body, content_type.as_deref())
                .await
                .map_err(|error| ClientError::new_err(error.to_string()))?;
            Python::attach(|py| {
                let etag = PyString::new(py, &response.etag);
                Ok(etag.unbind())
            })
        })
    }

    #[pyo3(signature = (owner, slug, dest_dir, notebook=None, version=None, force=false))]
    fn _download_workspace_notebook<'py>(
        &self,
        py: Python<'py>,
        owner: &str,
        slug: &str,
        dest_dir: std::path::PathBuf,
        notebook: Option<String>,
        version: Option<String>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let owner = owner.to_owned();
        let slug = slug.to_owned();
        future_into_py(py, async move {
            let client = inner.read().await.client.clone();
            let filename = download_workspace_notebook(
                client, owner, slug, dest_dir, notebook, version, force,
            )
            .await
            .map_err(|error| ClientError::new_err((error.message(),)))?;
            Ok(filename)
        })
    }

    #[getter]
    fn authenticated(&self) -> bool {
        self.inner.blocking_read().authenticated
    }
}

#[pymodule]
#[pyo3(name = "_aqora")]
pub fn aqora(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    use qio::{QuantumProgramResultSerializationFormat, QuantumProgramSerializationFormat};

    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    m.add_class::<PyClient>()?;
    m.add_function(wrap_pyfunction!(qio_build_model_payload, m)?)?;
    m.add_function(wrap_pyfunction!(qio_parse_result_payload, m)?)?;
    for (name, format) in [
        (
            "QIO_PROGRAM_UNKNOWN_SERIALIZATION_FORMAT",
            QuantumProgramSerializationFormat::UnknownSerializationFormat,
        ),
        (
            "QIO_PROGRAM_QASM_V1",
            QuantumProgramSerializationFormat::QasmV1,
        ),
        (
            "QIO_PROGRAM_QASM_V2",
            QuantumProgramSerializationFormat::QasmV2,
        ),
        (
            "QIO_PROGRAM_QASM_V3",
            QuantumProgramSerializationFormat::QasmV3,
        ),
        (
            "QIO_PROGRAM_QIR_V1",
            QuantumProgramSerializationFormat::QirV1,
        ),
        (
            "QIO_PROGRAM_CIRQ_CIRCUIT_JSON_V1",
            QuantumProgramSerializationFormat::CirqCircuitJsonV1,
        ),
        (
            "QIO_PROGRAM_PERCEVAL_CIRCUIT_JSON_V1",
            QuantumProgramSerializationFormat::PercevalCircuitJsonV1,
        ),
        (
            "QIO_PROGRAM_PULSER_SEQUENCE_JSON_V1",
            QuantumProgramSerializationFormat::PulserSequenceJsonV1,
        ),
        (
            "QIO_PROGRAM_TKET_CIRCUIT_JSON_V1",
            QuantumProgramSerializationFormat::TketCircuitJsonV1,
        ),
        (
            "QIO_PROGRAM_HUGR_V1",
            QuantumProgramSerializationFormat::HugrV1,
        ),
    ] {
        m.add(name, format as u16)?;
    }
    for (name, format) in [
        (
            "QIO_RESULT_CIRQ_RESULT_JSON_V1",
            QuantumProgramResultSerializationFormat::CirqResultJsonV1,
        ),
        (
            "QIO_RESULT_QISKIT_RESULT_JSON_V1",
            QuantumProgramResultSerializationFormat::QiskitResultJsonV1,
        ),
        (
            "QIO_RESULT_CUDAQ_SAMPLE_RESULT_JSON_V1",
            QuantumProgramResultSerializationFormat::CudaqSampleResultJsonV1,
        ),
        (
            "QIO_RESULT_PYTKET_BACKEND_RESULT_JSON_V1",
            QuantumProgramResultSerializationFormat::PytketBackendResultJsonV1,
        ),
        (
            "QIO_RESULT_QIR_LABELED_RESULT_V1",
            QuantumProgramResultSerializationFormat::QirLabeledResultV1,
        ),
        (
            "QIO_RESULT_QSYS_RESULT_JSON_V1",
            QuantumProgramResultSerializationFormat::QsysResultJsonV1,
        ),
    ] {
        m.add(name, format as u16)?;
    }
    Ok(())
}
