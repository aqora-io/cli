use std::{borrow::Cow, ffi::OsString, sync::Arc};

use aqora_client::{s3::S3Range, Client};
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
    graphql_client::{client as authenticated_client, unauthenticated_client},
};

#[pyfunction]
pub fn main(py: Python<'_>) -> PyResult<()> {
    let _sentry = crate::sentry::setup();
    let sys = py.import("sys")?;
    let argv = sys.getattr("argv")?.extract::<Vec<OsString>>()?;
    let exit_code = py.allow_threads(|| crate::run(argv));
    sys.getattr("exit")?.call1((exit_code,))?;
    Ok(())
}

import_exception!(aqora_cli, ClientError);

#[pyclass(frozen, name = "Client", module = "aqora_cli")]
struct PyClient {
    url: Url,
    inner: Arc<RwLock<PyClientInner>>,
}

struct PyClientInner {
    client: Client,
    authenticated: bool,
}

#[pymethods]
impl PyClient {
    #[new]
    #[pyo3(signature = (url=None))]
    fn new(url: Option<&str>) -> PyResult<Self> {
        let url = url
            .map_or_else(
                || {
                    std::env::var("AQORA_URL")
                        .ok()
                        .map_or(Cow::Borrowed("https://aqora.io"), Cow::Owned)
                },
                Cow::Borrowed,
            )
            .parse::<Url>()
            .map_err(|error| PyValueError::new_err((error.to_string(),)))?;

        let client = unauthenticated_client(url.clone())
            .map_err(|error| ClientError::new_err((error.message(),)))?;
        Ok(Self {
            url,
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
        let url = self.url.clone();
        future_into_py(py, async move {
            let mut inner = inner.write().await;
            let client = authenticated_client(config_home, url)
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

            Python::with_gil(move |py| {
                let py_json = py_json.bind(py);

                let response = PyBytes::new(py, &response);
                let response = py_json.call_method1(pyo3::intern!(py, "loads"), (response,))?;
                let Ok(response) = response.downcast::<PyDict>() else {
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
            Python::with_gil(|py| {
                let body = PyBytes::new(py, &body);
                Ok(body.unbind())
            })
        })
    }

    #[getter]
    fn authenticated(&self) -> bool {
        self.inner.blocking_read().authenticated
    }
}

#[pymodule]
#[pyo3(name = "_aqora_cli")]
pub fn aqora_cli(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_class::<PipelineConfig>()?;
    m.add_class::<LayerEvaluation>()?;
    m.add_class::<PyClient>()?;
    Ok(())
}
