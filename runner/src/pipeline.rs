use crate::python::{
    async_generator, async_python_run, deepcopy, format_err, serde_pickle, AsyncIterator, PyEnv,
};
use aqora_config::{AqoraSubmissionConfig, AqoraUseCaseConfig, PathStr, PathStrReplaceError};
use futures::prelude::*;
use pyo3::{prelude::*, types::PyString};
use serde::{Deserialize, Serialize};
use split_stream_by::{Either, SplitStreamByMapExt};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct LayerFunction {
    func: PyObject,
    context: bool,
}

impl LayerFunction {
    pub async fn call(
        &self,
        input: &PyObject,
        context: &PyObject,
    ) -> PyResult<LayerFunctionResult> {
        let input = deepcopy(input)?;
        let output = if self.context {
            async_python_run!(|py| self.func.as_ref(py).call1((input, context)))?.await?
        } else {
            async_python_run!(|py| self.func.as_ref(py).call1((input,)))?.await?
        };
        Ok(LayerFunctionResult {
            output,
            context: deepcopy(context)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Layer {
    name: String,
    transform: Option<LayerFunction>,
    metric: Option<LayerFunction>,
    branch: Option<LayerFunction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct LayerFunctionResult {
    #[serde(with = "serde_pickle")]
    output: PyObject,
    #[serde(with = "serde_pickle")]
    context: PyObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct LayerEvaluation {
    output: LayerFunctionResult,
    metric: Option<LayerFunctionResult>,
    branch: Option<LayerFunctionResult>,
}

impl LayerEvaluation {
    fn branch_str(&self) -> PyResult<Option<String>> {
        self.branch
            .as_ref()
            .map(|branch| Python::with_gil(|py| branch.output.extract::<String>(py)))
            .transpose()
    }

    fn next_input(&self) -> PyObject {
        self.output.output.clone()
    }
}

#[pymethods]
impl LayerEvaluation {
    fn __getitem__(&self, key: &str) -> Option<&PyObject> {
        match key {
            "output" => Some(&self.output.output),
            "metric" => self.metric.as_ref().map(|metric| &metric.output),
            "branch" => self.branch.as_ref().map(|branch| &branch.output),
            _ => None,
        }
    }
    #[getter]
    fn output(&self) -> &PyObject {
        &self.output.output
    }
    #[getter]
    fn metric(&self) -> Option<&PyObject> {
        self.metric.as_ref().map(|metric| &metric.output)
    }
    #[getter]
    fn branch(&self) -> Option<&PyObject> {
        self.metric.as_ref().map(|branch| &branch.output)
    }
}

pub type EvaluationResult = HashMap<String, Vec<LayerEvaluation>>;

impl Layer {
    pub async fn evaluate(
        &self,
        input: &PyObject,
        context: &PyObject,
    ) -> PyResult<LayerEvaluation> {
        let output = if let Some(transform) = self.transform.as_ref() {
            transform.call(input, context).await?
        } else {
            LayerFunctionResult {
                output: input.clone(),
                context: context.clone(),
            }
        };

        let metric = if let Some(metric) = self.metric.as_ref() {
            Some(metric.call(&output.output, context).await?)
        } else {
            None
        };

        let branch = if let Some(branch) = self.branch.as_ref() {
            Some(branch.call(&output.output, context).await?)
        } else {
            None
        };

        Ok(LayerEvaluation {
            output,
            metric,
            branch,
        })
    }
}

#[derive(Clone, Debug)]
#[pyclass]
pub struct PipelineConfig {
    pub data: PathBuf,
}

impl PipelineConfig {
    fn py_data<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        py.import("pathlib")?.getattr("Path")?.call1((&self.data,))
    }
}

#[pymethods]
impl PipelineConfig {
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<&'py PyAny>> {
        match key {
            "data" => self.py_data(py).map(Some),
            _ => Ok(None),
        }
    }
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.py_data(py)
    }
}

#[derive(Clone, Debug)]
pub struct Evaluator {
    config: PipelineConfig,
    context: Option<PyObject>,
    layers: Vec<Layer>,
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum EvaluationError {
    #[error("{}", format_err(_0))]
    Python(
        #[from]
        #[serde(
            serialize_with = "serde_pickle::serialize",
            deserialize_with = "serde_pickle::deserialize_pyerr"
        )]
        PyErr,
    ),
    #[error("Layer not found: {0}")]
    LayerNotFound(String),
}

impl Evaluator {
    pub async fn evaluate(
        &self,
        mut input: PyObject,
    ) -> Result<EvaluationResult, (EvaluationResult, EvaluationError)> {
        let mut out = EvaluationResult::new();
        macro_rules! try_or_bail {
            ($expression:expr) => {{
                match $expression {
                    Ok(out) => out,
                    Err(err) => return Err((out, EvaluationError::from(err))),
                }
            }};
        }
        let context = if let Some(context) = self.context.as_ref() {
            let context_input = try_or_bail!(deepcopy(&input));
            try_or_bail!(
                try_or_bail!(async_python_run!(|py| context
                    .as_ref(py)
                    .call1((context_input, self.config.clone().into_py(py)))))
                .await
            )
        } else {
            try_or_bail!(deepcopy(&input))
        };
        let mut layer_index = 0;
        while layer_index < self.layers.len() {
            let layer = &self.layers[layer_index];
            let result = try_or_bail!(layer.evaluate(&input, &context).await);
            if let Some(branch) = try_or_bail!(result.branch_str()) {
                layer_index = try_or_bail!(self
                    .layers
                    .iter()
                    .position(|layer| layer.name == branch)
                    .ok_or_else(|| EvaluationError::LayerNotFound(branch)));
            } else {
                layer_index += 1;
            }
            input = result.next_input();
            out.entry(layer.name.clone()).or_default().push(result);
        }
        Ok(out)
    }

    pub fn evaluate_all(
        self,
        inputs: impl Stream<Item = PyResult<PyObject>>,
    ) -> impl Stream<Item = Result<EvaluationResult, (EvaluationResult, EvaluationError)>> {
        let this = Arc::new(self);
        inputs
            .map_err(|err| (EvaluationResult::new(), EvaluationError::Python(err)))
            .map_ok(move |input| (input, this.clone()))
            .and_then(|(input, evaluator)| async move { evaluator.evaluate(input).await })
    }
}

pub struct Pipeline {
    generator: PyObject,
    aggregator: PyObject,
    context: Option<PyObject>,
    layers: Vec<Layer>,
    config: PipelineConfig,
}

#[derive(Error, Debug)]
pub enum PipelineImportError {
    #[error("{}", format_err(_0))]
    Python(#[from] PyErr),
    #[error(transparent)]
    PathStrReplace(#[from] PathStrReplaceError),
}

impl Pipeline {
    pub fn import(
        _: &PyEnv,
        use_case: &AqoraUseCaseConfig,
        submission: &AqoraSubmissionConfig,
        config: PipelineConfig,
    ) -> Result<Self, PipelineImportError> {
        Python::with_gil(|py| {
            let generator =
                Self::import_path(py, &use_case.generator, &submission.refs)?.into_py(py);
            let aggregator =
                Self::import_path(py, &use_case.aggregator, &submission.refs)?.into_py(py);
            let context = use_case
                .context
                .as_ref()
                .map(|path| {
                    Result::<_, PipelineImportError>::Ok(
                        Self::import_path(py, path, &submission.refs)?.into_py(py),
                    )
                })
                .transpose()?;
            let layers = use_case
                .layers
                .iter()
                .map(|layer| {
                    let transform = layer
                        .transform
                        .as_ref()
                        .map(|def| {
                            Result::<_, PipelineImportError>::Ok(LayerFunction {
                                func: Self::import_path(py, &def.path, &submission.refs)?
                                    .into_py(py),
                                context: def.context,
                            })
                        })
                        .transpose()?;
                    let metric = layer
                        .metric
                        .as_ref()
                        .map(|def| {
                            Result::<_, PipelineImportError>::Ok(LayerFunction {
                                func: Self::import_path(py, &def.path, &submission.refs)?
                                    .into_py(py),
                                context: def.context,
                            })
                        })
                        .transpose()?;
                    let branch = layer
                        .branch
                        .as_ref()
                        .map(|def| {
                            Result::<_, PipelineImportError>::Ok(LayerFunction {
                                func: Self::import_path(py, &def.path, &submission.refs)?
                                    .into_py(py),
                                context: def.context,
                            })
                        })
                        .transpose()?;
                    Ok(Layer {
                        name: layer.name.clone(),
                        transform,
                        metric,
                        branch,
                    })
                })
                .collect::<Result<Vec<_>, PipelineImportError>>()?;
            Ok(Self {
                generator,
                aggregator,
                context,
                layers,
                config,
            })
        })
    }

    pub fn generator(&self) -> PyResult<impl Stream<Item = PyResult<PyObject>>> {
        let generator = Python::with_gil(|py| {
            PyResult::Ok(
                self.generator
                    .as_ref(py)
                    .call1((self.config.clone().into_py(py),))?
                    .into_py(py),
            )
        })?;
        async_generator(generator)
    }

    pub fn evaluator(&self) -> Evaluator {
        Evaluator {
            context: self.context.clone(),
            layers: self.layers.clone(),
            config: self.config.clone(),
        }
    }

    pub async fn aggregate(
        &self,
        results: impl Stream<Item = Result<EvaluationResult, (EvaluationResult, EvaluationError)>>
            + Send
            + Sync
            + 'static,
    ) -> Result<Option<PyObject>, EvaluationError> {
        let (errs, results) = results.boxed().split_by_map(move |result| match result {
            Ok(result) => Python::with_gil(|py| Either::Right(result.into_py(py))),
            Err((_, err)) => Either::Left(Result::<PyObject, _>::Err(err)),
        });
        let iterator = AsyncIterator::new(results);
        let result = futures::stream::once(
            async_python_run!(|py| self.aggregator.as_ref(py).call1((iterator,)))?
                .map_err(EvaluationError::Python),
        )
        .boxed();
        let mut out_stream = futures::stream::select(result, errs);
        out_stream.next().await.transpose()
    }

    fn import_path<'py>(
        py: Python<'py>,
        path: &PathStr,
        refs: &HashMap<String, PathStr>,
    ) -> Result<&'py PyAny, PipelineImportError> {
        let path = path.replace_refs(refs)?;
        let module = PyModule::import(py, PyString::new(py, &path.module().to_string()))?;
        Ok(module.getattr(PyString::new(py, path.name()))?)
    }
}
