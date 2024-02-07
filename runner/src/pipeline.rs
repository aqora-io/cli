use crate::{
    pyproject::{AqoraSubmissionConfig, AqoraUseCaseConfig, PathStr, PathStrReplaceError},
    python::{async_generator, async_python_run, deepcopy, AsyncIterator},
};
use futures::prelude::*;
use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};
use split_stream_by::{Either, SplitStreamByMapExt};
use std::{collections::HashMap, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct LayerFunction {
    func: PyObject,
    context: bool,
}

impl LayerFunction {
    pub async fn call(&self, input: &PyObject, context: &PyObject) -> PyResult<PyObject> {
        let input = deepcopy(input)?;
        if self.context {
            Ok(async_python_run!(|py| self.func.as_ref(py).call1((input, context)))?.await?)
        } else {
            Ok(async_python_run!(|py| self.func.as_ref(py).call1((input,)))?.await?)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Layer {
    name: String,
    transform: Option<LayerFunction>,
    metric: Option<LayerFunction>,
    branch: Option<LayerFunction>,
}

#[derive(Debug)]
pub struct LayerEvaluation {
    output: PyObject,
    metric: Option<PyObject>,
    branch: Option<String>,
}

pub type EvaluationResult = HashMap<String, Vec<LayerEvaluation>>;

impl ToPyObject for LayerEvaluation {
    fn to_object(&self, py: Python) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("output", self.output.clone()).unwrap();
        if let Some(metric) = self.metric.as_ref() {
            dict.set_item("metric", metric).unwrap();
        }
        if let Some(branch) = self.branch.as_ref() {
            dict.set_item("branch", branch).unwrap();
        }
        dict.into()
    }
}

impl Layer {
    pub async fn evaluate(
        &self,
        input: &PyObject,
        context: &PyObject,
    ) -> PyResult<LayerEvaluation> {
        let output = if let Some(transform) = self.transform.as_ref() {
            transform.call(input, context).await?
        } else {
            input.clone()
        };

        let metric = if let Some(metric) = self.metric.as_ref() {
            Some(metric.call(&output, context).await?)
        } else {
            None
        };

        let branch = if let Some(branch) = self.branch.as_ref() {
            let branch = branch.call(&output, context).await?;
            Some(Python::with_gil(|py| branch.extract::<String>(py))?)
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
pub struct PipelineConfig {
    pub data: PathBuf,
}

impl ToPyObject for PipelineConfig {
    fn to_object(&self, py: Python) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("data", self.data.clone()).unwrap();
        dict.into()
    }
}

#[derive(Clone, Debug)]
pub struct Evaluator {
    config: PipelineConfig,
    context: Option<PyObject>,
    layers: Vec<Layer>,
}

#[derive(Error, Debug)]
pub enum EvaluationError {
    #[error(transparent)]
    Python(#[from] PyErr),
    #[error("Layer not found: {0}")]
    LayerNotFound(String),
}

impl Evaluator {
    pub async fn evaluate(&self, input: PyObject) -> Result<EvaluationResult, EvaluationError> {
        let mut input = input;
        let context = if let Some(context) = self.context.as_ref() {
            let context_input = deepcopy(&input)?;
            async_python_run!(|py| context
                .as_ref(py)
                .call1((context_input, self.config.to_object(py))))?
            .await?
        } else {
            deepcopy(&input)?
        };
        let mut out = EvaluationResult::new();
        let mut layer_index = 0;
        while layer_index < self.layers.len() {
            let layer = &self.layers[layer_index];
            let result = layer.evaluate(&input, &context).await?;
            input = result.output.clone();
            if let Some(branch) = result.branch.as_ref() {
                layer_index = self
                    .layers
                    .iter()
                    .position(|layer| layer.name == *branch)
                    .ok_or_else(|| EvaluationError::LayerNotFound(branch.clone()))?;
            } else {
                layer_index += 1;
            }
            out.entry(layer.name.clone()).or_default().push(result);
        }
        Ok(out)
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
    #[error(transparent)]
    Python(#[from] PyErr),
    #[error(transparent)]
    PathStrReplace(#[from] PathStrReplaceError),
}

impl Pipeline {
    pub fn import(
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
                    .call1((self.config.to_object(py),))?
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

    pub fn evaluate(
        &self,
        inputs: impl Stream<Item = PyResult<PyObject>>,
        evaluator: Evaluator,
    ) -> impl Stream<Item = Result<EvaluationResult, EvaluationError>> {
        inputs
            .map_err(EvaluationError::Python)
            .map_ok(move |input| (input, evaluator.clone()))
            .and_then(|(input, evaluator)| async move { evaluator.evaluate(input).await })
    }

    pub async fn aggregate(
        &self,
        results: impl Stream<Item = Result<EvaluationResult, EvaluationError>> + Send + Sync + 'static,
    ) -> Result<Option<PyObject>, EvaluationError> {
        let (errs, results) = results.boxed().split_by_map(move |result| match result {
            Ok(result) => Python::with_gil(|py| Either::Right(result.to_object(py))),
            Err(err) => Either::Left(Result::<PyObject, _>::Err(err)),
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
