use crate::python::{
    async_generator, async_python_run, deepcopy, format_err, serde_pickle, serde_pickle_opt,
    AsyncIterator, PyEnv,
};
use aqora_config::AqoraUseCaseConfig;
use futures::prelude::*;
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyIterator, PyNone, PyTuple},
};
use serde::{Deserialize, Serialize};
use split_stream_by::{Either, SplitStreamByMapExt};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct LayerFunction {
    func: PyObject,
    takes_input_arg: bool,
    takes_original_input_kwarg: bool,
    takes_context_kwarg: bool,
}

impl LayerFunction {
    pub fn new<'py>(py: Python<'py>, func: &'py PyAny) -> PyResult<Self> {
        let inspect = py.import(intern!(py, "inspect"))?;
        let parameter_cls = inspect.getattr(intern!(py, "Parameter"))?;
        let positional_only = parameter_cls.getattr(intern!(py, "POSITIONAL_ONLY"))?;
        let positional_or_keyword = parameter_cls.getattr(intern!(py, "POSITIONAL_OR_KEYWORD"))?;
        let var_positional = parameter_cls.getattr(intern!(py, "VAR_POSITIONAL"))?;
        let var_keyword = parameter_cls.getattr(intern!(py, "VAR_KEYWORD"))?;

        let mut takes_input_arg = false;
        let mut takes_original_input_kwarg = false;
        let mut takes_context_kwarg = false;
        let parameters = PyIterator::from_object(
            inspect
                .getattr(intern!(py, "signature"))?
                .call1((func,))?
                .call_method0(intern!(py, "values"))?,
        )?;
        for parameter in parameters {
            let parameter = parameter?;
            let kind = parameter.getattr(intern!(py, "kind"))?;
            if kind.eq(positional_only)? || kind.eq(var_positional)? {
                takes_input_arg = true;
                continue;
            }
            if kind.eq(var_keyword)? {
                takes_original_input_kwarg = true;
                takes_context_kwarg = true;
                continue;
            }
            if kind.eq(positional_or_keyword)? && !takes_input_arg {
                takes_input_arg = true;
                continue;
            }
            let name = parameter.getattr(intern!(py, "name"))?;
            if name.eq(intern!(py, "original_input"))? {
                takes_original_input_kwarg = true;
            } else if name.eq(intern!(py, "context"))? {
                takes_context_kwarg = true;
            }
        }
        Ok(Self {
            func: func.to_object(py),
            takes_input_arg,
            takes_original_input_kwarg,
            takes_context_kwarg,
        })
    }

    pub async fn call(
        &self,
        input: &PyObject,
        original_input: &PyObject,
        context: &PyObject,
    ) -> PyResult<PyObject> {
        async_python_run!(|py| {
            let args = if self.takes_input_arg {
                PyTuple::new(py, [deepcopy(py, input.as_ref(py))?])
            } else {
                PyTuple::empty(py)
            };
            let kwargs = PyDict::new(py);
            if self.takes_original_input_kwarg {
                kwargs.set_item(
                    intern!(py, "original_input"),
                    deepcopy(py, original_input.as_ref(py))?,
                )?;
            }
            if self.takes_context_kwarg {
                kwargs.set_item(intern!(py, "context"), deepcopy(py, context.as_ref(py))?)?;
            }
            self.func.as_ref(py).call(args, Some(kwargs))
        })?
        .await
    }
}

#[derive(Debug, Clone)]
pub struct Layer {
    name: String,
    transform: Option<LayerFunction>,
    context: Option<LayerFunction>,
    metric: Option<LayerFunction>,
    branch: Option<LayerFunction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct LayerEvaluation {
    #[serde(with = "serde_pickle")]
    transform: PyObject,
    #[serde(with = "serde_pickle")]
    context: PyObject,
    #[serde(with = "serde_pickle_opt")]
    metric: Option<PyObject>,
    #[serde(with = "serde_pickle_opt")]
    branch: Option<PyObject>,
}

impl LayerEvaluation {
    fn branch_str(&self) -> PyResult<Option<String>> {
        self.branch
            .as_ref()
            .map(|branch| Python::with_gil(|py| branch.extract::<String>(py)))
            .transpose()
    }
}

#[pymethods]
impl LayerEvaluation {
    fn __getitem__(&self, key: &str) -> Option<&PyObject> {
        match key {
            "output" => Some(&self.transform),
            "context" => Some(&self.context),
            "metric" => self.metric.as_ref(),
            "branch" => self.branch.as_ref(),
            _ => None,
        }
    }
    #[getter]
    fn output(&self) -> &PyObject {
        &self.transform
    }
    #[getter]
    fn context(&self) -> &PyObject {
        &self.context
    }
    #[getter]
    fn metric(&self) -> Option<&PyObject> {
        self.metric.as_ref()
    }
    #[getter]
    fn branch(&self) -> Option<&PyObject> {
        self.branch.as_ref()
    }
}

pub type EvaluationResult = HashMap<String, Vec<LayerEvaluation>>;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct EvaluateInputInfo {
    #[serde(with = "serde_pickle_opt")]
    pub input: Option<PyObject>,
    pub result: EvaluationResult,
    pub error: Option<EvaluationError>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct EvaluateAllInfo {
    #[serde(with = "serde_pickle_opt")]
    pub score: Option<PyObject>,
    pub num_inputs: u32,
}

impl Layer {
    pub async fn evaluate(
        &self,
        input: &PyObject,
        original_input: &PyObject,
        context: &PyObject,
    ) -> PyResult<LayerEvaluation> {
        let context = if let Some(context_transform) = self.context.as_ref() {
            context_transform
                .call(input, original_input, context)
                .await?
        } else {
            input.clone()
        };
        let transform = if let Some(transform) = self.transform.as_ref() {
            transform.call(input, original_input, &context).await?
        } else {
            input.clone()
        };
        let metric = if let Some(metric) = self.metric.as_ref() {
            Some(metric.call(&transform, original_input, &context).await?)
        } else {
            None
        };

        let branch = if let Some(branch) = self.branch.as_ref() {
            Some(branch.call(&transform, original_input, &context).await?)
        } else {
            None
        };

        Ok(LayerEvaluation {
            transform,
            context,
            metric,
            branch,
        })
    }

    pub async fn assert_metric(&self, evaluation: &LayerEvaluation) -> PyResult<()> {
        todo!()
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
        let original_input = input.clone();
        let mut context = Python::with_gil(|py| PyNone::get(py).into_py(py));
        let mut layer_index = 0;
        while layer_index < self.layers.len() {
            let layer = &self.layers[layer_index];
            let result = try_or_bail!(layer.evaluate(&input, &original_input, &context).await);
            if let Some(branch) = try_or_bail!(result.branch_str()) {
                layer_index = try_or_bail!(self
                    .layers
                    .iter()
                    .position(|layer| layer.name == branch)
                    .ok_or_else(|| EvaluationError::LayerNotFound(branch)));
            } else {
                layer_index += 1;
            }
            input = result.transform.clone();
            context = result.context.clone();
            out.entry(layer.name.clone()).or_default().push(result);
        }
        Ok(out)
    }

    pub async fn assert_metric(&self, results: &EvaluationResult) -> Result<(), EvaluationError> {
        let mut result_indexes = HashMap::<String, usize>::new();
        let mut layer_index = 0;
        while layer_index < self.layers.len() {
            let layer = &self.layers[layer_index];
            let layer_name = &layer.name;
            let result_index = *result_indexes.entry(layer_name.clone()).or_insert(0);
            let result = results
                .get(layer_name)
                .ok_or_else(|| EvaluationError::LayerNotFound(layer_name.clone()))?
                .get(result_index)
                .ok_or_else(|| EvaluationError::LayerNotFound(layer_name.clone()))?;
            layer.assert_metric(result).await?;
            result_indexes.insert(layer_name.clone(), result_index + 1);
            if let Some(branch) = result.branch_str()? {
                layer_index = self
                    .layers
                    .iter()
                    .position(|layer| layer.name == branch)
                    .ok_or_else(|| EvaluationError::LayerNotFound(branch))?;
            } else {
                layer_index += 1;
            }
        }
        Ok(())
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
    layers: Vec<Layer>,
    config: PipelineConfig,
}

impl Pipeline {
    pub fn import(
        env: &PyEnv,
        use_case: &AqoraUseCaseConfig,
        config: PipelineConfig,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let generator = env.import_path(py, &use_case.generator)?.into_py(py);
            let aggregator = env.import_path(py, &use_case.aggregator)?.into_py(py);
            let layers = use_case
                .layers
                .iter()
                .map(|layer| {
                    let transform = layer
                        .transform
                        .as_ref()
                        .map(|def| LayerFunction::new(py, env.import_path(py, &def.path)?))
                        .transpose()?;
                    let context = layer
                        .context
                        .as_ref()
                        .map(|def| LayerFunction::new(py, env.import_path(py, &def.path)?))
                        .transpose()?;
                    let metric = layer
                        .metric
                        .as_ref()
                        .map(|def| LayerFunction::new(py, env.import_path(py, &def.path)?))
                        .transpose()?;
                    let branch = layer
                        .branch
                        .as_ref()
                        .map(|def| LayerFunction::new(py, env.import_path(py, &def.path)?))
                        .transpose()?;
                    Ok(Layer {
                        name: layer.name.clone(),
                        transform,
                        context,
                        metric,
                        branch,
                    })
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Self {
                generator,
                aggregator,
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
}
