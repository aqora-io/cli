use crate::python::{
    async_generator, async_python_run, deepcopy, format_err, serde_pickle, serde_pickle_opt,
    AsyncIterator, PyEnv,
};
use aqora_config::{AqoraUseCaseConfig, FunctionDef};
use futures::prelude::*;
use pyo3::{
    exceptions::PyValueError,
    intern,
    prelude::*,
    pyclass,
    types::{PyDict, PyIterator, PyNone, PyTuple},
};
use serde::{Deserialize, Serialize};
use split_stream_by::{Either, SplitStreamByMapExt};
use std::{collections::HashMap, path::PathBuf};
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
                .getattr(intern!(py, "parameters"))?
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
pub enum LayerFunctionDef {
    None,
    Some(LayerFunction),
    UseDefault,
}

#[derive(Debug, Clone)]
pub struct Layer {
    name: String,
    transform: LayerFunctionDef,
    context: LayerFunctionDef,
    metric: LayerFunctionDef,
    branch: LayerFunctionDef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct LayerEvaluation {
    #[serde(with = "serde_pickle")]
    pub transform: PyObject,
    #[serde(with = "serde_pickle")]
    pub context: PyObject,
    #[serde(with = "serde_pickle_opt")]
    pub metric: Option<PyObject>,
    #[serde(with = "serde_pickle_opt")]
    pub branch: Option<PyObject>,
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
        default: Option<&LayerEvaluation>,
    ) -> PyResult<LayerEvaluation> {
        let context = match &self.context {
            LayerFunctionDef::Some(func) => func.call(input, original_input, context).await?,
            LayerFunctionDef::UseDefault => {
                if let Some(default) = default {
                    default.context.clone()
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Context function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => context.clone(),
        };
        let transform = match &self.transform {
            LayerFunctionDef::Some(func) => func.call(input, original_input, &context).await?,
            LayerFunctionDef::UseDefault => {
                if let Some(default) = default {
                    default.transform.clone()
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Transform function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => input.clone(),
        };
        let metric = match &self.metric {
            LayerFunctionDef::Some(func) => {
                Some(func.call(&transform, original_input, &context).await?)
            }
            LayerFunctionDef::UseDefault => {
                if let Some(metric) = default.as_ref().and_then(|default| default.metric.as_ref()) {
                    Some(metric.clone())
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Metric function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => None,
        };

        let branch = match &self.branch {
            LayerFunctionDef::Some(func) => {
                Some(func.call(&transform, original_input, &context).await?)
            }
            LayerFunctionDef::UseDefault => {
                if let Some(branch) = default.as_ref().and_then(|default| default.branch.as_ref()) {
                    Some(branch.clone())
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Branch function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => None,
        };

        Ok(LayerEvaluation {
            transform,
            context,
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
    #[error("{0}")]
    Custom(String),
}

impl EvaluationError {
    pub fn custom(err: impl ToString) -> Self {
        Self::Custom(err.to_string())
    }
}

impl Evaluator {
    pub async fn evaluate(
        &self,
        mut input: PyObject,
        defaults: Option<&EvaluationResult>,
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
            let default = defaults
                .and_then(|defaults| defaults.get(&layer.name))
                .and_then(|defaults| defaults.get(out.get(&layer.name).map_or(0, |v| v.len())));
            let result = try_or_bail!(
                layer
                    .evaluate(&input, &original_input, &context, default)
                    .await
            );
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
                    Ok(Layer {
                        name: layer.name.clone(),
                        transform: Self::import_function_def(py, env, layer.transform.as_ref())?,
                        context: Self::import_function_def(py, env, layer.context.as_ref())?,
                        metric: Self::import_function_def(py, env, layer.metric.as_ref())?,
                        branch: Self::import_function_def(py, env, layer.branch.as_ref())?,
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

    fn import_function_def(
        py: Python,
        env: &PyEnv,
        def: Option<&FunctionDef>,
    ) -> PyResult<LayerFunctionDef> {
        if let Some(FunctionDef { path, .. }) = def {
            if path.has_ref() {
                Ok(LayerFunctionDef::UseDefault)
            } else {
                Ok(LayerFunctionDef::Some(LayerFunction::new(
                    py,
                    env.import_path(py, path)?,
                )?))
            }
        } else {
            Ok(LayerFunctionDef::None)
        }
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
