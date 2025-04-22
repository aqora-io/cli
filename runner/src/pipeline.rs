use crate::python::{
    async_generator, async_python_run, deepcopy, format_err, serde_pickle, serde_pickle_opt,
    AsyncIterator, PyBoundObject, PyEnv,
};
use aqora_config::{AqoraUseCaseConfig, FunctionDef};
use futures::prelude::*;
use pyo3::{
    exceptions::PyValueError,
    intern,
    prelude::*,
    pyclass,
    types::{PyDict, PyIterator, PyNone, PyTuple},
    BoundObject,
};
use serde::{Deserialize, Serialize};
use split_stream_by::{Either, SplitStreamByMapExt};
use std::{collections::HashMap, path::PathBuf};
use thiserror::Error;

#[derive(Debug)]
pub struct LayerFunction {
    func: PyObject,
    takes_input_arg: bool,
    takes_original_input_kwarg: bool,
    takes_context_kwarg: bool,
}

impl LayerFunction {
    pub fn new<'py>(py: Python<'py>, func: PyBoundObject<'py>) -> PyResult<Self> {
        let inspect = py.import(intern!(py, "inspect"))?.into_pyobject(py)?;
        let parameter_cls = inspect.getattr(intern!(py, "Parameter"))?;
        let positional_only = parameter_cls.getattr(intern!(py, "POSITIONAL_ONLY"))?;
        let positional_or_keyword = parameter_cls.getattr(intern!(py, "POSITIONAL_OR_KEYWORD"))?;
        let var_positional = parameter_cls.getattr(intern!(py, "VAR_POSITIONAL"))?;
        let var_keyword = parameter_cls.getattr(intern!(py, "VAR_KEYWORD"))?;

        let mut takes_input_arg = false;
        let mut takes_original_input_kwarg = false;
        let mut takes_context_kwarg = false;
        let parameters = PyIterator::from_object(
            &inspect
                .getattr(intern!(py, "signature"))?
                .call1((&func,))?
                .getattr(intern!(py, "parameters"))?
                .call_method0(intern!(py, "values"))?,
        )?;
        for parameter in parameters {
            let parameter = parameter?;
            let kind = parameter.getattr(intern!(py, "kind"))?;
            if kind.eq(&positional_only)? || kind.eq(&var_positional)? {
                takes_input_arg = true;
                continue;
            }
            if kind.eq(&var_keyword)? {
                takes_original_input_kwarg = true;
                takes_context_kwarg = true;
                continue;
            }
            if kind.eq(&positional_or_keyword)? && !takes_input_arg {
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
            func: func.unbind(),
            takes_input_arg,
            takes_original_input_kwarg,
            takes_context_kwarg,
        })
    }

    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            func: self.func.clone_ref(py),
            takes_input_arg: self.takes_input_arg,
            takes_original_input_kwarg: self.takes_original_input_kwarg,
            takes_context_kwarg: self.takes_context_kwarg,
        }
    }

    pub async fn call(
        &self,
        input: &PyObject,
        original_input: &PyObject,
        context: &PyObject,
    ) -> PyResult<PyObject> {
        async_python_run!(|py| {
            let args = if self.takes_input_arg {
                PyTuple::new(py, [deepcopy(input.bind(py))?])?
            } else {
                PyTuple::empty(py)
            };

            let kwargs = PyDict::new(py);
            if self.takes_original_input_kwarg {
                kwargs.set_item(
                    intern!(py, "original_input"),
                    deepcopy(original_input.bind(py))?,
                )?;
            }
            if self.takes_context_kwarg {
                kwargs.set_item(intern!(py, "context"), deepcopy(context.bind(py))?)?;
            }

            Ok(self.func.call(py, args, Some(&kwargs))?.into_bound(py))
        })?
        .await
    }
}

#[derive(Debug)]
pub enum LayerFunctionDef {
    None,
    Some(LayerFunction),
    UseDefault,
}

impl LayerFunctionDef {
    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            LayerFunctionDef::None => LayerFunctionDef::None,
            LayerFunctionDef::Some(func) => LayerFunctionDef::Some(func.clone_ref(py)),
            LayerFunctionDef::UseDefault => LayerFunctionDef::UseDefault,
        }
    }
}

#[derive(Debug)]
pub struct Layer {
    name: String,
    transform: LayerFunctionDef,
    context: LayerFunctionDef,
    metric: LayerFunctionDef,
    branch: LayerFunctionDef,
}

impl Layer {
    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            name: self.name.clone(),
            transform: self.transform.clone_ref(py),
            context: self.context.clone_ref(py),
            metric: self.metric.clone_ref(py),
            branch: self.branch.clone_ref(py),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize, Default, Debug)]
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
                    Python::with_gil(|py| default.context.clone_ref(py))
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Context function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => Python::with_gil(|py| context.clone_ref(py)),
        };
        let transform = match &self.transform {
            LayerFunctionDef::Some(func) => func.call(input, original_input, &context).await?,
            LayerFunctionDef::UseDefault => {
                if let Some(default) = default {
                    Python::with_gil(|py| default.transform.clone_ref(py))
                } else {
                    return Err(PyErr::new::<PyValueError, _>(
                        "Transform function is ignored but no default is provided",
                    ));
                }
            }
            LayerFunctionDef::None => Python::with_gil(|py| input.clone_ref(py)),
        };
        let metric = match &self.metric {
            LayerFunctionDef::Some(func) => {
                Some(func.call(&transform, original_input, &context).await?)
            }
            LayerFunctionDef::UseDefault => {
                if let Some(metric) = default.as_ref().and_then(|default| default.metric.as_ref()) {
                    let cloned = Python::with_gil(|py| metric.clone_ref(py));
                    Some(cloned)
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
                    Python::with_gil(|py| branch.clone_ref(py)).into()
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
    fn py_data<'py>(&self, py: Python<'py>) -> PyResult<PyBoundObject<'py>> {
        py.import("pathlib")?.getattr("Path")?.call1((&self.data,))
    }
}

#[pymethods]
impl PipelineConfig {
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<PyBoundObject<'py>>> {
        match key {
            "data" => self.py_data(py).map(Some),
            _ => Ok(None),
        }
    }
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<PyBoundObject<'py>> {
        self.py_data(py)
    }
}

#[derive(Debug)]
pub struct Evaluator {
    layers: Vec<Layer>,
}

impl Evaluator {
    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            layers: self
                .layers
                .iter()
                .map(|layer| layer.clone_ref(py))
                .collect(),
        }
    }
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

        let (original_input, mut context) = Python::with_gil(|py| {
            (
                input.clone_ref(py),
                PyNone::get(py).unbind().clone_ref(py).into_any(),
            )
        });

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
            Python::with_gil(|py| {
                input = result.transform.clone_ref(py);
                context = result.context.clone_ref(py);
            });
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
            let generator = env.import_path(py, &use_case.generator)?.unbind();
            let aggregator = env.import_path(py, &use_case.aggregator)?.unbind();
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
                    .call1(py, (self.config.clone().into_pyobject(py)?,))?
                    .into_pyobject(py)?
                    .unbind(),
            )
        })?;
        async_generator(generator)
    }

    pub fn evaluator(&self, py: Python<'_>) -> Evaluator {
        Evaluator {
            layers: self
                .layers
                .iter()
                .map(|layer| layer.clone_ref(py))
                .collect(),
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
            Ok(result) => Python::with_gil(|py| match result.into_pyobject(py) {
                Ok(py_object) => Either::Right(py_object.into_any().unbind()),
                Err(err) => Either::Left(Err(EvaluationError::Python(err))),
            }),
            Err((_, err)) => Either::Left(Err(err)),
        });
        let iterator = AsyncIterator::new(results);
        let result = futures::stream::once(
            async_python_run!(|py| Ok(self.aggregator.call1(py, (iterator,))?.into_bound(py)))?
                .map_err(EvaluationError::Python),
        )
        .boxed();
        let mut out_stream = futures::stream::select(result, errs);
        out_stream.next().await.transpose()
    }
}
