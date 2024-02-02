use crate::{
    error::{self, Result},
    pyproject::{AqoraSubmissionConfig, AqoraUseCaseConfig, PathStr},
    python::{async_generator, async_python_run, deepcopy, AsyncIterator},
};
use futures::prelude::*;
use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct LayerFunction {
    func: Py<PyAny>,
    context: bool,
}

impl LayerFunction {
    pub async fn call(&self, input: &Py<PyAny>, context: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        let input = deepcopy(input)?;
        if self.context {
            Ok(async_python_run!(|py| {
                let kwargs = PyDict::new(py);
                kwargs.set_item("context", context)?;
                self.func.as_ref(py).call((input,), Some(kwargs))
            }))
        } else {
            Ok(async_python_run!(|py| self.func.as_ref(py).call1((input,))))
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
    output: Py<PyAny>,
    metric: Option<Py<PyAny>>,
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
        input: &Py<PyAny>,
        context: &Py<PyAny>,
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
pub struct Evaluator {
    context: Option<Py<PyAny>>,
    layers: Vec<Layer>,
}

impl Evaluator {
    pub async fn evaluate(&self, input: Py<PyAny>) -> Result<EvaluationResult> {
        let mut input = input;
        let context = if let Some(context) = self.context.as_ref() {
            let context_input = deepcopy(&input)?;
            async_python_run!(|py| context.as_ref(py).call1((context_input,)))
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
                    .ok_or_else(|| {
                        error::user(
                            &format!("Branch layer {} not found", branch),
                            "Check the layer names in the use case configuration",
                        )
                    })?;
            } else {
                layer_index += 1;
            }
            out.entry(layer.name.clone()).or_default().push(result);
        }
        Ok(out)
    }
}

pub struct Pipeline {
    generator: Py<PyAny>,
    aggregator: Py<PyAny>,
    context: Option<Py<PyAny>>,
    layers: Vec<Layer>,
}

impl Pipeline {
    pub fn import(
        use_case: &AqoraUseCaseConfig,
        submission: &AqoraSubmissionConfig,
    ) -> Result<Self> {
        Python::with_gil(|py| {
            let generator = Self::import_path(py, &use_case.generator, &submission.refs)?
                .call0()?
                .into_py(py);
            let aggregator =
                Self::import_path(py, &use_case.aggregator, &submission.refs)?.into_py(py);
            let context = use_case
                .context
                .as_ref()
                .map(|path| {
                    PyResult::Ok(Self::import_path(py, path, &submission.refs)?.into_py(py))
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
                            PyResult::Ok(LayerFunction {
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
                            PyResult::Ok(LayerFunction {
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
                            PyResult::Ok(LayerFunction {
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
                .collect::<Result<Vec<_>>>()?;
            Ok(Self {
                generator,
                aggregator,
                context,
                layers,
            })
        })
    }

    pub fn generator(&self) -> PyResult<impl Stream<Item = PyResult<Py<PyAny>>>> {
        async_generator(self.generator.clone())
    }

    pub fn evaluator(&self) -> Evaluator {
        Evaluator {
            context: self.context.clone(),
            layers: self.layers.clone(),
        }
    }

    pub fn evaluate(
        &self,
        inputs: impl Stream<Item = PyResult<Py<PyAny>>>,
        evaluator: Evaluator,
    ) -> impl Stream<Item = PyResult<Py<PyAny>>> {
        inputs
            .map_err(PyErr::from)
            .map_ok(move |input| (input, evaluator.clone()))
            .and_then(|(input, evaluator)| async move {
                match evaluator.evaluate(input).await {
                    Ok(result) => Ok(Python::with_gil(|py| result.to_object(py))),
                    Err(err) => Err(PyErr::from(err)),
                }
            })
    }

    pub async fn aggregate(
        &self,
        results: impl Stream<Item = PyResult<Py<PyAny>>> + Send + Sync + 'static,
    ) -> PyResult<Py<PyAny>> {
        let iterator = AsyncIterator::new(results);
        Ok(async_python_run!(|py| self
            .aggregator
            .as_ref(py)
            .call1((iterator,))))
    }

    fn import_path<'py>(
        py: Python<'py>,
        path: &PathStr,
        refs: &HashMap<String, PathStr>,
    ) -> Result<&'py PyAny> {
        let path = path.replace_refs(refs)?;
        let module = PyModule::import(py, PyString::new(py, &path.module().to_string()))?;
        Ok(module.getattr(PyString::new(py, path.name()))?)
    }
}
