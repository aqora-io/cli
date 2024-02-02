use crate::{
    error::{self, Result},
    pyproject::{AqoraSubmissionConfig, AqoraUseCaseConfig, PathStr},
    python::{async_generator, async_python_run, deepcopy, AsyncIterator},
};
use futures::prelude::*;
use pyo3::{prelude::*, types::PyString};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Layer {
    name: String,
    transform: Option<Py<PyAny>>,
    metric: Option<Py<PyAny>>,
    branch: Option<Py<PyAny>>,
}

#[derive(Debug)]
pub struct LayerEvaluation {
    output: Py<PyAny>,
    metric: Option<Py<PyAny>>,
    branch: Option<String>,
}

impl Layer {
    pub async fn evaluate(&self, input: &Py<PyAny>) -> PyResult<LayerEvaluation> {
        let output = if let Some(transform) = self.transform.as_ref() {
            let transform_input = deepcopy(input)?;
            async_python_run!(|py| transform.as_ref(py).call1((transform_input,)))
        } else {
            input.clone()
        };

        let metric = if let Some(metric) = self.metric.as_ref() {
            let metric_input = deepcopy(input)?;
            let metric_output = deepcopy(&output)?;
            Some(async_python_run!(|py| metric
                .as_ref(py)
                .call1((metric_input, metric_output,))))
        } else {
            None
        };

        let branch = if let Some(branch) = self.branch.as_ref() {
            let transform_output = deepcopy(&output)?;
            let branch = async_python_run!(|py| branch.as_ref(py).call1((transform_output,)));
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

pub struct Pipeline {
    generator: Py<PyAny>,
    aggregator: Py<PyAny>,
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
            let layers = use_case
                .layers
                .iter()
                .map(|layer| {
                    let transform = layer
                        .transform
                        .as_ref()
                        .map(|path| Self::import_path(py, path, &submission.refs))
                        .transpose()?
                        .map(|imported| imported.into_py(py));
                    let metric = layer
                        .metric
                        .as_ref()
                        .map(|path| Self::import_path(py, path, &submission.refs))
                        .transpose()?
                        .map(|imported| imported.into_py(py));
                    let branch = layer
                        .branch
                        .as_ref()
                        .map(|path| Self::import_path(py, path, &submission.refs))
                        .transpose()?
                        .map(|imported| imported.into_py(py));
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
                layers,
            })
        })
    }

    pub fn generator(&self) -> PyResult<impl Stream<Item = PyResult<Py<PyAny>>>> {
        async_generator(self.generator.clone())
    }

    pub async fn evaluate(
        &self,
        input: Py<PyAny>,
    ) -> Result<HashMap<String, Vec<LayerEvaluation>>> {
        let mut input = input;
        let mut out = HashMap::<String, Vec<LayerEvaluation>>::new();
        let mut layer_index = 0;
        while layer_index < self.layers.len() {
            let layer = &self.layers[layer_index];
            let result = layer.evaluate(&input).await?;
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

    pub async fn test_aggregator(&self) -> PyResult<()> {
        let iterator = AsyncIterator::new(futures::stream::iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                .iter()
                .map(|x| Python::with_gil(|py| Ok(x.to_object(py)))),
        ));
        async_python_run!(|py| self.aggregator.as_ref(py).call1((iterator,)));
        Ok(())
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
