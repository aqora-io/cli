use crate::error::{self, Error};
use aqora_config::{AqoraConfig, AqoraSubmissionConfig, AqoraUseCaseConfig, FunctionDef, PathStr};
use aqora_runner::python::PyEnv;
use pyo3::prelude::*;
use serde::{de, Deserialize};
use std::{
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
};
use thiserror::Error;

const PARAMETERS_TAG: &str = "parameters";
const CODE_TYPE: &str = "code";

const AQORA_PARAMETERS: &str = r#"input = __aqora__args[0]
context = __aqora__kwargs.get("context")
original_input = __aqora__kwargs.get("original_input")"#;

#[derive(Default)]
pub struct CellSource(Vec<String>);

impl<'de> de::Deserialize<'de> for CellSource {
    fn deserialize<D>(deserializer: D) -> Result<CellSource, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct CellSourceVisitor;

        impl<'de> de::Visitor<'de> for CellSourceVisitor {
            type Value = CellSource;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or a list of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<CellSource, E>
            where
                E: de::Error,
            {
                Ok(CellSource(vec![value.to_string()]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<CellSource, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut source = Vec::new();
                while let Some(value) = seq.next_element()? {
                    source.push(value);
                }
                Ok(CellSource(source))
            }
        }

        deserializer.deserialize_any(CellSourceVisitor)
    }
}

impl<'a> From<&'a str> for CellSource {
    fn from(s: &'a str) -> Self {
        CellSource(vec![s.to_string()])
    }
}

#[derive(Deserialize, Default)]
pub struct Metadata {
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Deserialize)]
pub struct Cell {
    pub cell_type: String,
    #[serde(default)]
    pub metadata: Metadata,
    #[serde(default)]
    pub source: CellSource,
}

impl Cell {
    pub fn is_code(&self) -> bool {
        self.cell_type == CODE_TYPE
    }
}

#[derive(Deserialize, Default)]
pub struct Ipynb {
    #[serde(default)]
    pub cells: Vec<Cell>,
}

fn inject_parameters(cells: &mut Vec<Cell>) {
    let mut parameter_indices = cells
        .iter()
        .enumerate()
        .filter_map(|(i, cell)| {
            if cell.metadata.tags.contains(&PARAMETERS_TAG.to_string()) {
                Some(i)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let mut offset = 0;
    if parameter_indices.is_empty() {
        parameter_indices.push(0);
    } else {
        offset += 1;
    }
    for i in parameter_indices {
        cells.insert(
            i + offset,
            Cell {
                cell_type: CODE_TYPE.to_string(),
                metadata: Metadata::default(),
                source: CellSource::from(AQORA_PARAMETERS),
            },
        );
        offset += 1;
    }
}

impl Ipynb {
    fn into_python_function(mut self) -> String {
        inject_parameters(&mut self.cells);
        let cells = self
            .cells
            .iter()
            .filter_map(|cell| {
                if cell.is_code() {
                    let code = cell.source.0.join("").replace(r#"'''"#, r#"'\''"#);
                    Some(format!("exec('''\n{code}\n''', globals())"))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>()
            .join("\n\n    ");
        format!(
            r#"async def __aqora__(*__aqora__args, **__aqora__kwargs):
    globals().update(locals())

    {cells}

    if 'output' in globals():
        return globals()['output']
    else:
        raise NameError("No 'output' variable found in the notebook")"#
        )
    }
}

#[derive(Error, Debug)]
pub enum NotebookToPythonFunctionError {
    #[error("Invalid notebook {0}: {0}")]
    Json(PathBuf, #[source] serde_json::Error),
    #[error("Could not read python notebook {0}: {1}")]
    Read(PathBuf, #[source] std::io::Error),
    #[error("Could not write python script {0}: {1}")]
    Write(PathBuf, #[source] std::io::Error),
    #[error("Could not find notebook {0}")]
    CouldNotFindNotebook(PathStr<'static>),
    #[error(transparent)]
    Python(#[from] PyErr),
}

impl From<NotebookToPythonFunctionError> for Error {
    fn from(e: NotebookToPythonFunctionError) -> Self {
        use NotebookToPythonFunctionError::*;
        match e {
            Python(e) => error::system(&format!("{}", e), ""),
            err => error::user(
                &format!("{}", err),
                "Check the files, configuration and permissions and try again",
            ),
        }
    }
}

fn notebook_path(env: &PyEnv, path: &PathStr) -> Result<PathBuf, NotebookToPythonFunctionError> {
    let paths = Python::with_gil(|py| env.find_spec_search_locations(py, path))?;
    let filename = Path::new(path.name()).with_extension("ipynb");
    for path in paths {
        let notebook = path.join(&filename);
        if notebook.exists() {
            return Ok(notebook);
        }
    }
    Err(NotebookToPythonFunctionError::CouldNotFindNotebook(
        path.clone().into_owned(),
    ))
}

fn convert_notebook(
    env: &PyEnv,
    function_def: &mut FunctionDef,
) -> Result<(), NotebookToPythonFunctionError> {
    let path = match function_def {
        FunctionDef {
            path,
            notebook: true,
        } => path,
        _ => return Ok(()),
    };

    let notebook_path = notebook_path(env, path)?;
    let ipynb: Ipynb = serde_json::from_reader(
        std::fs::File::open(&notebook_path)
            .map_err(|e| NotebookToPythonFunctionError::Read(notebook_path.clone(), e))?,
    )
    .map_err(|e| NotebookToPythonFunctionError::Json(notebook_path.clone(), e))?;

    let python_path = {
        let mut file_name = OsString::from("__generated__");
        file_name.push(notebook_path.file_name().unwrap());
        let mut python_path = notebook_path.clone();
        python_path.set_file_name(file_name);
        python_path.set_extension("py");
        python_path
    };

    std::fs::write(&python_path, ipynb.into_python_function())
        .map_err(|e| NotebookToPythonFunctionError::Write(python_path.clone(), e))?;

    let generated_name = format!("__generated__{}", path.name());
    let mut generated_path = path.module().into_owned();
    generated_path.push(&generated_name);
    generated_path.push("__aqora__");

    function_def.path = generated_path;
    function_def.notebook = false;
    Ok(())
}

pub fn convert_submission_notebooks(
    env: &PyEnv,
    submission: &mut AqoraSubmissionConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    for function in submission.refs.values_mut() {
        convert_notebook(env, function)?;
    }
    Ok(())
}

pub fn convert_use_case_notebooks(
    env: &PyEnv,
    use_case: &mut AqoraUseCaseConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    for layer in &mut use_case.layers {
        for function in [
            &mut layer.transform,
            &mut layer.context,
            &mut layer.metric,
            &mut layer.branch,
        ]
        .into_iter()
        .flatten()
        {
            convert_notebook(env, function)?;
        }
    }
    for test in use_case.tests.values_mut() {
        for layer_override in test.overrides.values_mut() {
            for function in [
                &mut layer_override.transform,
                &mut layer_override.context,
                &mut layer_override.metric,
                &mut layer_override.branch,
            ]
            .into_iter()
            .flatten()
            {
                convert_notebook(env, function)?;
            }
        }
        for function in test.refs.values_mut() {
            convert_notebook(env, function)?;
        }
    }
    Ok(())
}

pub fn convert_project_notebooks(
    env: &PyEnv,
    config: &mut AqoraConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    match config {
        AqoraConfig::UseCase(use_case) => convert_use_case_notebooks(env, use_case),
        AqoraConfig::Submission(submission) => convert_submission_notebooks(env, submission),
    }
}
