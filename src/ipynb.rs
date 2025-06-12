use crate::error::{self, Error};
use aqora_config::{AqoraConfig, AqoraSubmissionConfig, AqoraUseCaseConfig, PathStr};
use aqora_runner::python::PyEnv;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use serde::{de, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

const PARAMETERS_TAG: &str = "parameters";

const AQORA_PARAMETERS: &str = r#"input = __aqora__args[0]
context = __aqora__kwargs.get("context")
original_input = __aqora__kwargs.get("original_input")"#;

#[derive(Default, Serialize)]
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

#[derive(Deserialize, Serialize, Default)]
pub struct Metadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(flatten)]
    pub rest: Option<serde_json::Value>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "cell_type", rename_all = "snake_case")]
pub enum Cell {
    Code {
        #[serde(default)]
        execution_count: Option<usize>,
        #[serde(default)]
        metadata: Metadata,
        #[serde(default)]
        source: CellSource,
        #[serde(default)]
        outputs: Vec<serde_json::Value>,
        #[serde(flatten)]
        rest: Option<serde_json::Value>,
    },
    Markdown {
        #[serde(default)]
        metadata: Metadata,
        #[serde(default)]
        source: CellSource,
        #[serde(flatten)]
        rest: Option<serde_json::Value>,
    },
    Raw {
        #[serde(default)]
        metadata: Metadata,
        #[serde(flatten)]
        rest: Option<serde_json::Value>,
    },
}

impl Cell {
    fn metadata(&self) -> &Metadata {
        match self {
            Cell::Code { metadata, .. } => metadata,
            Cell::Markdown { metadata, .. } => metadata,
            Cell::Raw { metadata, .. } => metadata,
        }
    }
}

#[derive(Deserialize, Serialize, Default)]
pub struct Ipynb {
    #[serde(default)]
    pub cells: Vec<Cell>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbformat: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbformat_minor: Option<usize>,
    #[serde(flatten)]
    pub rest: Option<serde_json::Value>,
}

fn inject_parameters(cells: &mut Vec<Cell>) {
    let mut parameter_indices = cells
        .iter()
        .enumerate()
        .filter_map(|(i, cell)| {
            if cell
                .metadata()
                .tags
                .as_ref()
                .map(|tags| tags.contains(&PARAMETERS_TAG.to_string()))
                .unwrap_or(false)
            {
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
            Cell::Code {
                source: CellSource::from(AQORA_PARAMETERS),
                execution_count: Default::default(),
                metadata: Default::default(),
                outputs: Default::default(),
                rest: Default::default(),
            },
        );
        offset += 1;
    }
}

#[derive(Error, Debug)]
pub enum NotebookToPythonFunctionError {
    #[error("Invalid notebook {0}: {1}")]
    Json(PathBuf, #[source] serde_json::Error),
    #[error("Could not read python notebook {0}: {1}")]
    Read(PathBuf, #[source] std::io::Error),
    #[error("Could not write python script {0}: {1}")]
    Write(PathBuf, #[source] std::io::Error),
    #[error("Could not find notebook {0}")]
    CouldNotFindNotebook(PathStr<'static>),
    #[error("nbconvert failed for {0}: {1}")]
    NbconvertFailed(PathBuf, #[source] PyErr),
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

fn python_exporter(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let templates = PyDict::new(py);
    let template_name = pyo3::intern!(py, "aqora");
    templates.set_item(
        template_name,
        pyo3::intern!(
            py,
            r#"
{%- extends 'null.j2' -%}

{%- block header -%}
#!/usr/bin/env python
# coding: utf-8
{% endblock header %}

{% block in_prompt %}
{% if resources.global_content_filter.include_input_prompt -%}
    # In[{{ cell.execution_count if cell.execution_count else ' ' }}]:
{% endif %}
{% endblock in_prompt %}

{% block input %}
{{ cell.source | ipython2python }}
{% endblock input %}

{% block markdowncell scoped %}
{{ cell.source | comment_lines }}
{% endblock markdowncell %}
"#
        ),
    )?;
    let loader = py
        .import(pyo3::intern!(py, "jinja2"))?
        .call_method1(pyo3::intern!(py, "DictLoader"), (templates,))?;
    let kwargs = PyDict::new(py);
    kwargs.set_item(
        pyo3::intern!(py, "extra_loaders"),
        PyList::new(py, [loader])?,
    )?;
    kwargs.set_item(pyo3::intern!(py, "template_file"), template_name)?;
    py.import(pyo3::intern!(py, "nbconvert"))?.call_method(
        pyo3::intern!(py, "PythonExporter"),
        (),
        Some(&kwargs),
    )
}

async fn notebook_to_script(
    _env: &PyEnv,
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<(), NotebookToPythonFunctionError> {
    let input_path = input_path.as_ref().to_path_buf();
    let output_path = output_path.as_ref().to_path_buf();

    if tokio::fs::try_exists(&output_path).await.is_ok() {
        if let Ok((input_meta, output_meta)) = futures::future::try_join(
            tokio::fs::metadata(&input_path),
            tokio::fs::metadata(&output_path),
        )
        .await
        {
            if let (Ok(input_modified), Ok(output_modified)) =
                (input_meta.modified(), output_meta.modified())
            {
                if input_modified <= output_modified {
                    return Ok(());
                }
            }
        }
    }

    let mut ipynb: Ipynb = serde_json::from_reader(
        std::fs::File::open(&input_path)
            .map_err(|e| NotebookToPythonFunctionError::Read(input_path.clone(), e))?,
    )
    .map_err(|e| NotebookToPythonFunctionError::Json(input_path.clone(), e))?;

    inject_parameters(&mut ipynb.cells);

    let ipynb_json = serde_json::to_string(&ipynb)
        .map_err(|e| NotebookToPythonFunctionError::Json(input_path.clone(), e))?;

    let output_dir = output_path.parent().ok_or_else(|| {
        NotebookToPythonFunctionError::Read(
            output_path.clone(),
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Could not find parent directory",
            ),
        )
    })?;
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| NotebookToPythonFunctionError::Write(output_dir.to_path_buf(), e))?;

    let script = Python::with_gil(|py| {
        let reads_kwargs = PyDict::new(py);
        reads_kwargs.set_item(pyo3::intern!(py, "as_version"), ipynb.nbformat.unwrap_or(4))?;
        let notebook = py.import(pyo3::intern!(py, "nbformat"))?.call_method(
            pyo3::intern!(py, "reads"),
            (ipynb_json,),
            Some(&reads_kwargs),
        )?;

        Ok(python_exporter(py)?
            .call_method1(pyo3::intern!(py, "from_notebook_node"), (notebook,))?
            .get_item(0)?
            .extract::<String>())
    })
    .map_err(|err| NotebookToPythonFunctionError::NbconvertFailed(input_path.clone(), err))?
    .map_err(|err| NotebookToPythonFunctionError::NbconvertFailed(input_path.clone(), err))?;

    tokio::fs::write(&output_path, script)
        .await
        .map_err(|e| NotebookToPythonFunctionError::Write(output_path.clone(), e))?;

    Ok(())
}

struct NotebookMeta {
    path: PathStr<'static>,
    notebook_path: PathBuf,
    generated_name: String,
}

impl NotebookMeta {
    fn notebook_dir(&self) -> Result<&Path, NotebookToPythonFunctionError> {
        self.notebook_path.parent().ok_or_else(|| {
            NotebookToPythonFunctionError::Read(
                self.notebook_path.clone(),
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Could not find parent directory",
                ),
            )
        })
    }

    fn converted_path(&self) -> Result<PathBuf, NotebookToPythonFunctionError> {
        Ok(self
            .notebook_dir()?
            .join("__aqora__")
            .join("generated")
            .join(format!("{}.converted.py", self.generated_name)))
    }

    fn script_path(&self) -> Result<PathBuf, NotebookToPythonFunctionError> {
        Ok(self
            .notebook_dir()?
            .join("__aqora__")
            .join("generated")
            .join(format!("{}.py", self.generated_name)))
    }

    fn aqora_module_path(&self) -> Result<PathBuf, NotebookToPythonFunctionError> {
        Ok(self.notebook_dir()?.join("__aqora__").join("__init__.py"))
    }

    fn function_name(&self) -> String {
        format!("generated_{}", self.generated_name)
    }

    fn new_path(&self) -> PathStr<'static> {
        let mut generated_path = self.path.module().into_owned();
        generated_path.push("__aqora__");
        generated_path.push(self.function_name());
        generated_path
    }

    fn script_function_def(&self) -> String {
        format!(
            r#"import ast
from pathlib import Path

__aqora__path = Path(__file__).parent / '{name}.converted.py'
with open(__aqora__path) as f:
    __aqora__script = compile(f.read(), __aqora__path, 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)

async def __aqora__(*__aqora__args, **__aqora__kwargs):
    globals().update(locals())
    coroutine = eval(__aqora__script, globals())
    if coroutine is not None:
        await coroutine
    if 'output' in globals():
        return globals()['output']
    else:
        raise NameError("No 'output' variable found in the notebook")
"#,
            name = self.generated_name
        )
    }

    fn module_function_def(&self) -> String {
        format!(
            r#"
spec_{name} = importlib.util.spec_from_file_location(
    '{module_name}',
    dir_path / 'generated' / '{name}.py')
module_{name} = importlib.util.module_from_spec(spec_{name})
sys.modules['{module_name}'] = module_{name}
spec_{name}.loader.exec_module(module_{name})

async def {func}(*__aqora__args, **__aqora__kwargs):
    return await module_{name}.__aqora__(*__aqora__args, **__aqora__kwargs)
"#,
            func = self.function_name(),
            module_name = self.path,
            name = self.generated_name
        )
    }
}

fn get_meta(env: &PyEnv, path: &PathStr) -> Result<NotebookMeta, NotebookToPythonFunctionError> {
    let notebook_path = notebook_path(env, path)?;
    let notebook_path = dunce::canonicalize(&notebook_path).map_err(|e| {
        NotebookToPythonFunctionError::Read(
            notebook_path.clone(),
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Could not find notebook: {}", e),
            ),
        )
    })?;
    let generated_name = base32::encode(
        base32::Alphabet::Rfc4648 { padding: false },
        path.name().as_bytes(),
    )
    .to_lowercase();
    Ok(NotebookMeta {
        path: path.clone().into_owned(),
        notebook_path,
        generated_name,
    })
}

async fn convert_notebooks<'a, 'b: 'a>(
    env: &PyEnv,
    paths: impl IntoIterator<Item = &'a mut PathStr<'b>>,
) -> Result<(), NotebookToPythonFunctionError> {
    let paths = paths
        .into_iter()
        .map(|path| get_meta(env, path).map(|meta| (path, meta)))
        .collect::<Result<Vec<_>, _>>()?;

    let mut to_convert = paths.iter().collect::<Vec<_>>();
    to_convert.dedup_by_key(|(path, _)| path);

    let converted =
        futures::future::try_join_all(to_convert.into_iter().map(|(_, meta)| async move {
            notebook_to_script(env, &meta.notebook_path, meta.converted_path()?).await?;
            let script_path = meta.script_path()?;
            tokio::fs::write(&script_path, meta.script_function_def())
                .await
                .map_err(|e| NotebookToPythonFunctionError::Read(script_path.clone(), e))?;
            Result::<_, NotebookToPythonFunctionError>::Ok((
                meta.aqora_module_path()?,
                meta.module_function_def(),
            ))
        }))
        .await?;

    for (path, meta) in paths {
        *path = meta.new_path();
    }

    let mut generated = HashMap::new();
    for (module_path, function_def) in converted {
        generated
            .entry(module_path)
            .or_insert_with(Vec::new)
            .push(function_def);
    }

    futures::future::try_join_all(generated.into_iter().map(
        |(module_path, functions)| async move {
            let mut module = tokio::fs::File::create(&module_path)
                .await
                .map_err(|e| NotebookToPythonFunctionError::Write(module_path.clone(), e))?;
            module
                .write_all(
                    [
                        "import importlib.util",
                        "import sys",
                        "from pathlib import Path",
                        "",
                        "dir_path = Path(__file__).resolve().parent",
                        "",
                        "",
                    ]
                    .join("\n")
                    .as_bytes(),
                )
                .await
                .map_err(|e| NotebookToPythonFunctionError::Write(module_path.clone(), e))?;
            module
                .write_all(functions.join("\n\n").as_bytes())
                .await
                .map_err(|e| NotebookToPythonFunctionError::Write(module_path.clone(), e))?;
            module
                .flush()
                .await
                .map_err(|e| NotebookToPythonFunctionError::Write(module_path.clone(), e))?;
            Result::<_, NotebookToPythonFunctionError>::Ok(())
        },
    ))
    .await?;

    Ok(())
}

pub async fn convert_submission_notebooks(
    env: &PyEnv,
    submission: &mut AqoraSubmissionConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    convert_notebooks(
        env,
        submission
            .refs
            .values_mut()
            .filter(|f| f.notebook)
            .map(|f| &mut f.path),
    )
    .await
}

pub async fn convert_use_case_notebooks(
    env: &PyEnv,
    use_case: &mut AqoraUseCaseConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    let mut paths = Vec::new();
    for layer in &mut use_case.layers {
        paths.extend(
            [
                &mut layer.transform,
                &mut layer.context,
                &mut layer.metric,
                &mut layer.branch,
            ]
            .into_iter()
            .flatten()
            .filter(|f| f.notebook)
            .map(|f| &mut f.path),
        )
    }
    for test in use_case.tests.values_mut() {
        for layer_override in test.overrides.values_mut() {
            paths.extend(
                [
                    &mut layer_override.transform,
                    &mut layer_override.context,
                    &mut layer_override.metric,
                    &mut layer_override.branch,
                ]
                .into_iter()
                .flatten()
                .filter(|f| f.notebook)
                .map(|f| &mut f.path),
            )
        }
        paths.extend(
            test.refs
                .values_mut()
                .filter(|f| f.notebook)
                .map(|f| &mut f.path),
        );
    }
    convert_notebooks(env, paths).await
}

pub async fn convert_project_notebooks(
    env: &PyEnv,
    config: &mut AqoraConfig,
) -> Result<(), NotebookToPythonFunctionError> {
    match config {
        AqoraConfig::UseCase(use_case) => convert_use_case_notebooks(env, use_case).await,
        AqoraConfig::Submission(submission) => convert_submission_notebooks(env, submission).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqora_runner::python::PyEnvOptions;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    const EXAMPLE_IPYNB: &str = r###"{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Hello world!\n"
     ]
    }
   ],
   "source": [
    "# This is some example code\n",
    "print(\"Hello world!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {
    "tags": [
     "parameters"
    ]
   },
   "outputs": [],
   "source": [
    "# This block has parameters tag"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "This is a markdown block"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "/home/julian\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/home/julian/Development/aqora/use-cases/h2-groundstate-energy/template/.venv/lib/python3.10/site-packages/IPython/core/magics/osm.py:393: UserWarning: This is now an optional IPython functionality, using bookmarks requires you to install the `pickleshare` library.\n",
      "  bkms = self.shell.db.get('bookmarks', {})\n",
      "/home/julian/Development/aqora/use-cases/h2-groundstate-energy/template/.venv/lib/python3.10/site-packages/IPython/core/magics/osm.py:428: UserWarning: This is now an optional IPython functionality, setting dhist requires you to install the `pickleshare` library.\n",
      "  self.shell.db['dhist'] = compress_dhist(dhist)[-100:]\n"
     ]
    }
   ],
   "source": [
    "# This one has a magic func\n",
    "%cd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "hello\n"
     ]
    }
   ],
   "source": [
    "# And this one has a shell command\n",
    "!echo \"hello\""
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": ".venv",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}"###;

    const CONVERTED_SCRIPT: &str = r#"#!/usr/bin/env python
# coding: utf-8

# In[1]:


# This is some example code
print("Hello world!")


# In[2]:


# This block has parameters tag


# In[ ]:


input = __aqora__args[0]
context = __aqora__kwargs.get("context")
original_input = __aqora__kwargs.get("original_input")


# This is a markdown block

# In[3]:


# This one has a magic func
get_ipython().run_line_magic('cd', '')


# In[4]:


# And this one has a shell command
get_ipython().system('echo "hello"')

"#;

    #[test]
    fn test_ipynb_deserialization() {
        let ipynb: Ipynb = serde_json::from_str(EXAMPLE_IPYNB).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(EXAMPLE_IPYNB).unwrap(),
            serde_json::to_value(&ipynb).unwrap()
        );
    }

    #[tokio::test]
    async fn test_notebook_to_script() {
        pyo3::prepare_freethreaded_python();
        let temp_dir = TempDir::new().unwrap();
        let env = PyEnv::init(
            which::which("uv").unwrap(),
            &temp_dir.path().join(".venv"),
            PyEnvOptions::default(),
        )
        .await
        .unwrap();
        let input = temp_dir.path().join("input.ipynb");
        let output = temp_dir.path().join("output.py");
        std::fs::write(&input, EXAMPLE_IPYNB).unwrap();
        notebook_to_script(&env, &input, &output).await.unwrap();
        let script = std::fs::read_to_string(output).unwrap();
        assert_eq!(script, CONVERTED_SCRIPT);
    }
}
