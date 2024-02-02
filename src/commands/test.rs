use super::{install, Install};
use crate::{
    credentials::get_access_token,
    error::{self, Error, Result},
    graphql_client::{custom_scalars::*, GraphQLClient},
    id::Id,
    pipeline::Pipeline,
    pyproject::{project_data_dir, PackageName, PyProject},
    python::{async_generator, pypi_url, AsyncIterator, PipOptions, PyEnv},
};
use clap::Args;
use futures::prelude::*;
use graphql_client::GraphQLQuery;
use indicatif::{MultiProgress, ProgressBar};
use pyo3::{prelude::*, types::PyModule, Python};
use std::path::PathBuf;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "src/graphql/get_competition_use_case.graphql",
    schema_path = "src/graphql/schema.graphql",
    response_derives = "Debug"
)]
pub struct GetCompetitionUseCase;

async fn get_use_case(
    client: &GraphQLClient,
    competition_id: Id,
) -> Result<get_competition_use_case::GetCompetitionUseCaseNodeOnCompetitionUseCase> {
    let use_case = match client
        .send::<GetCompetitionUseCase>(get_competition_use_case::Variables {
            id: competition_id.to_node_id(),
        })
        .await?
        .node
    {
        get_competition_use_case::GetCompetitionUseCaseNode::Competition(c) => {
            if let Some(use_case) = c.use_case {
                use_case
            } else {
                return Err(error::user(
                    "No use case found",
                    "Please contact the competition organizer",
                ));
            }
        }
        _ => {
            return Err(error::user(
                "No use case found",
                "Please contact the competition organizer",
            ));
        }
    };
    Ok(use_case)
}

#[derive(Args, Debug, Clone)]
#[command(author, version, about)]
pub struct Test {
    #[arg(short, long, default_value = "https://app.aqora.io")]
    pub url: String,
    #[arg(short, long, default_value = ".")]
    pub project_dir: PathBuf,
}

impl From<Test> for Install {
    fn from(args: Test) -> Self {
        Install {
            url: args.url,
            project_dir: args.project_dir,
            upgrade: false,
        }
    }
}

// pub async fn run_pipeline(args: Test, project: PyProject) -> Result<f32> {
//     // TODO fix me
//     let use_case_project = PyProject::for_project("examples/use_case")?;
//     let out = pyo3::Python::with_gil(|py| {
//         let pipeline = PyModule::from_code(
//             py,
//             include_str!("../py/pipeline.py"),
//             "pipeline.py",
//             "pipeline",
//         )?;
//         let layer_cls = pipeline.getattr("Layer")?;
//         let pipeline_cls = pipeline.getattr("Pipeline")?;

//         let config = use_case_project.aqora()?.as_use_case()?;
//         let generator = config.generator.path.import(py)?.call0()?;
//         let aggregator = config.aggregator.path.import(py)?;

//         let layers = config
//             .layers
//             .iter()
//             .map(|layer| {
//                 let evaluator = layer.evaluate.path.import(py)?;
//                 let metric = if let Some(metric) = layer.metric.as_ref() {
//                     Some(metric.path.import(py)?)
//                 } else {
//                     None
//                 };
//                 layer_cls.call1((evaluator, metric))
//             })
//             .collect::<pyo3::PyResult<Vec<_>>>()?;

//         let pipeline = pipeline_cls.call1((generator, layers, aggregator))?;

//         Ok::<_, Error>(pyo3_asyncio::into_future_with_locals(
//             &pyo3_asyncio::tokio::get_current_locals(py)?,
//             pipeline.call_method0("run")?,
//         )?)
//     })?
//     .await?;
//     Ok(pyo3::Python::with_gil(|py| out.extract(py))?)
// }

pub async fn test_submission(args: Test, project: PyProject) -> Result<()> {
    let m = MultiProgress::new();

    let submission = project.aqora()?.as_submission()?;

    let mut venv_pb = ProgressBar::new_spinner().with_message("Applying changes");
    venv_pb.enable_steady_tick(std::time::Duration::from_millis(100));
    venv_pb = m.add(venv_pb);

    let env = PyEnv::init(&args.project_dir).await?;

    env.pip_install(
        [args.project_dir.to_string_lossy().to_string()],
        &PipOptions {
            no_deps: true,
            ..Default::default()
        },
        Some(&venv_pb),
    )
    .await?;

    venv_pb.finish_with_message("Changes applied");

    let client = GraphQLClient::new(args.url.parse()?).await?;
    let use_case_res = get_use_case(&client, submission.competition).await?;
    let use_case_pyproject = PyProject::from_toml(use_case_res.pyproject_toml)?;
    let use_case = use_case_pyproject.aqora()?.as_use_case()?;

    let pipeline = Pipeline::import(use_case, submission)?;
    let score = pipeline
        .aggregate(pipeline.evaluate(pipeline.generator()?, pipeline.evaluator()))
        .await?;

    println!("Score: {}", score);

    Ok(())
}

pub async fn test(args: Test) -> Result<()> {
    if !project_data_dir(&args.project_dir).exists() {
        install(args.clone().into()).await?;
    }
    let project = PyProject::for_project(&args.project_dir)?;
    let aqora = project.aqora()?;
    if aqora.is_submission() {
        test_submission(args, project).await
    } else {
        Err(error::user(
            "Use cases not supported",
            "Run test on a submission instead",
        ))
    }
}
