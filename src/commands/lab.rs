use std::ffi::OsString;

use clap::Args;
use serde::Serialize;

use crate::error::Result;

use super::GlobalArgs;

use crate::commands::python::{python, Python};

#[derive(Args, Debug, Serialize)]
pub struct Lab {
    pub jupyter_args: Vec<OsString>,
}

pub async fn lab(args: Lab, global_args: GlobalArgs) -> Result<()> {
    let args = Python {
        module: Some("jupyterlab".into()),
        python_args: args.jupyter_args,
    };
    python(args, global_args).await
}
