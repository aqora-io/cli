#[pyclass]
struct IPython;

#[pymethods]
impl IPython {
    fn system(&self, code: &str) -> PyResult<()> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        py.run(code, None, None)?;
        Ok(())
    }
}
