//! Process and IPC management for persistent Python sessions.
//!
//! `kernel/python/worker.py` speaks a deliberately small protocol: each JSON message is
//! preceded by a four byte, big-endian payload length.  This module keeps the
//! process details out of the MM2 source/sink implementations and provides
//! synchronous operations, which is what the current MM2 evaluator requires.

use std::collections::HashMap;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{Value, json};

use crate::python::{PyCommand, PyResult, PyValue, py_command_from_expr, py_result_to_expr_bytes};
use mork_expr::Expr;

const DEFAULT_WORKER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/python/worker.py");
const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum PyIpcError {
    Io(io::Error),
    Json(serde_json::Error),
    Protocol(String),
    MissingResult(String),
    WorkerFailed(String),
}

impl std::fmt::Display for PyIpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "python worker I/O error: {e}"),
            Self::Json(e) => write!(f, "python worker JSON error: {e}"),
            Self::Protocol(e) => write!(f, "invalid python worker response: {e}"),
            Self::MissingResult(id) => write!(f, "no cached Python result for request {id:?}"),
            Self::WorkerFailed(e) => write!(f, "python worker failed: {e}"),
        }
    }
}

impl std::error::Error for PyIpcError {}
impl From<io::Error> for PyIpcError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for PyIpcError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// A single session-scoped worker and its framed standard-I/O channels.
pub struct PyWorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PyWorkerProcess {
    pub fn spawn(session: &str) -> Result<Self, PyIpcError> {
        Self::spawn_with_worker(session, worker_path())
    }

    pub fn spawn_with_worker<P: AsRef<Path>>(session: &str, worker: P) -> Result<Self, PyIpcError> {
        let python = std::env::var_os("MORK_PYTHON").unwrap_or_else(|| "python3".into());
        let mut child = Command::new(python)
            .arg(worker.as_ref())
            .arg(session)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| PyIpcError::Protocol("worker stdin was not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| PyIpcError::Protocol("worker stdout was not piped".into()))?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn transact(&mut self, request: &Value) -> Result<Value, PyIpcError> {
        let payload = serde_json::to_vec(request)?;
        let len = u32::try_from(payload.len())
            .map_err(|_| PyIpcError::Protocol("request frame is too large".into()))?;
        self.stdin.write_all(&len.to_be_bytes())?;
        self.stdin.write_all(&payload)?;
        self.stdin.flush()?;

        let mut header = [0u8; 4];
        self.stdout.read_exact(&mut header)?;
        let size = u32::from_be_bytes(header);
        if size > MAX_FRAME_SIZE {
            return Err(PyIpcError::Protocol(format!(
                "response frame is too large: {size} bytes"
            )));
        }
        let mut body = vec![0u8; size as usize];
        self.stdout.read_exact(&mut body)?;
        Ok(serde_json::from_slice(&body)?)
    }
}

impl Drop for PyWorkerProcess {
    fn drop(&mut self) {
        // Closing stdin lets a well-behaved worker exit naturally.  A worker
        // blocked elsewhere is still reaped/terminated so MORK cannot leak it.
        let _ = self.stdin.flush();
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            _ => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

/// Owns all Python workers and the completed request results visible to PY sources.
pub struct PySessionManager {
    workers: HashMap<String, PyWorkerProcess>,
    results: HashMap<String, PyResult>,
    worker_path: PathBuf,
}

impl Default for PySessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PySessionManager {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
            results: HashMap::new(),
            worker_path: worker_path(),
        }
    }

    pub fn with_worker<P: Into<PathBuf>>(worker: P) -> Self {
        Self {
            workers: HashMap::new(),
            results: HashMap::new(),
            worker_path: worker.into(),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn worker(&mut self, session: &str) -> Result<&mut PyWorkerProcess, PyIpcError> {
        let path = self.worker_path.clone();
        if !self.workers.contains_key(session) {
            let process = PyWorkerProcess::spawn_with_worker(session, path)?;
            self.workers.insert(session.to_owned(), process);
        }
        Ok(self
            .workers
            .get_mut(session)
            .expect("worker inserted above"))
    }

    /// Execute a `Sink::PY` command, waiting until the worker response is cached.
    pub fn execute_sink(&mut self, session: &str, command: PyCommand) -> Result<(), PyIpcError> {
        let request = command_json(&command)?;
        let response = self.worker(session)?.transact(&request)?;
        let result = result_from_json(response)?;
        self.results
            .insert(command_req_id(&command).to_owned(), result);
        Ok(())
    }

    /// Resolve a `Source::PY` query from the completed-result cache.
    pub fn execute_source(
        &self,
        _session: &str,
        query: &PyCommand,
    ) -> Result<PyResult, PyIpcError> {
        let req_id = command_req_id(query);
        self.results
            .get(req_id)
            .cloned()
            .ok_or_else(|| PyIpcError::MissingResult(req_id.to_owned()))
    }

    pub fn execute_sink_expr(&mut self, session: &str, command: Expr) -> Result<(), PyIpcError> {
        let command = py_command_from_expr(command).map_err(PyIpcError::Protocol)?;
        self.execute_sink(session, command)
    }

    pub fn execute_source_expr(&self, session: &str, query: Expr) -> Result<Vec<u8>, PyIpcError> {
        let query = py_command_from_expr(query).map_err(PyIpcError::Protocol)?;
        let result = self.execute_source(session, &query)?;
        Ok(py_result_to_expr_bytes(&result)?)
    }

    pub fn result(&self, req_id: &str) -> Option<&PyResult> {
        self.results.get(req_id)
    }
    pub fn clear_results(&mut self) {
        self.results.clear();
    }
}

fn worker_path() -> PathBuf {
    std::env::var_os("MORK_PY_WORKER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_WORKER))
}

fn command_req_id(command: &PyCommand) -> &str {
    match command {
        PyCommand::Import { req_id, .. }
        | PyCommand::ImportFile { req_id, .. }
        | PyCommand::GetAttr { req_id, .. }
        | PyCommand::GetVariable { req_id, .. }
        | PyCommand::SetVariable { req_id, .. }
        | PyCommand::Call { req_id, .. }
        | PyCommand::CallMethod { req_id, .. }
        | PyCommand::DropObject { req_id, .. } => req_id,
    }
}

fn value_json(value: &PyValue) -> Value {
    match value {
        PyValue::None => json!({"type":"none", "value":null}),
        PyValue::Bool(v) => json!({"type":"bool", "value":v}),
        PyValue::Int(v) => json!({"type":"int", "value":v}),
        PyValue::Float(v) => json!({"type":"float", "value":v}),
        PyValue::Str(v) => json!({"type":"str", "value":v}),
        PyValue::Handle(v) => json!({"type":"handle", "value":v}),
        PyValue::List(v) => {
            json!({"type":"list", "value":v.iter().map(value_json).collect::<Vec<_>>() })
        }
        PyValue::Dict(v) => {
            json!({"type":"dict", "value":v.iter().map(|(k,x)| json!([k,value_json(x)])).collect::<Vec<_>>() })
        }
    }
}

fn command_json(command: &PyCommand) -> Result<Value, PyIpcError> {
    let value = match command {
        PyCommand::Import { req_id, module } => {
            json!({"command":"import","req_id":req_id,"module":module})
        }
        PyCommand::ImportFile {
            req_id,
            module,
            path,
        } => {
            json!({"command":"import-file","req_id":req_id,"module":module,"path":path})
        }
        PyCommand::GetAttr {
            req_id,
            target,
            attr,
        } => json!({"command":"getattr","req_id":req_id,"target":value_json(target),"attr":attr}),
        PyCommand::GetVariable { req_id, name } => {
            json!({"command":"get-variable","req_id":req_id,"name":name})
        }
        PyCommand::SetVariable {
            req_id,
            name,
            value,
        } => {
            json!({"command":"set-variable","req_id":req_id,"name":name,"value":value_json(value)})
        }
        PyCommand::Call {
            req_id,
            module,
            function,
            args,
            kwargs,
        } => {
            json!({"command":"call","req_id":req_id,"module":module,"function":function,"args":args.iter().map(value_json).collect::<Vec<_>>(),"kwargs":kwargs.iter().map(|(k,v)| json!([k,value_json(v)])).collect::<Vec<_>>() })
        }
        PyCommand::CallMethod {
            req_id,
            target,
            method,
            args,
            kwargs,
        } => {
            json!({"command":"call-method","req_id":req_id,"target":value_json(target),"method":method,"args":args.iter().map(value_json).collect::<Vec<_>>(),"kwargs":kwargs.iter().map(|(k,v)| json!([k,value_json(v)])).collect::<Vec<_>>() })
        }
        PyCommand::DropObject { req_id, handle } => {
            json!({"command":"drop-object","req_id":req_id,"handle":handle})
        }
    };
    Ok(value)
}

fn value_from_json(value: &Value) -> Result<PyValue, PyIpcError> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| PyIpcError::Protocol("result is missing type".into()))?;
    match kind {
        "none" => Ok(PyValue::None),
        "bool" => {
            Ok(PyValue::Bool(value["value"].as_bool().ok_or_else(
                || PyIpcError::Protocol("invalid bool".into()),
            )?))
        }
        "int" => {
            Ok(PyValue::Int(value["value"].as_i64().ok_or_else(|| {
                PyIpcError::Protocol("invalid int".into())
            })?))
        }
        "float" => {
            Ok(PyValue::Float(value["value"].as_f64().ok_or_else(
                || PyIpcError::Protocol("invalid float".into()),
            )?))
        }
        "str" => Ok(PyValue::Str(
            value["value"]
                .as_str()
                .ok_or_else(|| PyIpcError::Protocol("invalid string".into()))?
                .into(),
        )),
        "handle" => Ok(PyValue::Handle(
            value["value"]
                .as_str()
                .ok_or_else(|| PyIpcError::Protocol("invalid handle".into()))?
                .into(),
        )),
        "list" => Ok(PyValue::List(
            value["value"]
                .as_array()
                .ok_or_else(|| PyIpcError::Protocol("invalid list".into()))?
                .iter()
                .map(value_from_json)
                .collect::<Result<_, _>>()?,
        )),
        "dict" => Ok(PyValue::Dict(
            value["value"]
                .as_array()
                .ok_or_else(|| PyIpcError::Protocol("invalid dict".into()))?
                .iter()
                .map(|x| {
                    let a = x
                        .as_array()
                        .ok_or_else(|| PyIpcError::Protocol("invalid dict entry".into()))?;
                    Ok((
                        a[0].as_str()
                            .ok_or_else(|| PyIpcError::Protocol("invalid dict key".into()))?
                            .into(),
                        value_from_json(&a[1])?,
                    ))
                })
                .collect::<Result<_, PyIpcError>>()?,
        )),
        _ => Err(PyIpcError::Protocol(format!(
            "unknown result type {kind:?}"
        ))),
    }
}

fn result_from_json(response: Value) -> Result<PyResult, PyIpcError> {
    match response.get("status").and_then(Value::as_str) {
        Some("ok") => Ok(PyResult::Ok(value_from_json(
            response
                .get("result")
                .ok_or_else(|| PyIpcError::Protocol("ok response has no result".into()))?,
        )?)),
        Some("error") => Ok(PyResult::Error {
            error_type: response["type"].as_str().unwrap_or("PythonError").into(),
            message: response["message"].as_str().unwrap_or("").into(),
            traceback: response["traceback"].as_str().unwrap_or("").into(),
        }),
        _ => Err(PyIpcError::Protocol(
            "response status must be ok or error".into(),
        )),
    }
}
