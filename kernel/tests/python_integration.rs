//! Integration coverage for the Python worker protocol and result cache.
//!
//! The MM2 spellings exercised here are kept in
//! `kernel/resources/python/integration.mm2`. The end-to-end tests below
//! drive the source/sink zipper path through `Space`.

use std::path::PathBuf;

use mork::space::Space;
use mork::{PyCommand, PyResult, PySessionManager, PyValue};

fn manager() -> PySessionManager {
    let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/worker.py");
    PySessionManager::with_worker(worker)
}

fn space() -> Space {
    let mut space = Space::new();
    let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/worker.py");
    space.py = PySessionManager::with_worker(worker);
    space
}

fn ok_value(result: PyResult) -> PyValue {
    match result {
        PyResult::Ok(value) => value,
        PyResult::Error {
            error_type,
            message,
            traceback,
        } => panic!("unexpected Python error {error_type}: {message}\n{traceback}"),
    }
}

fn run(manager: &mut PySessionManager, session: &str, command: PyCommand) -> PyResult {
    let req_id = match &command {
        PyCommand::Import { req_id, .. }
        | PyCommand::ImportFile { req_id, .. }
        | PyCommand::GetAttr { req_id, .. }
        | PyCommand::GetVariable { req_id, .. }
        | PyCommand::SetVariable { req_id, .. }
        | PyCommand::Call { req_id, .. }
        | PyCommand::CallMethod { req_id, .. }
        | PyCommand::DropObject { req_id, .. } => req_id.clone(),
    };
    manager.execute_sink(session, command).unwrap();
    manager
        .execute_source(
            session,
            &PyCommand::GetVariable {
                req_id,
                name: "unused".into(),
            },
        )
        .unwrap()
}

#[test]
fn imports_python_module() {
    let mut manager = manager();
    let result = run(
        &mut manager,
        "py1",
        PyCommand::Import {
            req_id: "import-math".into(),
            module: "math".into(),
        },
    );
    assert!(matches!(ok_value(result), PyValue::Handle(handle) if handle == "@py:py1:1"));
    assert_eq!(manager.worker_count(), 1);
}

#[test]
fn calls_builtin_function_and_reads_float_result() {
    let mut manager = manager();
    let result = run(
        &mut manager,
        "py1",
        PyCommand::Call {
            req_id: "sqrt-16".into(),
            module: "math".into(),
            function: "sqrt".into(),
            args: vec![PyValue::Int(16)],
            kwargs: vec![],
        },
    );
    assert_eq!(ok_value(result), PyValue::Float(4.0));
}

#[test]
fn imports_arbitrary_file_into_persistent_session() {
    let mut manager = manager();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/python_fixture.py");
    let imported = run(
        &mut manager,
        "py1",
        PyCommand::ImportFile {
            req_id: "import-file".into(),
            module: "file_functions".into(),
            path: path.to_string_lossy().into_owned(),
        },
    );
    assert_eq!(ok_value(imported), PyValue::None);

    let result = run(
        &mut manager,
        "py1",
        PyCommand::Call {
            req_id: "file-call".into(),
            module: "file_functions".into(),
            function: "triple".into(),
            args: vec![PyValue::Int(7)],
            kwargs: vec![],
        },
    );
    assert_eq!(ok_value(result), PyValue::Int(21));
}

#[test]
fn sets_and_gets_python_variable() {
    let mut manager = manager();
    let set = PyCommand::SetVariable {
        req_id: "set-answer".into(),
        name: "answer".into(),
        value: PyValue::Int(41),
    };
    assert_eq!(ok_value(run(&mut manager, "py1", set)), PyValue::None);

    let get = PyCommand::GetVariable {
        req_id: "get-answer".into(),
        name: "answer".into(),
    };
    assert_eq!(ok_value(run(&mut manager, "py1", get)), PyValue::Int(41));
}

#[test]
fn preserves_object_across_exec_steps() {
    let mut manager = manager();
    let module = run(
        &mut manager,
        "py1",
        PyCommand::Import {
            req_id: "module".into(),
            module: "math".into(),
        },
    );
    let handle = ok_value(module);
    let result = run(
        &mut manager,
        "py1",
        PyCommand::GetAttr {
            req_id: "pi".into(),
            target: handle,
            attr: "pi".into(),
        },
    );
    assert_eq!(ok_value(result), PyValue::Float(std::f64::consts::PI));
}

#[test]
#[ignore = "requires NumPy to be installed in the Python environment"]
fn creates_and_operates_on_numpy_array_handle() {
    let mut manager = manager();
    let numpy = run(
        &mut manager,
        "py1",
        PyCommand::Import {
            req_id: "numpy".into(),
            module: "numpy".into(),
        },
    );
    let array = run(
        &mut manager,
        "py1",
        PyCommand::Call {
            req_id: "array".into(),
            module: "numpy".into(),
            function: "array".into(),
            args: vec![PyValue::List(vec![PyValue::Int(1), PyValue::Int(2)])],
            kwargs: vec![],
        },
    );
    assert!(matches!(ok_value(numpy), PyValue::Handle(h) if h == "@py:py1:1"));
    let array_handle = ok_value(array);
    assert!(matches!(array_handle, PyValue::Handle(ref h) if h == "@py:py1:2"));
    let values = run(
        &mut manager,
        "py1",
        PyCommand::CallMethod {
            req_id: "array-list".into(),
            target: array_handle,
            method: "tolist".into(),
            args: vec![],
            kwargs: vec![],
        },
    );
    assert_eq!(
        ok_value(values),
        PyValue::List(vec![PyValue::Int(1), PyValue::Int(2)])
    );
}

#[test]
#[ignore = "requires NumPy to be installed in the Python environment"]
fn converts_small_numpy_result_to_mm2_list() {
    let mut manager = manager();
    let result = run(
        &mut manager,
        "py1",
        PyCommand::Call {
            req_id: "tolist".into(),
            module: "numpy".into(),
            function: "array".into(),
            args: vec![PyValue::List(vec![PyValue::Int(3), PyValue::Int(5)])],
            kwargs: vec![],
        },
    );
    let handle = ok_value(result);
    let result = run(
        &mut manager,
        "py1",
        PyCommand::CallMethod {
            req_id: "tolist-result".into(),
            target: handle,
            method: "tolist".into(),
            args: vec![],
            kwargs: vec![],
        },
    );
    assert_eq!(
        ok_value(result),
        PyValue::List(vec![PyValue::Int(3), PyValue::Int(5)])
    );
}

#[test]
fn returns_python_exceptions_as_py_error() {
    let mut manager = manager();
    let result = run(
        &mut manager,
        "py1",
        PyCommand::Call {
            req_id: "missing-module".into(),
            module: "module_that_does_not_exist_for_mork_tests".into(),
            function: "anything".into(),
            args: vec![],
            kwargs: vec![],
        },
    );
    assert!(
        matches!(result, PyResult::Error { error_type, .. } if error_type == "ModuleNotFoundError")
    );
}

#[test]
fn named_sessions_have_isolated_state_and_handles() {
    let mut manager = manager();
    let first = run(
        &mut manager,
        "py1",
        PyCommand::SetVariable {
            req_id: "py1-set".into(),
            name: "shared_name".into(),
            value: PyValue::Int(1),
        },
    );
    assert_eq!(ok_value(first), PyValue::None);
    let second = run(
        &mut manager,
        "py2",
        PyCommand::GetVariable {
            req_id: "py2-get".into(),
            name: "shared_name".into(),
        },
    );
    assert!(matches!(second, PyResult::Error { error_type, .. } if error_type == "KeyError"));

    let one = run(
        &mut manager,
        "py1",
        PyCommand::Import {
            req_id: "py1-import".into(),
            module: "math".into(),
        },
    );
    let two = run(
        &mut manager,
        "py2",
        PyCommand::Import {
            req_id: "py2-import".into(),
            module: "math".into(),
        },
    );
    assert!(matches!(ok_value(one), PyValue::Handle(h) if h == "@py:py1:1"));
    assert!(matches!(ok_value(two), PyValue::Handle(h) if h == "@py:py2:1"));
    assert_eq!(manager.worker_count(), 2);
}

#[test]
fn mm2_python_sink_then_source_round_trip() {
    let mut space = space();
    space
        .add_all_sexpr(
            br#"
            (seed 0)
            (exec 0 (, (seed 0))
                (O (PY py1 (Call sqrt-16 math sqrt (Args (Int 16)) (Kwargs)))))
            "#,
        )
        .unwrap();

    space.metta_calculus(100);
    assert!(
        space.py.result("sqrt-16").is_some(),
        "Python sink did not execute"
    );

    space
        .add_all_sexpr(
            br#"
            (exec 1 (I (PY py1 (Call sqrt-16 math sqrt (Args (Int 16)) (Kwargs))
                              (Ok (Float 4))))
                (, (sqrt-result 4)))
            "#,
        )
        .unwrap();
    space.metta_calculus(100);

    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    let output = String::from_utf8(output).unwrap();
    println!("MM2 Python output:\n{output}");
    assert!(output.contains("(sqrt-result"));
}

#[test]
fn mm2_python_state_survives_between_sink_and_source() {
    let mut space = space();
    space
        .add_all_sexpr(
            br#"
            (seed 0)
            (exec 0 (, (seed 0))
                (O (PY py1 (SetVariable set-answer answer (Int 41)))))
            "#,
        )
        .unwrap();
    space.metta_calculus(100);

    space
        .add_all_sexpr(
            br#"
            (exec 1 (, (seed 0))
                (O (PY py1 (GetVariable get-answer answer))))
            "#,
        )
        .unwrap();
    space.metta_calculus(100);

    space
        .add_all_sexpr(
            br#"
            (exec 2 (I (PY py1 (GetVariable get-answer answer)
                              (Ok (Int 41))))
                (, (answer 41)))
            "#,
        )
        .unwrap();
    space.metta_calculus(100);

    let mut output = Vec::new();
    space.dump_all_sexpr(&mut output).unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("(answer 41)"));
}
