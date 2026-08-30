use serde::{Deserialize, Serialize};
use std::io::Write;
use std::str::FromStr;

use mork_expr::macros::{DeserializableExpr, SerializableExpr};
use mork_expr::{Expr, Tag, byte_item, item_byte};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PyValue {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<PyValue>),
    Dict(Vec<(String, PyValue)>),
    Handle(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PyCommand {
    Import {
        req_id: String,
        module: String,
    },
    ImportFile {
        req_id: String,
        module: String,
        path: String,
    },
    GetAttr {
        req_id: String,
        target: PyValue,
        attr: String,
    },
    GetVariable {
        req_id: String,
        name: String,
    },
    SetVariable {
        req_id: String,
        name: String,
        value: PyValue,
    },
    Call {
        req_id: String,
        module: String,
        function: String,
        args: Vec<PyValue>,
        kwargs: Vec<(String, PyValue)>,
    },
    CallMethod {
        req_id: String,
        target: PyValue,
        method: String,
        args: Vec<PyValue>,
        kwargs: Vec<(String, PyValue)>,
    },
    DropObject {
        req_id: String,
        handle: String,
    },
}

pub use PyCommand as PyQuery;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PyResult {
    Ok(PyValue),
    Error {
        error_type: String,
        message: String,
        traceback: String,
    },
}

fn write_symbol<W: Write>(out: &mut W, symbol: &str) -> Result<(), std::io::Error> {
    let len = symbol.as_bytes().len();
    debug_assert!(len < 64);
    out.write_all(&[item_byte(Tag::SymbolSize(len as u8))])?;
    out.write_all(symbol.as_bytes())?;
    Ok(())
}

fn write_form_start<W: Write>(
    out: &mut W,
    head: &str,
    fields: usize,
) -> Result<(), std::io::Error> {
    let arity = fields + 1;
    debug_assert!(arity < 64);
    out.write_all(&[item_byte(Tag::Arity(arity as u8))])?;
    write_symbol(out, head)
}

fn expr_len(expr: Expr) -> usize {
    expr.span().len()
}

fn parse_symbol_at(expr: Expr, offset: usize) -> Result<(String, usize), String> {
    let child = Expr {
        ptr: unsafe { expr.ptr.add(offset) },
    };
    match unsafe { byte_item(*child.ptr) } {
        Tag::SymbolSize(len) => {
            let bytes = unsafe { std::slice::from_raw_parts(child.ptr.add(1), len as usize) };
            let symbol = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            Ok((symbol.to_string(), 1 + len as usize))
        }
        other => Err(format!("expected symbol at offset {offset}, got {other:?}")),
    }
}

fn parse_form_header(expr: Expr) -> Result<(String, usize, usize), String> {
    let total_len = expr_len(expr);
    match unsafe { byte_item(*expr.ptr) } {
        Tag::Arity(_) => {
            let (head, head_len) = parse_symbol_at(expr, 1)?;
            Ok((head, 1 + head_len, total_len))
        }
        other => Err(format!(
            "expected list at start of expression, got {other:?}"
        )),
    }
}

fn parse_py_value_expr(expr: Expr) -> Result<(PyValue, usize), String> {
    match unsafe { byte_item(*expr.ptr) } {
        Tag::SymbolSize(_) => {
            let (symbol, len) = parse_symbol_at(expr, 0)?;
            if symbol == "None" {
                Ok((PyValue::None, len))
            } else {
                Err(format!("unexpected atom {symbol:?} for PyValue"))
            }
        }
        Tag::Arity(_) => {
            let (head, mut offset, total_len) = parse_form_header(expr)?;
            match head.as_str() {
                "Bool" => {
                    let (symbol, consumed) = parse_symbol_at(expr, offset)?;
                    offset += consumed;
                    if offset != total_len {
                        return Err("Bool value has trailing data".to_string());
                    }
                    match symbol.as_str() {
                        "true" => Ok((PyValue::Bool(true), total_len)),
                        "false" => Ok((PyValue::Bool(false), total_len)),
                        _ => Err(format!("invalid Bool payload {symbol:?}")),
                    }
                }
                "Int" => {
                    let (symbol, consumed) = parse_symbol_at(expr, offset)?;
                    offset += consumed;
                    if offset != total_len {
                        return Err("Int value has trailing data".to_string());
                    }
                    let value = i64::from_str(&symbol).map_err(|e| e.to_string())?;
                    Ok((PyValue::Int(value), total_len))
                }
                "Float" => {
                    let (symbol, consumed) = parse_symbol_at(expr, offset)?;
                    offset += consumed;
                    if offset != total_len {
                        return Err("Float value has trailing data".to_string());
                    }
                    let value = f64::from_str(&symbol).map_err(|e| e.to_string())?;
                    Ok((PyValue::Float(value), total_len))
                }
                "Str" => {
                    let (symbol, consumed) = parse_symbol_at(expr, offset)?;
                    offset += consumed;
                    if offset != total_len {
                        return Err("Str value has trailing data".to_string());
                    }
                    Ok((PyValue::Str(symbol), total_len))
                }
                "List" => {
                    let mut values = Vec::new();
                    while offset < total_len {
                        let child = Expr {
                            ptr: unsafe { expr.ptr.add(offset) },
                        };
                        let (value, consumed) = parse_py_value_expr(child)?;
                        values.push(value);
                        offset += consumed;
                    }
                    Ok((PyValue::List(values), total_len))
                }
                "Dict" => {
                    let mut entries = Vec::new();
                    while offset < total_len {
                        let entry_expr = Expr {
                            ptr: unsafe { expr.ptr.add(offset) },
                        };
                        let (entry_head, mut entry_offset, entry_total) =
                            parse_form_header(entry_expr)?;
                        if entry_head != "Entry" {
                            return Err(format!("expected Entry inside Dict, got {entry_head:?}"));
                        }
                        let (key, key_consumed) = parse_symbol_at(entry_expr, entry_offset)?;
                        entry_offset += key_consumed;
                        let value_expr = Expr {
                            ptr: unsafe { entry_expr.ptr.add(entry_offset) },
                        };
                        let (value, value_consumed) = parse_py_value_expr(value_expr)?;
                        entry_offset += value_consumed;
                        if entry_offset != entry_total {
                            return Err("Entry value has trailing data".to_string());
                        }
                        entries.push((key, value));
                        offset += entry_total;
                    }
                    Ok((PyValue::Dict(entries), total_len))
                }
                "Handle" => {
                    let (symbol, consumed) = parse_symbol_at(expr, offset)?;
                    offset += consumed;
                    if offset != total_len {
                        return Err("Handle value has trailing data".to_string());
                    }
                    Ok((PyValue::Handle(symbol), total_len))
                }
                _ => Err(format!("unknown PyValue form {head:?}")),
            }
        }
        other => Err(format!("unexpected tag {other:?} for PyValue")),
    }
}

fn parse_py_kwargs_expr(expr: Expr) -> Result<(Vec<(String, PyValue)>, usize), String> {
    let (head, mut offset, total_len) = parse_form_header(expr)?;
    if head != "Kwargs" {
        return Err(format!("expected Kwargs, got {head:?}"));
    }
    let mut kwargs = Vec::new();
    while offset < total_len {
        let entry_expr = Expr {
            ptr: unsafe { expr.ptr.add(offset) },
        };
        let (entry_head, mut entry_offset, entry_total) = parse_form_header(entry_expr)?;
        if entry_head != "Entry" {
            return Err(format!("expected Entry inside Kwargs, got {entry_head:?}"));
        }
        let (key, key_consumed) = parse_symbol_at(entry_expr, entry_offset)?;
        entry_offset += key_consumed;
        let value_expr = Expr {
            ptr: unsafe { entry_expr.ptr.add(entry_offset) },
        };
        let (value, value_consumed) = parse_py_value_expr(value_expr)?;
        entry_offset += value_consumed;
        if entry_offset != entry_total {
            return Err("Kwargs Entry has trailing data".to_string());
        }
        kwargs.push((key, value));
        offset += entry_total;
    }
    Ok((kwargs, total_len))
}

fn parse_py_args_expr(expr: Expr) -> Result<(Vec<PyValue>, usize), String> {
    let (head, mut offset, total_len) = parse_form_header(expr)?;
    if head != "Args" {
        return Err(format!("expected Args, got {head:?}"));
    }
    let mut args = Vec::new();
    while offset < total_len {
        let value_expr = Expr {
            ptr: unsafe { expr.ptr.add(offset) },
        };
        let (value, consumed) = parse_py_value_expr(value_expr)?;
        args.push(value);
        offset += consumed;
    }
    Ok((args, total_len))
}

fn parse_py_command_expr(expr: Expr) -> Result<(PyCommand, usize), String> {
    let (head, mut offset, total_len) = parse_form_header(expr)?;
    match head.as_str() {
        "Import" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (module, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("Import command has trailing data".to_string());
            }
            Ok((PyCommand::Import { req_id, module }, total_len))
        }
        "ImportFile" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (module, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (path, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("ImportFile command has trailing data".to_string());
            }
            Ok((
                PyCommand::ImportFile {
                    req_id,
                    module,
                    path,
                },
                total_len,
            ))
        }
        "GetAttr" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let target_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (target, consumed) = parse_py_value_expr(target_expr)?;
            offset += consumed;
            let (attr, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("GetAttr command has trailing data".to_string());
            }
            Ok((
                PyCommand::GetAttr {
                    req_id,
                    target,
                    attr,
                },
                total_len,
            ))
        }
        "GetVariable" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (name, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("GetVariable command has trailing data".to_string());
            }
            Ok((PyCommand::GetVariable { req_id, name }, total_len))
        }
        "SetVariable" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (name, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let value_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (value, consumed) = parse_py_value_expr(value_expr)?;
            offset += consumed;
            if offset != total_len {
                return Err("SetVariable command has trailing data".to_string());
            }
            Ok((
                PyCommand::SetVariable {
                    req_id,
                    name,
                    value,
                },
                total_len,
            ))
        }
        "Call" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (module, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (function, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let args_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (args, consumed) = parse_py_args_expr(args_expr)?;
            offset += consumed;
            let kwargs_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (kwargs, consumed) = parse_py_kwargs_expr(kwargs_expr)?;
            offset += consumed;
            if offset != total_len {
                return Err("Call command has trailing data".to_string());
            }
            Ok((
                PyCommand::Call {
                    req_id,
                    module,
                    function,
                    args,
                    kwargs,
                },
                total_len,
            ))
        }
        "CallMethod" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let target_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (target, consumed) = parse_py_value_expr(target_expr)?;
            offset += consumed;
            let (method, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let args_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (args, consumed) = parse_py_args_expr(args_expr)?;
            offset += consumed;
            let kwargs_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (kwargs, consumed) = parse_py_kwargs_expr(kwargs_expr)?;
            offset += consumed;
            if offset != total_len {
                return Err("CallMethod command has trailing data".to_string());
            }
            Ok((
                PyCommand::CallMethod {
                    req_id,
                    target,
                    method,
                    args,
                    kwargs,
                },
                total_len,
            ))
        }
        "DropObject" => {
            let (req_id, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (handle, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("DropObject command has trailing data".to_string());
            }
            Ok((PyCommand::DropObject { req_id, handle }, total_len))
        }
        _ => Err(format!("unknown PyCommand form {head:?}")),
    }
}

fn parse_py_result_expr(expr: Expr) -> Result<(PyResult, usize), String> {
    let (head, mut offset, total_len) = parse_form_header(expr)?;
    match head.as_str() {
        "Ok" => {
            let value_expr = Expr {
                ptr: unsafe { expr.ptr.add(offset) },
            };
            let (value, consumed) = parse_py_value_expr(value_expr)?;
            offset += consumed;
            if offset != total_len {
                return Err("Ok result has trailing data".to_string());
            }
            Ok((PyResult::Ok(value), total_len))
        }
        "Error" => {
            let (error_type, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (message, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            let (traceback, consumed) = parse_symbol_at(expr, offset)?;
            offset += consumed;
            if offset != total_len {
                return Err("Error result has trailing data".to_string());
            }
            Ok((
                PyResult::Error {
                    error_type,
                    message,
                    traceback,
                },
                total_len,
            ))
        }
        _ => Err(format!("unknown PyResult form {head:?}")),
    }
}

fn write_py_value<W: Write>(out: &mut W, value: &PyValue) -> Result<(), std::io::Error> {
    match value {
        PyValue::None => write_symbol(out, "None"),
        PyValue::Bool(flag) => {
            write_form_start(out, "Bool", 1)?;
            write_symbol(out, if *flag { "true" } else { "false" })
        }
        PyValue::Int(value) => {
            write_form_start(out, "Int", 1)?;
            write_symbol(out, &value.to_string())
        }
        PyValue::Float(value) => {
            write_form_start(out, "Float", 1)?;
            write_symbol(out, &value.to_string())
        }
        PyValue::Str(value) => {
            write_form_start(out, "Str", 1)?;
            write_symbol(out, value)
        }
        PyValue::List(values) => {
            write_form_start(out, "List", values.len())?;
            for value in values {
                write_py_value(out, value)?;
            }
            Ok(())
        }
        PyValue::Dict(entries) => {
            write_form_start(out, "Dict", entries.len())?;
            for (key, value) in entries {
                write_form_start(out, "Entry", 2)?;
                write_symbol(out, key)?;
                write_py_value(out, value)?;
            }
            Ok(())
        }
        PyValue::Handle(handle) => {
            write_form_start(out, "Handle", 1)?;
            write_symbol(out, handle)
        }
    }
}

fn write_py_kwargs<W: Write>(
    out: &mut W,
    kwargs: &[(String, PyValue)],
) -> Result<(), std::io::Error> {
    write_form_start(out, "Kwargs", kwargs.len())?;
    for (key, value) in kwargs {
        write_form_start(out, "Entry", 2)?;
        write_symbol(out, key)?;
        write_py_value(out, value)?;
    }
    Ok(())
}

fn write_py_args<W: Write>(out: &mut W, args: &[PyValue]) -> Result<(), std::io::Error> {
    write_form_start(out, "Args", args.len())?;
    for arg in args {
        write_py_value(out, arg)?;
    }
    Ok(())
}

fn write_py_command<W: Write>(out: &mut W, command: &PyCommand) -> Result<(), std::io::Error> {
    match command {
        PyCommand::Import { req_id, module } => {
            write_form_start(out, "Import", 2)?;
            write_symbol(out, req_id)?;
            write_symbol(out, module)
        }
        PyCommand::ImportFile {
            req_id,
            module,
            path,
        } => {
            write_form_start(out, "ImportFile", 3)?;
            write_symbol(out, req_id)?;
            write_symbol(out, module)?;
            write_symbol(out, path)
        }
        PyCommand::GetAttr {
            req_id,
            target,
            attr,
        } => {
            write_form_start(out, "GetAttr", 3)?;
            write_symbol(out, req_id)?;
            write_py_value(out, target)?;
            write_symbol(out, attr)
        }
        PyCommand::GetVariable { req_id, name } => {
            write_form_start(out, "GetVariable", 2)?;
            write_symbol(out, req_id)?;
            write_symbol(out, name)
        }
        PyCommand::SetVariable {
            req_id,
            name,
            value,
        } => {
            write_form_start(out, "SetVariable", 3)?;
            write_symbol(out, req_id)?;
            write_symbol(out, name)?;
            write_py_value(out, value)
        }
        PyCommand::Call {
            req_id,
            module,
            function,
            args,
            kwargs,
        } => {
            write_form_start(out, "Call", 5)?;
            write_symbol(out, req_id)?;
            write_symbol(out, module)?;
            write_symbol(out, function)?;
            write_py_args(out, args)?;
            write_py_kwargs(out, kwargs)
        }
        PyCommand::CallMethod {
            req_id,
            target,
            method,
            args,
            kwargs,
        } => {
            write_form_start(out, "CallMethod", 5)?;
            write_symbol(out, req_id)?;
            write_py_value(out, target)?;
            write_symbol(out, method)?;
            write_py_args(out, args)?;
            write_py_kwargs(out, kwargs)
        }
        PyCommand::DropObject { req_id, handle } => {
            write_form_start(out, "DropObject", 2)?;
            write_symbol(out, req_id)?;
            write_symbol(out, handle)
        }
    }
}

fn write_py_result<W: Write>(out: &mut W, result: &PyResult) -> Result<(), std::io::Error> {
    match result {
        PyResult::Ok(value) => {
            write_form_start(out, "Ok", 1)?;
            write_py_value(out, value)
        }
        PyResult::Error {
            error_type,
            message,
            traceback,
        } => {
            write_form_start(out, "Error", 3)?;
            write_symbol(out, error_type)?;
            write_symbol(out, message)?;
            write_symbol(out, traceback)
        }
    }
}

impl SerializableExpr for PyValue {
    fn size(&self) -> usize {
        let mut buf = Vec::new();
        SerializableExpr::serialize(self, &mut buf).expect("serializing PyValue into Vec");
        buf.len()
    }

    fn serialize<W: Write>(&self, out: &mut W) -> Result<(), std::io::Error> {
        write_py_value(out, self)
    }
}

impl DeserializableExpr for PyValue {
    fn advanced(e: Expr) -> usize {
        parse_py_value_expr(e)
            .map(|(_, len)| len)
            .expect("invalid PyValue expression")
    }

    fn check(e: Expr) -> bool {
        parse_py_value_expr(e).is_ok()
    }

    fn deserialize_unchecked(e: Expr) -> Self {
        parse_py_value_expr(e)
            .map(|(value, _)| value)
            .expect("invalid PyValue expression")
    }
}

impl SerializableExpr for PyCommand {
    fn size(&self) -> usize {
        let mut buf = Vec::new();
        SerializableExpr::serialize(self, &mut buf).expect("serializing PyCommand into Vec");
        buf.len()
    }

    fn serialize<W: Write>(&self, out: &mut W) -> Result<(), std::io::Error> {
        write_py_command(out, self)
    }
}

impl DeserializableExpr for PyCommand {
    fn advanced(e: Expr) -> usize {
        parse_py_command_expr(e)
            .map(|(_, len)| len)
            .expect("invalid PyCommand expression")
    }

    fn check(e: Expr) -> bool {
        parse_py_command_expr(e).is_ok()
    }

    fn deserialize_unchecked(e: Expr) -> Self {
        parse_py_command_expr(e)
            .map(|(value, _)| value)
            .expect("invalid PyCommand expression")
    }
}

impl SerializableExpr for PyResult {
    fn size(&self) -> usize {
        let mut buf = Vec::new();
        SerializableExpr::serialize(self, &mut buf).expect("serializing PyResult into Vec");
        buf.len()
    }

    fn serialize<W: Write>(&self, out: &mut W) -> Result<(), std::io::Error> {
        write_py_result(out, self)
    }
}

impl DeserializableExpr for PyResult {
    fn advanced(e: Expr) -> usize {
        parse_py_result_expr(e)
            .map(|(_, len)| len)
            .expect("invalid PyResult expression")
    }

    fn check(e: Expr) -> bool {
        parse_py_result_expr(e).is_ok()
    }

    fn deserialize_unchecked(e: Expr) -> Self {
        parse_py_result_expr(e)
            .map(|(value, _)| value)
            .expect("invalid PyResult expression")
    }
}

pub fn py_value_to_expr_bytes(value: &PyValue) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    SerializableExpr::serialize(value, &mut out)?;
    Ok(out)
}

pub fn py_value_from_expr(expr: Expr) -> Result<PyValue, String> {
    parse_py_value_expr(expr).map(|(value, _)| value)
}

pub fn py_command_to_expr_bytes(command: &PyCommand) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    SerializableExpr::serialize(command, &mut out)?;
    Ok(out)
}

pub fn py_command_from_expr(expr: Expr) -> Result<PyCommand, String> {
    parse_py_command_expr(expr).map(|(value, _)| value)
}

pub fn py_query_to_expr_bytes(query: &PyQuery) -> Result<Vec<u8>, std::io::Error> {
    py_command_to_expr_bytes(query)
}

pub fn py_query_from_expr(expr: Expr) -> Result<PyQuery, String> {
    py_command_from_expr(expr)
}

pub fn py_result_to_expr_bytes(result: &PyResult) -> Result<Vec<u8>, std::io::Error> {
    let mut out = Vec::new();
    SerializableExpr::serialize(result, &mut out)?;
    Ok(out)
}

pub fn py_result_from_expr(expr: Expr) -> Result<PyResult, String> {
    parse_py_result_expr(expr).map(|(value, _)| value)
}
