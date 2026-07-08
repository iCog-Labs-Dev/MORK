/// Small CLI formatting helpers for the `run` flow.
///
/// Keeping the terminal layout in one place makes the output consistent
/// whether the command is printing status, timing, or final results.
pub fn print_box(title: &str, lines: &[String]) {
    // Measure by character count so the borders stay aligned even when the
    // content includes multi-byte UTF-8 symbols.
    let content_width = lines
        .iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0)
        .max(title.chars().count())
        + 2;

    println!("╭─ {} {}╮", title, "─".repeat(content_width - title.len()-1));

    for line in lines {
        println!("│ {:<width$} │", line, width = content_width);
    }

    println!("╰{}╯", "─".repeat(content_width + 2));
}

/// Print the run header so the input and destination are visible up front.
pub fn print_header(file: &str, output: &str) {
    let lines = vec![
        format!("File: {}", file),
        format!("Output: {}", output),
    ];

    let width = lines
        .iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0);

    println!("╔{}╗", "═".repeat(width + 2));

    for line in lines {
        println!("║ {:<width$} ║", line, width = width);
    }

    println!("╚{}╝", "═".repeat(width + 2));
    println!();
}

/// Emit a short status line when instrumentation is enabled.
pub fn print_loaded(expressions: usize) {
    println!("✓ Loaded {} expressions", expressions);
    println!();
}

/// Summarize the execution pass so users can see work, time, and counters at a glance.
pub fn print_execution(
    steps: usize,
    elapsed_ms: u128,
    unifications: usize,
    writes: usize,
    transitions: usize,
) {
    let lines = vec![
        format!("Steps: {}", steps),
        format!("Time: {} ms", elapsed_ms),
        format!("Unifications: {}", unifications),
        format!("Writes: {}", writes),
        format!("Transitions: {}", transitions),
    ];

    print_box("Execution", &lines);
    println!();
}

/// Show the size of the data that is about to be dumped when instrumentation is active.
pub fn print_dumping(count: usize) {
    let lines = vec![
        format!("Expressions: {}", count),
    ];

    print_box("Dumping", &lines);
    println!();
}

/// Frame the final result so multi-line output is still easy to scan in the terminal.
pub fn print_result(result: &str) {
    let lines: Vec<String> =
        result.lines().map(|s| s.to_string()).collect();

    print_box("Result", &lines);
}