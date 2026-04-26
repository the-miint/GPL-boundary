//! Integration test: verify the binary links and runs successfully.

use std::process::Command;

#[test]
fn test_version_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .arg("--version")
        .output()
        .expect("Failed to execute gpl-boundary");

    assert!(
        output.status.success(),
        "gpl-boundary --version failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fasttree"),
        "Version output missing fasttree: {stdout}"
    );
    assert!(
        stdout.contains("prodigal"),
        "Version output missing prodigal: {stdout}"
    );
    assert!(
        stdout.contains("gpl_boundary"),
        "Version output missing gpl_boundary: {stdout}"
    );
    assert!(
        stdout.contains("bowtie2-align"),
        "Version output missing bowtie2-align: {stdout}"
    );
    assert!(
        stdout.contains("bowtie2-build"),
        "Version output missing bowtie2-build: {stdout}"
    );
}

#[test]
fn test_list_tools_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .arg("--list-tools")
        .output()
        .expect("Failed to execute gpl-boundary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("fasttree"),
        "list-tools missing fasttree: {stdout}"
    );
    assert!(
        stdout.contains("prodigal"),
        "list-tools missing prodigal: {stdout}"
    );
    assert!(
        stdout.contains("bowtie2-align"),
        "list-tools missing bowtie2-align: {stdout}"
    );
    assert!(
        stdout.contains("bowtie2-build"),
        "list-tools missing bowtie2-build: {stdout}"
    );
}
