//! Compile-time channel, variant-shape, and payload codec contracts.
#![cfg(feature = "sonic")]

#[test]
fn integration_compile_time_macro_contracts() {
    let cases = trybuild::TestCases::new();
    let fixtures =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../pgnotify-macros/tests/ui");
    cases.pass(fixtures.join("valid_contracts.rs"));
    cases.compile_fail(fixtures.join("reject_*.rs"));
}
