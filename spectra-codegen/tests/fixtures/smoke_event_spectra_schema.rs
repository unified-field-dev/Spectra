spectra_schema! {
    SmokeEvent {
        store: "default",
        table: "smoke_event",
        version: "0.1.0",
        description: "Smoke event for codegen tests",
        fields: [
            message: {
                r#type: String,
                classification: { pii: false, safe_for_console: true },
            },
        ],
    }
}
