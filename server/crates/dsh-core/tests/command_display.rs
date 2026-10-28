//! F7b guard: `Display for Command` (openraft 0.10 AppData) must render ONLY the
//! variant name — openraft debug-logs `EntryPayload normal:{app_data}`.

use dsh_core::command::Command;

#[test]
fn display_is_variant_name_only() {
    let cmd = Command::RotateMasterKey {
        kek: b"SECRET-KEK".to_vec(),
        kek_enc: b"WRAPPED-SECRET".to_vec(),
    };
    assert_eq!(format!("{}", cmd), "RotateMasterKey");
}

#[test]
fn display_never_leaks_field_content() {
    let secret = "SECRET-KEK";
    let cmd = Command::RotateMasterKey {
        kek: secret.as_bytes().to_vec(),
        kek_enc: b"WRAPPED-SECRET".to_vec(),
    };
    let shown = format!("{}", cmd);
    for needle in [secret, "WRAPPED-SECRET"] {
        assert!(
            !shown.contains(needle),
            "Display leaked {needle:?}: {shown}"
        );
    }
    // other secret-bearing variants also stay redacted
    assert_eq!(
        format!(
            "{}",
            Command::AdminSetPassword {
                password_hash: "hash-value".to_string()
            }
        ),
        "AdminSetPassword"
    );
    assert_eq!(
        format!(
            "{}",
            Command::ProjectAdminSetPassword {
                username: "alice".to_string(),
                salt: "salt-value".to_string(),
                password_hash: "hash-value".to_string()
            }
        ),
        "ProjectAdminSetPassword",
    );
    assert_eq!(
        format!(
            "{}",
            Command::ProjectTokenRevoke {
                project: "p".into(),
                token_id: "token-value".to_string()
            }
        ),
        "ProjectTokenRevoke",
    );
    // Debug still carries the raw bytes as an int array (dev-only, not a log path)
    assert!(format!("{:?}", cmd).contains("kek"));
}
