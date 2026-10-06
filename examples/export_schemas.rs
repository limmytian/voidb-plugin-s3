use std::fs;
use std::path::Path;
use voidb_plugin_s3::s3_capabilities;

fn main() -> anyhow::Result<()> {
    let schemas_dir = Path::new("schemas");
    fs::create_dir_all(schemas_dir)?;

    let capabilities = s3_capabilities();

    // Export capability schemas
    for cap in &capabilities {
        let input_path = schemas_dir.join(format!("{}-input.schema.json", cap.id));
        let output_path = schemas_dir.join(format!("{}-output.schema.json", cap.id));

        fs::write(&input_path, serde_json::to_string_pretty(&cap.input_schema)? + "\n")?;
        fs::write(&output_path, serde_json::to_string_pretty(&cap.output_schema)? + "\n")?;
        println!("Exported schemas for capability: {}", cap.id);
    }

    // Export profile schema
    let profile_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "S3ConnectionProfile",
        "type": "object",
        "required": ["provider", "auth"],
        "properties": {
            "provider": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Aws", "Minio", "R2", "Custom"]
                    },
                    "region": {
                        "type": "string",
                        "description": "AWS or custom region"
                    },
                    "endpoint": {
                        "type": "string",
                        "description": "MinIO or custom S3 endpoint URL"
                    },
                    "account_id": {
                        "type": "string",
                        "description": "Cloudflare account ID for R2"
                    },
                    "path_style": {
                        "type": "boolean",
                        "default": true,
                        "description": "Use path-style bucket addressing"
                    }
                }
            },
            "bucket": {
                "type": ["string", "null"],
                "description": "Default bucket name"
            },
            "auth": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["AccessKey", "Auto", "Anonymous"]
                    },
                    "access_key": {
                        "type": "string",
                        "description": "Access Key ID"
                    },
                    "secret_key": {
                        "type": "string",
                        "description": "Secret Access Key"
                    }
                }
            },
            "timeout": {
                "type": "integer",
                "minimum": 1,
                "default": 30,
                "description": "Connection timeout in seconds"
            }
        },
        "additionalProperties": false
    });

    let profile_path = schemas_dir.join("profile.schema.json");
    fs::write(&profile_path, serde_json::to_string_pretty(&profile_schema)? + "\n")?;
    println!("Exported profile schema to {}", profile_path.display());

    Ok(())
}
