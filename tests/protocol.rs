//! ワーカープロトコルの試験。protocol/worker-protocol.json と Rust 側の定義の一致を確かめる。

use watapachi_bot::resolver::protocol::{
	parse_response, ErrorCode, ProtocolError, WorkerRequest, WorkerResponse, OPS,
};

fn definition() -> serde_json::Value {
	let text = include_str!("../protocol/worker-protocol.json");
	serde_json::from_str(text).expect("protocol.json は JSON")
}

fn names(value: &serde_json::Value) -> Vec<String> {
	value
		.as_array()
		.expect("配列")
		.iter()
		.map(|v| v.as_str().expect("文字列").to_string())
		.collect()
}

#[test]
fn ops_and_error_codes_match_protocol_json() {
	let json = definition();
	assert_eq!(
		OPS.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
		names(&json["ops"])
	);
	let codes: Vec<String> = ErrorCode::ALL
		.iter()
		.map(|c| c.as_str().to_string())
		.collect();
	assert_eq!(codes, names(&json["error_codes"]));
	// 全ての操作が WorkerRequest の直列化結果の op と対応する
	let requests = [
		WorkerRequest::Resolve {
			url: "u".into(),
			format_id: None,
		},
		WorkerRequest::FlatPlaylist {
			url: "u".into(),
			limit: 1,
		},
		WorkerRequest::Search {
			query: "q".into(),
			limit: 1,
		},
		WorkerRequest::Ping,
	];
	let ops: Vec<String> = requests
		.iter()
		.map(|r| {
			serde_json::from_str::<serde_json::Value>(&r.to_line(1).unwrap()).unwrap()["op"]
				.as_str()
				.unwrap()
				.to_string()
		})
		.collect();
	assert_eq!(ops, names(&json["ops"]));
}

#[test]
fn request_line_has_id_and_no_newline() {
	let line = WorkerRequest::Resolve {
		url: "https://x/\n".into(),
		format_id: Some("140".into()),
	}
	.to_line(9)
	.unwrap();
	assert!(!line.contains('\n'));
	let value: serde_json::Value = serde_json::from_str(&line).unwrap();
	assert_eq!(value["id"], 9);
	assert_eq!(value["format_id"], "140");
	let ping: serde_json::Value =
		serde_json::from_str(&WorkerRequest::Ping.to_line(4).unwrap()).unwrap();
	assert_eq!(ping, serde_json::json!({"id": 4, "op": "ping"}));
}

#[test]
fn parses_resolve_response() {
	let line = r#"{"id": 1, "ok": true, "result": {"id": "abc", "title": "t", "duration": 262, "is_live": false}}"#;
	match parse_response(line).unwrap() {
		WorkerResponse::Success { id, result } => {
			assert_eq!(id, Some(1));
			assert_eq!(result["title"], "t");
			assert_eq!(result["is_live"], false);
		}
		other => panic!("成功のはず: {other:?}"),
	}
}

#[test]
fn parses_error_response() {
	let line = r#"{"id": 2, "ok": false, "error": {"code": "not_found", "message": "gone"}}"#;
	match parse_response(line).unwrap() {
		WorkerResponse::Failure { id, code, message } => {
			assert_eq!(id, Some(2));
			assert_eq!(code, ErrorCode::NotFound);
			assert_eq!(message, "gone");
		}
		other => panic!("失敗のはず: {other:?}"),
	}
}

#[test]
fn null_id_and_unknown_code_are_handled() {
	let line = r#"{"id": null, "ok": false, "error": {"code": "weird", "message": "m"}}"#;
	match parse_response(line).unwrap() {
		WorkerResponse::Failure { id, code, .. } => {
			assert_eq!(id, None);
			assert_eq!(code, ErrorCode::Internal);
		}
		other => panic!("失敗のはず: {other:?}"),
	}
}

#[test]
fn rejects_malformed_lines() {
	assert!(matches!(
		parse_response("not json"),
		Err(ProtocolError::Json(_))
	));
	assert!(matches!(
		parse_response(r#"{"id":1,"ok":true}"#),
		Err(ProtocolError::Malformed(_))
	));
	assert!(matches!(
		parse_response(r#"{"id":1,"ok":false}"#),
		Err(ProtocolError::Malformed(_))
	));
}
