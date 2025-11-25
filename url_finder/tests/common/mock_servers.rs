#![allow(dead_code)]

use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub struct MockExternalServices {
    pub lotus: MockServer,
    pub cid_contact: MockServer,
    pub piece_server: MockServer,
}

impl MockExternalServices {
    pub async fn start() -> Self {
        Self {
            lotus: MockServer::start().await,
            cid_contact: MockServer::start().await,
            piece_server: MockServer::start().await,
        }
    }

    pub fn lotus_url(&self) -> String {
        self.lotus.uri()
    }

    pub fn cid_contact_url(&self) -> String {
        self.cid_contact.uri()
    }

    pub fn piece_server_url(&self) -> String {
        self.piece_server.uri()
    }

    pub async fn setup_lotus_peer_id_mock(
        &self,
        provider: &str,
        peer_id: &str,
        multiaddrs: Vec<String>,
    ) {
        // force fallback to lotus
        Mock::given(method("POST"))
            .and(path("/rpc/v1"))
            .and(body_partial_json(json!({
                "method": "eth_call"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": "0x0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            })))
            .mount(&self.lotus)
            .await;

        Mock::given(method("POST"))
            .and(path("/rpc/v1"))
            .and(body_partial_json(json!({
                "method": "Filecoin.StateMinerInfo",
                "params": [provider, null]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "PeerId": peer_id,
                    "Multiaddrs": multiaddrs
                }
            })))
            .mount(&self.lotus)
            .await;
    }

    pub async fn setup_cid_contact_mock(&self, peer_id: &str, multiaddrs: Vec<String>) {
        let response_body = json!({
            "Publisher": {
                "ID": peer_id,
                "Addrs": multiaddrs
            }
        });

        Mock::given(method("GET"))
            .and(path(format!("/providers/{peer_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
            .mount(&self.cid_contact)
            .await;
    }

    pub async fn setup_piece_retrieval_mock(&self, piece_cid: &str, should_succeed: bool) {
        if should_succeed {
            for http_method in ["GET", "HEAD"] {
                Mock::given(method(http_method))
                    .and(path(format!("/piece/{piece_cid}")))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "application/piece")
                            .insert_header("etag", "\"mock-etag-12345\"")
                            .set_body_raw(vec![0u8; 100], "application/piece"),
                    )
                    .mount(&self.piece_server)
                    .await;
            }
        } else {
            for http_method in ["GET", "HEAD"] {
                Mock::given(method(http_method))
                    .and(path(format!("/piece/{piece_cid}")))
                    .respond_with(ResponseTemplate::new(404))
                    .mount(&self.piece_server)
                    .await;
            }
        }
    }
}
