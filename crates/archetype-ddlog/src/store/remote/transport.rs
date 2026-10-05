//! Cap streamed provider responses before OpenDAL can aggregate them.
use futures::StreamExt;
use http::{Method, Request, Response, StatusCode, header};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport};
use opendal_http_transport_reqwest::ReqwestTransport;

pub(super) struct BoundedTransport(ReqwestTransport);
fn rejected() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Provider response outside bounded protocol",
    )
}
impl BoundedTransport {
    pub fn new() -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| anyhow::anyhow!("Provider transport unavailable"))?;
        Ok(Self(ReqwestTransport::new(client)))
    }
}
impl HttpTransport for BoundedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let head = request.method() == Method::HEAD;
        let range = request
            .headers()
            .get(header::RANGE)
            .map(|value| {
                let text = value.to_str().map_err(|_| rejected())?;
                let (start, end) = text
                    .strip_prefix("bytes=")
                    .and_then(|v| v.split_once('-'))
                    .ok_or_else(rejected)?;
                let start: u64 = start.parse().map_err(|_| rejected())?;
                let end: u64 = end.parse().map_err(|_| rejected())?;
                let count = end
                    .checked_sub(start)
                    .and_then(|n| n.checked_add(1))
                    .ok_or_else(rejected)?;
                if count > 65536 {
                    return Err(rejected());
                }
                Ok((start, end, count))
            })
            .transpose()?;
        let response = self.0.fetch(request).await?;
        cap_response(response, head, range)
    }
}
fn cap_response(
    response: Response<HttpBody>,
    head: bool,
    range: Option<(u64, u64, u64)>,
) -> opendal::Result<Response<HttpBody>> {
    let status = response.status();
    let limit = if head {
        0
    } else if !status.is_success() {
        4096
    } else if let Some((start, end, count)) = range {
        if status != StatusCode::PARTIAL_CONTENT {
            return Err(rejected());
        }
        let content_range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(rejected)?;
        let prefix = format!("bytes {start}-{end}/");
        if !content_range.starts_with(&prefix)
            || content_range[prefix.len()..].parse::<u64>().is_err()
        {
            return Err(rejected());
        }
        count
    } else {
        2 << 20
    };
    if !head {
        if let Some(length) = response.headers().get(header::CONTENT_LENGTH) {
            let length: u64 = length
                .to_str()
                .map_err(|_| rejected())?
                .parse()
                .map_err(|_| rejected())?;
            if length > limit {
                return Err(rejected());
            }
        }
        if response.headers().contains_key(header::CONTENT_ENCODING) {
            return Err(rejected());
        }
    }
    let (parts, body) = response.into_parts();
    let mut consumed = 0u64;
    let body = body.map_inner(move |stream| {
        Box::new(stream.map(move |item| {
            let block = item.map_err(|_| rejected())?;
            consumed = consumed
                .checked_add(block.len() as u64)
                .ok_or_else(rejected)?;
            if consumed > limit {
                return Err(rejected());
            }
            Ok(block)
        }))
    });
    Ok(Response::from_parts(parts, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    fn body(chunks: Vec<Vec<u8>>, seen: Arc<AtomicUsize>) -> HttpBody {
        HttpBody::new(
            futures::stream::iter(chunks.into_iter().map(move |bytes| {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Buffer::from(bytes))
            })),
            None,
        )
    }
    #[tokio::test]
    async fn range_ignored_is_rejected_before_reading_body() {
        let seen = Arc::new(AtomicUsize::new(0));
        let response = Response::builder()
            .status(200)
            .body(body(vec![vec![0; 65536]], seen.clone()))
            .unwrap();
        assert!(cap_response(response, false, Some((0, 3, 4))).is_err());
        assert_eq!(seen.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn oversized_chunked_range_and_error_stop_before_aggregation() {
        for (status, range, limit) in [(206, Some((0, 3, 4)), 4), (403, None, 4096)] {
            let seen = Arc::new(AtomicUsize::new(0));
            let response = Response::builder()
                .status(status)
                .header(header::CONTENT_RANGE, "bytes 0-3/4")
                .body(body(
                    vec![vec![0; limit], vec![0; 1], vec![0; limit]],
                    seen.clone(),
                ))
                .unwrap();
            let mut guarded = cap_response(response, false, range).unwrap().into_body();
            assert!(guarded.to_buffer().await.is_err());
            assert_eq!(seen.load(Ordering::SeqCst), 2);
        }
    }
    // Exercise the exact S3 -> custom transport -> Reqwest composition. HTTP is
    // enabled only here for an owned loopback listener with synthetic keys.
    fn loopback(
        response: Vec<u8>,
    ) -> (
        opendal::Operator,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    ) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sender, received) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0];
            while request.len() < 8192 && !request.ends_with(b"\r\n\r\n") {
                if socket.read(&mut byte).unwrap() == 0 {
                    break;
                }
                request.push(byte[0]);
            }
            sender.send(String::from_utf8(request).unwrap()).unwrap();
            // A client rejecting headers may drop before consuming the body.
            let _ = socket.write_all(&response);
        });
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap();
        let operator = opendal::Operator::new(
            opendal::services::S3::default()
                .bucket("synthetic-bucket")
                .root("task/case")
                .endpoint(&endpoint)
                .region("auto")
                .access_key_id("synthetic-access-key")
                .secret_access_key("synthetic-secret-key")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .unwrap()
        .with_context(opendal::OperationContext::new().with_http_transport(
            opendal::HttpTransporter::new(BoundedTransport(ReqwestTransport::new(client))),
        ));
        (operator, received, thread)
    }
    #[tokio::test]
    async fn selected_s3_client_rejects_ignored_range_and_oversized_chunked_bodies() {
        use super::super::{DataProvider, S3Provider};
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\nConnection: close\r\n\r\n".to_vec(),
            b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-3/4\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nabcde\r\n0\r\n\r\n".to_vec(),
            [b"HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1001\r\n".as_slice(), &vec![b'x';4097], b"\r\n0\r\n\r\n".as_slice()].concat(),
        ] {
            let (operator,received,thread)=loopback(response);
            assert!(S3Provider(operator).range("objects/test",0..4).await.is_err());
            let request=received.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
            assert!(request.to_ascii_lowercase().contains("range: bytes=0-3"));
            thread.join().unwrap();
        }
    }
    #[tokio::test]
    async fn selected_s3_client_emits_conditional_create_and_accepts_exact_range() {
        use super::super::{DataProvider, S3Provider};
        let (operator, received, thread) =
            loopback(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec());
        S3Provider(operator)
            .create("objects/test", bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();
        let request = received
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap();
        assert!(request.to_ascii_lowercase().contains("if-none-match: *"));
        assert!(request.starts_with("PUT "));
        thread.join().unwrap();
        let (operator,received,thread)=loopback(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-3/4\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata".to_vec());
        assert_eq!(
            S3Provider(operator)
                .range("objects/test", 0..4)
                .await
                .unwrap()
                .as_ref(),
            b"data"
        );
        received
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap();
        thread.join().unwrap();
    }
}
