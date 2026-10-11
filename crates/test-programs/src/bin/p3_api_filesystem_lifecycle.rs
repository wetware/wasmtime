use futures::{future, join};
use test_programs::p3::wasi::filesystem::types::{DescriptorFlags, OpenFlags, PathFlags};
use test_programs::p3::{wasi, wit_stream};
use wit_bindgen::StreamResult;

struct Component;

test_programs::p3::export!(Component);

impl test_programs::p3::exports::wasi::cli::run::Guest for Component {
    async fn run() -> Result<(), ()> {
        let args = wasi::cli::environment::get_arguments();
        let scenario = args.get(2).expect("missing lifecycle scenario");
        let preopens = wasi::filesystem::preopens::get_directories();
        let (dir, _) = &preopens[0];
        let file = dir
            .open_at(
                PathFlags::empty(),
                "lifecycle.txt".to_string(),
                OpenFlags::CREATE,
                DescriptorFlags::READ | DescriptorFlags::WRITE,
            )
            .await
            .unwrap();

        let contents = vec![b'x'; 16 * 1024 - 1];
        let (mut data_tx, data_rx) = wit_stream::new();
        join!(
            async {
                file.write_via_stream(data_rx, 0).await.unwrap();
            },
            async {
                assert!(data_tx.write_all(contents).await.is_empty());
                drop(data_tx);
            },
        );
        let (mut data_tx, data_rx) = wit_stream::new();
        join!(
            async {
                file.append_via_stream(data_rx).await.unwrap();
            },
            async {
                assert!(data_tx.write_all(vec![b'x']).await.is_empty());
                drop(data_tx);
            },
        );

        let offset = match scenario.as_str() {
            "eof" => 0,
            "error" => u64::MAX,
            other => panic!("unknown lifecycle scenario: {other}"),
        };
        let (mut stream, status) = file.read_via_stream(offset);
        // Closing the status reader must not end an otherwise-live stream
        // session.
        drop(status);

        if scenario == "eof" {
            for _ in 0..2 {
                let (result, buffer) = stream.read(Vec::with_capacity(8 * 1024)).await;
                assert!(matches!(result, StreamResult::Complete(_)));
                assert_eq!(buffer.len(), 8 * 1024);
            }
        }

        let (result, buffer) = stream.read(Vec::with_capacity(8 * 1024)).await;
        assert!(matches!(result, StreamResult::Dropped));
        assert!(buffer.is_empty());

        // Keep the inert stream resource alive so the host test can prove the
        // admission value was released by terminal EOF/error, rather than by
        // resource destruction or Store teardown.
        future::pending::<()>().await;
        drop(stream);
        Ok(())
    }
}

fn main() {
    unreachable!()
}
