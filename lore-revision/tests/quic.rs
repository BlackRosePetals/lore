// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
mod tests {
    use std::any::type_name_of_val;
    use std::mem::MaybeUninit;
    use std::ops::Deref;
    use std::sync::Arc;

    use bytes::Bytes;
    use lore_base::types::Address;
    use lore_base::types::Fragment;
    use lore_transport::ProtocolError;
    use lore_transport::Storage;
    use lore_transport::quic::storage_service::client::StorageClient;

    include!("helper.rs");

    #[allow(clippy::explicit_deref_methods, clippy::mem_forget)]
    #[test]
    fn test_futures_size() {
        // Check async body size with
        // RUSTFLAGS="-Zprint-type-sizes" cargo +nightly build --release --package lore -j1 >sizes.txt
        //
        // Then grep for the source line in sizes.txt, which at time of writing would be e.g.
        // grep "lore-core/src/protocol/quic/storage_service/client.rs:234:67" sizes.txt
        // and similar for inner bodies/functions
        //
        // This test verifies the actual runtime future size

        // We are not actually using it, just need to be able to construct the future of calling a function
        // through an async_trait dyn dispatch - which will put the future on heap, meaning we care about the size
        let client = {
            let client_uninit = Box::new(MaybeUninit::<StorageClient>::uninit());
            let client_raw = Box::into_raw(client_uninit) as *const StorageClient;
            unsafe { Arc::from_raw(client_raw as *const dyn Storage) }
        };
        {
            let address = Address::default();
            let quic_get_future = client.get(0, &address);
            let actual_type = type_name_of_val(&quic_get_future);
            let actual_size = size_of_val(&quic_get_future);

            let val = quic_get_future.as_ref();
            let val_ref = val.deref();
            let quic_get_future_size = size_of_val(val_ref);
            let quic_get_future_type = type_name_of_val(val_ref);

            let error_size = size_of::<ProtocolError>();
            let result_size = size_of::<Result<(Fragment, Bytes), ProtocolError>>();

            // Never raise this value - if the test fails, it means the code that caused the
            // future size to increase must be fixed/refactored. This constant should remain.
            const EXPECTED_SIZE: usize = 320;
            assert!(
                quic_get_future_size <= EXPECTED_SIZE,
                "If this test fails, the protocol get future size has increased and you need to look into why\nQUIC protocol get inner future \"{quic_get_future_type}\" currently {quic_get_future_size} bytes, should be {EXPECTED_SIZE} or less\nActual future type \"{actual_type}\" and size {actual_size}\nProtocolError size: {error_size}\nResult size: {result_size}"
            );
            // Make sure we don't call the function on the uninit object
            std::mem::forget(quic_get_future);
        }
        std::mem::forget(client);
    }

    mod v2 {
        use lore_transport::quic::command_header::CommandHeader;
        use lore_transport::quic::storage_service::Command;

        #[test]
        fn test_parse_command_header() {
            let mut bytes = vec![];
            bytes.extend_from_slice(&(3u8.to_le_bytes()));
            bytes.extend_from_slice(&(65536u32.to_le_bytes()[..3]));
            bytes.extend_from_slice(&(834u32.to_le_bytes()));

            assert_eq!(
                CommandHeader {
                    cmd: 3,
                    error: false,
                    size_or_status: 65536,
                    command_id: 834,
                    session_id: 0,
                    v4: false,
                },
                CommandHeader::from_bytes(bytes.as_slice())
            );

            assert_eq!(
                CommandHeader {
                    cmd: 3,
                    error: false,
                    size_or_status: 65536,
                    command_id: 834,
                    session_id: 0,
                    v4: false,
                }
                .to_bytes()
                .as_ref(),
                bytes.as_slice()
            );
        }

        #[test]
        fn test_parse_from_bytes() {
            let hex = "0010000000000000";

            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("Failed to parse hex"))
                .collect::<Vec<u8>>();

            let byte_array: [u8; 8] = bytes.clone().try_into().expect("incorrect length");
            let header = CommandHeader::from_bytes(&byte_array);

            assert_eq!(
                header,
                CommandHeader {
                    cmd: Command::Authorize as u8,
                    error: false,
                    size_or_status: 16,
                    command_id: 0,
                    session_id: 0,
                    v4: false,
                }
            );

            assert_eq!(bytes.as_slice(), header.to_bytes().as_ref());

            let hex_out = hex::encode(bytes);
            assert_eq!(hex, hex_out);
        }

        #[test]
        fn all_bits_set_parsed_correctly() {
            let expected_bytes = vec![0b11111111; 8];

            let parsed_header = CommandHeader::from_bytes(&expected_bytes);
            assert_eq!(parsed_header.cmd, u8::MAX);
            assert!(parsed_header.error);
            // is meant to be a u23
            assert_eq!(parsed_header.size_or_status, 0b11111111111111111111111);
            assert_eq!(parsed_header.command_id, u32::MAX);

            let to_bytes = parsed_header.to_bytes();
            assert_eq!(to_bytes, expected_bytes.as_slice());
        }

        #[test]
        fn no_bits_set_parsed_correctly() {
            let expected_bytes = vec![0b00000000; 8];

            let parsed_header = CommandHeader::from_bytes(&expected_bytes);
            assert_eq!(parsed_header.cmd, 0);
            assert!(!parsed_header.error);
            assert_eq!(parsed_header.size_or_status, 0);
            assert_eq!(parsed_header.command_id, 0);

            let to_bytes = parsed_header.to_bytes();
            assert_eq!(to_bytes, expected_bytes.as_slice());
        }

        #[test]
        fn all_bits_but_no_error_bit_parsed_correctly() {
            let mut expected_bytes = vec![0b11111111; 8];
            // error bit not set
            expected_bytes[3] = 0b01111111;

            let parsed_header = CommandHeader::from_bytes(&expected_bytes);
            assert_eq!(parsed_header.cmd, u8::MAX);
            assert!(!parsed_header.error);
            // is meant to be a u23
            assert_eq!(parsed_header.size_or_status, 0b11111111111111111111111);
            assert_eq!(parsed_header.command_id, u32::MAX);

            let to_bytes = parsed_header.to_bytes();
            assert_eq!(to_bytes, expected_bytes.as_slice());
        }

        #[test]
        fn no_bits_but_error_parsed_correctly() {
            let mut expected_bytes = vec![0b00000000; 8];
            // error bit set
            expected_bytes[3] = 0b10000000;

            let parsed_header = CommandHeader::from_bytes(&expected_bytes);
            assert_eq!(parsed_header.cmd, 0);
            assert!(parsed_header.error);
            assert_eq!(parsed_header.size_or_status, 0);
            assert_eq!(parsed_header.command_id, 0);

            let to_bytes = parsed_header.to_bytes();
            assert_eq!(to_bytes, expected_bytes.as_slice());
        }
    }

    mod v4 {
        use lore_transport::quic::command_header::COMMAND_HEADER_SIZE_V4;
        use lore_transport::quic::command_header::CommandHeader;
        use lore_transport::quic::storage_service::Command;

        #[test]
        fn v4_header_size() {
            assert_eq!(COMMAND_HEADER_SIZE_V4, 12);
        }

        #[test]
        fn v4_round_trip() {
            let header = CommandHeader::new_with_session(Command::Authorize as u8, 42, 256, 7);

            let bytes = header.to_bytes_v4();
            assert_eq!(bytes.len(), 12);

            let parsed = CommandHeader::from_bytes_v4(&bytes);
            assert_eq!(parsed, header);
        }

        #[test]
        fn v4_session_id_preserved() {
            let header = CommandHeader::new_with_session(Command::Get as u8, 100, 32, 12345);
            let bytes = header.to_bytes_v4();
            let parsed = CommandHeader::from_bytes_v4(&bytes);

            assert_eq!(parsed.session_id, 12345);
            assert_eq!(parsed.cmd, Command::Get as u8);
            assert_eq!(parsed.command_id, 100);
            assert_eq!(parsed.size_or_status, 32);
            assert!(!parsed.error);
        }

        #[test]
        fn v4_error_response_preserves_session() {
            let header = CommandHeader::new_with_session(Command::Get as u8, 55, 0, 999);
            let error = header.response_error(3);

            assert_eq!(error.session_id, 999);
            assert!(error.error);
            assert_eq!(error.size_or_status, 3);

            let bytes = error.to_bytes_v4();
            let parsed = CommandHeader::from_bytes_v4(&bytes);
            assert_eq!(parsed, error);
        }

        #[test]
        fn v4_success_response_preserves_session() {
            let header = CommandHeader::new_with_session(Command::Put as u8, 77, 0, 500);
            let success = header.response_success(128);

            assert_eq!(success.session_id, 500);
            assert!(!success.error);
            assert_eq!(success.size_or_status, 128);

            let bytes = success.to_bytes_v4();
            let parsed = CommandHeader::from_bytes_v4(&bytes);
            assert_eq!(parsed, success);
        }

        #[test]
        fn v4_from_bytes_reads_v2_prefix_correctly() {
            // Build a v4 header and verify the first 8 bytes match v2 parsing
            let header = CommandHeader::new_with_session(Command::Query as u8, 200, 64, 42);
            let v4_bytes = header.to_bytes_v4();
            let v2_parsed = CommandHeader::from_bytes(&v4_bytes[..8]);

            assert_eq!(v2_parsed.cmd, header.cmd);
            assert_eq!(v2_parsed.command_id, header.command_id);
            assert_eq!(v2_parsed.size_or_status, header.size_or_status);
            assert_eq!(v2_parsed.session_id, 0); // v2 doesn't read session_id
        }

        #[test]
        fn v4_session_id_zero_for_authorize() {
            let header = CommandHeader::new_with_session(Command::Authorize as u8, 1, 20, 0);
            assert_eq!(header.session_id, 0);

            let bytes = header.to_bytes_v4();
            let parsed = CommandHeader::from_bytes_v4(&bytes);
            assert_eq!(parsed.session_id, 0);
        }

        #[test]
        fn authorize_opcode_unchanged() {
            assert_eq!(Command::Authorize as u8, 0);
        }
    }
}
