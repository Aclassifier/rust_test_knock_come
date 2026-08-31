## Knock-come

See the "knock-come" described at https://www.teigfam.net/oyvind/home/technology/009-the-knock-come-deadlock-free-pattern/

This is an implementation of the Knock-come pattern as described in the above note.

This is "my" first ever Rust project. I have used the Google AI mode to help me. It was like having a sometimes failing but then soon up again with next try friend beside me. Good for a retired alone sitting person! 

Also see https://github.com/Aclassifier/xc_test_knock_come

### Automated build and packaging pipeline

The project includes a robust automation workflow through the `build_app.sh` script, which treats the Rust source code (`rust_test_knock_come.rs`) as the single source of truth. 

#### How it works:
* **Dynamic extraction:** The script parses the Rust file using `grep` and `sed` to automatically extract the active semantic mode, version number, and the exact app bundle name defined inside the `CURRENT_APP_NAME` match block
* **Safe configuration injecting:** To package the application with context-specific metadata without altering project files permanently, the script creates a temporary backup (`Cargo.toml.bak`) and injects the extracted strings into placeholder slots in `Cargo.toml`
* **Zero-side-effect bundling:** It triggers the compilation via `cargo bundle --release`. Immediately after compilation—regardless of success or failure—the modified configuration is discarded and the pristine, original `Cargo.toml` is fully restored from the backup, ensuring a clean and failure-proof build cycle


