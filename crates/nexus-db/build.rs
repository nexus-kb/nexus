use std::fs;
use std::path::Path;

fn main() {
    let migrations_dir = Path::new("migrations");
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    if let Ok(entries) = fs::read_dir(migrations_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
}
