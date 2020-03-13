fn main() {
    println!("cargo:rustc-link-search=./api/src/apic/");
    println!("cargo:rustc-link-lib=wrapper");
}
