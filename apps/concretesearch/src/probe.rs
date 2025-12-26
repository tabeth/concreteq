
#[allow(unused_imports)]
use tantivy::directory::WatchCallback;

// This function will fail compilation with type mismatch, revealing the true type.
pub fn probe() {
    let cb: WatchCallback = unsafe { std::mem::zeroed() };
    let x: usize = cb;
}
