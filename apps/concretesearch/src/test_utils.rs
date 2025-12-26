use std::sync::Once;
use foundationdb;

static INIT: Once = Once::new();

pub fn init_fdb() {
    INIT.call_once(|| {
        let network = unsafe { foundationdb::boot() };
        // Leak the network handle to keep the background thread alive throughout the process lifetime
        std::mem::forget(network);
    });
}

pub async fn clear_subspace(db: &foundationdb::Database, prefix: &[u8]) {
    let subspace = foundationdb::tuple::Subspace::from(prefix);
    let _ = db.run(|trx, _maybe_committed| {
        let subspace = subspace.clone();
        async move {
            let (begin, end) = subspace.range();
            trx.clear_range(&begin, &end);
            Ok(())
        }
    }).await;
}
