use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options, WriteBatch};
use tempfile::TempDir;

const META: &str = "meta";
const COVERAGE_RANGES: &str = "coverage_ranges";
const BLOCKS_BY_HASH: &str = "blocks_by_hash";
const BLOCKS_BY_SCORE: &str = "blocks_by_score";
const TX_BY_ID: &str = "tx_by_id";
const TX_DETAIL_BY_ID: &str = "tx_detail_by_id";
const TX_ACCEPTANCE: &str = "tx_acceptance";
const ADDRESS_HISTORY: &str = "address_history";
const ADDRESS_UTXOS: &str = "address_utxos";
const OUTPOINT_STATE: &str = "outpoint_state";
const SPENDS_BY_OUTPOINT: &str = "spends_by_outpoint";
const MEMPOOL: &str = "mempool";

const ROWS: u64 = 10_000;
const PAGE_ROWS: usize = 100;

fn open_db() -> (TempDir, DB) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let families = [
        META,
        COVERAGE_RANGES,
        BLOCKS_BY_HASH,
        BLOCKS_BY_SCORE,
        TX_BY_ID,
        TX_DETAIL_BY_ID,
        TX_ACCEPTANCE,
        ADDRESS_HISTORY,
        ADDRESS_UTXOS,
        OUTPOINT_STATE,
        SPENDS_BY_OUTPOINT,
        MEMPOOL,
    ]
    .into_iter()
    .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));

    let db = DB::open_cf_descriptors(&opts, dir.path(), families).expect("open rocksdb");
    (dir, db)
}

fn fill_db() -> (TempDir, DB) {
    let (dir, db) = open_db();
    let mut batch = WriteBatch::default();

    for i in 0..ROWS {
        batch.put_cf(&db.cf_handle(TX_BY_ID).expect("tx cf"), txid(i), value(i));
        batch.put_cf(
            &db.cf_handle(BLOCKS_BY_SCORE).expect("block score cf"),
            block_score_key(i),
            value(i),
        );
        batch.put_cf(
            &db.cf_handle(ADDRESS_HISTORY).expect("address history cf"),
            address_history_key(i % 1_000, i, i),
            value(i),
        );
        batch.put_cf(
            &db.cf_handle(ADDRESS_UTXOS).expect("address utxos cf"),
            address_utxo_key(i % 1_000, i),
            value(i),
        );
    }

    db.write(batch).expect("seed rocksdb");
    (dir, db)
}

fn txid(seed: u64) -> [u8; 32] {
    fixed_32(seed ^ 0x7478)
}

fn script_hash(seed: u64) -> [u8; 32] {
    fixed_32(seed ^ 0x51c1_9170)
}

fn fixed_32(seed: u64) -> [u8; 32] {
    let mut out = [0_u8; 32];
    for chunk in 0..4 {
        let value = seed
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(chunk as u64);
        out[chunk * 8..(chunk + 1) * 8].copy_from_slice(&value.to_be_bytes());
    }
    out
}

fn value(seed: u64) -> [u8; 96] {
    let mut out = [0_u8; 96];
    for chunk in 0..12 {
        let value = seed.wrapping_mul(31).wrapping_add(chunk as u64);
        out[chunk * 8..(chunk + 1) * 8].copy_from_slice(&value.to_be_bytes());
    }
    out
}

fn block_score_key(score: u64) -> [u8; 40] {
    let mut key = [0_u8; 40];
    key[..8].copy_from_slice(&score.to_be_bytes());
    key[8..].copy_from_slice(&fixed_32(score));
    key
}

fn address_history_key(address_seed: u64, daa_score: u64, tx_seed: u64) -> [u8; 74] {
    let mut key = [0_u8; 74];
    key[..32].copy_from_slice(&script_hash(address_seed));
    key[32..40].copy_from_slice(&(!daa_score).to_be_bytes());
    key[40..72].copy_from_slice(&txid(tx_seed));
    key[72..].copy_from_slice(&0_u16.to_be_bytes());
    key
}

fn address_history_prefix(address_seed: u64) -> [u8; 32] {
    script_hash(address_seed)
}

fn address_utxo_key(address_seed: u64, tx_seed: u64) -> [u8; 68] {
    let mut key = [0_u8; 68];
    key[..32].copy_from_slice(&script_hash(address_seed));
    key[32..64].copy_from_slice(&txid(tx_seed));
    key[64..].copy_from_slice(&0_u32.to_be_bytes());
    key
}

fn bench_txid_lookup(c: &mut Criterion) {
    let (_dir, db) = fill_db();
    let cf = db.cf_handle(TX_BY_ID).expect("tx cf");
    let mut i = 0_u64;

    c.bench_function("rocks_txid_point_lookup", |b| {
        b.iter(|| {
            i = i.wrapping_add(1);
            db.get_cf(&cf, txid(i % ROWS)).expect("lookup")
        });
    });
}

fn bench_block_score_scan(c: &mut Criterion) {
    let (_dir, db) = fill_db();
    let cf = db.cf_handle(BLOCKS_BY_SCORE).expect("block score cf");
    let start = block_score_key(5_000);

    c.bench_function("rocks_block_score_page_scan", |b| {
        b.iter(|| {
            db.iterator_cf(&cf, IteratorMode::From(&start, rocksdb::Direction::Forward))
                .take(PAGE_ROWS)
                .count()
        });
    });
}

fn bench_address_history_scan(c: &mut Criterion) {
    let (_dir, db) = fill_db();
    let cf = db.cf_handle(ADDRESS_HISTORY).expect("address history cf");
    let prefix = address_history_prefix(42);

    c.bench_function("rocks_address_history_page_scan", |b| {
        b.iter(|| db.prefix_iterator_cf(&cf, prefix).take(PAGE_ROWS).count());
    });
}

fn bench_address_utxo_update(c: &mut Criterion) {
    c.bench_function("rocks_address_utxo_create_spend_batch", |b| {
        b.iter_batched(
            fill_db,
            |(_dir, db)| {
                let cf = db.cf_handle(ADDRESS_UTXOS).expect("address utxos cf");
                let mut batch = WriteBatch::default();
                for i in 0..1_000 {
                    batch.put_cf(&cf, address_utxo_key(7, ROWS + i), value(i));
                    batch.delete_cf(&cf, address_utxo_key(7, i));
                }
                db.write(batch).expect("utxo update batch");
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_mixed_ingest(c: &mut Criterion) {
    c.bench_function("rocks_mixed_ingest_batch", |b| {
        b.iter_batched(
            open_db,
            |(_dir, db)| {
                let tx_cf = db.cf_handle(TX_BY_ID).expect("tx cf");
                let block_cf = db.cf_handle(BLOCKS_BY_SCORE).expect("block score cf");
                let history_cf = db.cf_handle(ADDRESS_HISTORY).expect("history cf");
                let utxo_cf = db.cf_handle(ADDRESS_UTXOS).expect("utxo cf");

                for batch_index in 0..10_u64 {
                    let mut batch = WriteBatch::default();
                    for row in 0..1_000_u64 {
                        let i = batch_index * 1_000 + row;
                        batch.put_cf(&tx_cf, txid(i), value(i));
                        batch.put_cf(&block_cf, block_score_key(i), value(i));
                        batch.put_cf(&history_cf, address_history_key(i % 1_000, i, i), value(i));
                        batch.put_cf(&utxo_cf, address_utxo_key(i % 1_000, i), value(i));
                    }
                    db.write(batch).expect("mixed ingest batch");
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_txid_lookup,
    bench_block_score_scan,
    bench_address_history_scan,
    bench_address_utxo_update,
    bench_mixed_ingest
);
criterion_main!(benches);
