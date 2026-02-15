use nexus_core::config::Settings;
use nexus_db::{CatalogStore, Db, JobStore, LineageStore, PipelineStore};

#[derive(Clone)]
pub struct ApiState {
    pub settings: Settings,
    pub db: Db,
    pub jobs: JobStore,
    pub catalog: CatalogStore,
    pub lineage: LineageStore,
    pub pipeline: PipelineStore,
}

impl ApiState {
    pub fn new(settings: Settings, db: Db) -> Self {
        let jobs = JobStore::new(db.pool().clone());
        let catalog = CatalogStore::new(db.pool().clone());
        let lineage = LineageStore::new(db.pool().clone());
        let pipeline = PipelineStore::new(db.pool().clone());
        Self {
            settings,
            db,
            jobs,
            catalog,
            lineage,
            pipeline,
        }
    }
}
