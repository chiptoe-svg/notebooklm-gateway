import pytest

from nlm_gateway.errors import GatewayError

T0 = 1_760_000_000.0


def test_job_roundtrip_and_isolation(store):
    j = store.create_job("a", "podcast", {"format": "brief"}, ["https://x"], ["up_1"], T0)
    assert j.status == "new" and j.urls == ["https://x"] and j.upload_ids == ["up_1"] and j.planned_sources == 2
    assert store.get_job(j.id, "a").params == {"format": "brief"}
    assert store.get_job(j.id, "b") is None
    assert store.get_job(j.id) is not None


def test_update_and_claim(store):
    j = store.create_job("a", "podcast", {}, ["u"], [], T0)
    assert store.claim_next(T0) is None  # "new" is not claimable
    store.update_job(j.id, T0, status="queued", source_ids=["s1"], accepted=True)
    c = store.claim_next(T0 + 1)
    assert c.id == j.id and c.status == "running" and c.source_ids == ["s1"] and c.accepted is True
    assert store.claim_next(T0 + 2) is None


def test_update_rejects_unknown_field(store):
    j = store.create_job("a", "podcast", {}, ["u"], [], T0)
    with pytest.raises(KeyError):
        store.update_job(j.id, T0, bogus=1)


def test_claim_order_is_fifo(store):
    a = store.create_job("a", "podcast", {}, ["u"], [], T0)
    b = store.create_job("a", "podcast", {}, ["u"], [], T0 + 1)
    for j in (b, a):
        store.update_job(j.id, T0, status="queued")
    assert store.claim_next(T0).id == a.id


def test_count_billable(store):
    store.create_job("a", "podcast", {}, ["u"], [], T0)
    failed_early = store.create_job("a", "podcast", {}, ["u"], [], T0)
    failed_late = store.create_job("a", "podcast", {}, ["u"], [], T0)
    store.create_job("b", "podcast", {}, ["u"], [], T0)
    store.create_job("a", "report", {}, ["u"], [], T0)
    store.create_job("a", "podcast", {}, ["u"], [], T0 - 90_000)
    store.update_job(failed_early.id, T0, status="failed")
    store.update_job(failed_late.id, T0, status="failed", accepted=True)
    assert store.count_billable("podcast", T0 - 86_400, "a") == 2  # ok + failed_late
    assert store.count_billable("podcast", T0 - 86_400) == 3


def test_uploads_claim_rules(store):
    u1 = store.add_upload("a", "x.pdf", "/tmp/u1", 10, T0, 7200)
    u2 = store.add_upload("b", "y.pdf", "/tmp/u2", 10, T0, 7200)
    u3 = store.add_upload("a", "z.pdf", "/tmp/u3", 10, T0 - 10_000, 7200)  # expired
    assert [u.id for u in store.claim_uploads([u1.id], "a", "job1", T0)] == [u1.id]
    for ids, who in (([u1.id], "a"), ([u2.id], "a"), ([u3.id], "a"), (["up_missing"], "a")):
        with pytest.raises(GatewayError) as e:
            store.claim_uploads(ids, who, "job2", T0)
        assert e.value.code == "bad_request"
    store.release_uploads("job1")
    assert store.get_upload(u1.id).job_id is None
    assert [u.id for u in store.expired_uploads(T0)] == [u3.id]


def test_claim_rejects_duplicate_ids(store):
    u = store.add_upload("a", "x.pdf", "/tmp/u1", 10, T0, 7200)
    with pytest.raises(GatewayError) as e:
        store.claim_uploads([u.id, u.id], "a", "job1", T0)
    assert e.value.code == "bad_request"
    assert store.get_upload(u.id).job_id is None


def test_claim_is_all_or_nothing(store):
    good = store.add_upload("a", "x.pdf", "/tmp/u1", 10, T0, 7200)
    with pytest.raises(GatewayError):
        store.claim_uploads([good.id, "up_missing"], "a", "job1", T0)
    assert store.get_upload(good.id).job_id is None


def test_notebook_cache(store):
    assert store.get_notebook("r · 2026-W41") is None
    store.put_notebook("r · 2026-W41", "nb1", "a", 3, T0)
    store.adjust_sources("r · 2026-W41", 2)
    assert store.get_notebook("r · 2026-W41") == ("nb1", 5)
    store.forget_notebook("r · 2026-W41")
    assert store.get_notebook("r · 2026-W41") is None


def test_existing_db_without_title_column_is_migrated(tmp_path):
    import sqlite3
    from nlm_gateway.store import Store
    path = tmp_path / "old.db"
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE jobs (
      id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, kind TEXT NOT NULL, params TEXT NOT NULL,
      status TEXT NOT NULL, error_code TEXT, error_message TEXT,
      notebook_id TEXT, notebook_title TEXT, notebook_url TEXT,
      urls TEXT NOT NULL DEFAULT '[]', upload_ids TEXT NOT NULL DEFAULT '[]', source_ids TEXT NOT NULL DEFAULT '[]',
      planned_sources INTEGER NOT NULL DEFAULT 0,
      task_id TEXT, artifact_id TEXT, accepted INTEGER NOT NULL DEFAULT 0,
      size_bytes INTEGER, mime TEXT, text TEXT,
      rollback_pending INTEGER NOT NULL DEFAULT 0, generation_requested INTEGER NOT NULL DEFAULT 0,
      rollback_attempts INTEGER NOT NULL DEFAULT 0,
      created_at REAL NOT NULL, updated_at REAL NOT NULL, finished_at REAL);
    INSERT INTO jobs (id, consumer_id, kind, params, status, created_at, updated_at)
      VALUES ('old', 'a', 'report', '{}', 'done', 1, 1);""")
    db.commit()
    db.close()
    s = Store(path)
    assert s.get_job("old").title is None
    s.update_job("old", T0, title="Lighthouses")
    assert s.get_job("old").title == "Lighthouses"
    Store(path)  # reopening an already-migrated db is a no-op


def test_existing_db_without_failed_indices_is_migrated(tmp_path):
    import sqlite3
    from nlm_gateway.store import Store
    path = tmp_path / "old.db"
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE jobs (
      id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, kind TEXT NOT NULL, params TEXT NOT NULL,
      status TEXT NOT NULL, error_code TEXT, error_message TEXT,
      notebook_id TEXT, notebook_title TEXT, notebook_url TEXT,
      urls TEXT NOT NULL DEFAULT '[]', upload_ids TEXT NOT NULL DEFAULT '[]', source_ids TEXT NOT NULL DEFAULT '[]',
      planned_sources INTEGER NOT NULL DEFAULT 0,
      task_id TEXT, artifact_id TEXT, accepted INTEGER NOT NULL DEFAULT 0,
      size_bytes INTEGER, mime TEXT, text TEXT,
      rollback_pending INTEGER NOT NULL DEFAULT 0, generation_requested INTEGER NOT NULL DEFAULT 0,
      rollback_attempts INTEGER NOT NULL DEFAULT 0, title TEXT,
      created_at REAL NOT NULL, updated_at REAL NOT NULL, finished_at REAL);
    INSERT INTO jobs (id, consumer_id, kind, params, status, created_at, updated_at)
      VALUES ('old', 'a', 'report', '{}', 'done', 1, 1);""")
    db.commit()
    db.close()
    s = Store(path)
    assert s.get_job("old").failed_indices == []
    s.update_job("old", T0, failed_indices=[1, 3])
    assert s.get_job("old").failed_indices == [1, 3]
    Store(path)  # reopening an already-migrated db is a no-op
