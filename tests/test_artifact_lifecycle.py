"""Exercise archive failure/recovery and the seven independently packaged bots."""

import csv
import importlib
import io
import json
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
BOTS = ("solobot", "trendobot", "fibobot", "firebot", "haemabot", "titanbot", "meanbot")
PREFIXES = ("common", "logger", "oms", "utils")
IST = ZoneInfo("Asia/Kolkata")


def purge_modules():
    for name in tuple(sys.modules):
        if any(name == prefix or name.startswith(prefix + ".") for prefix in PREFIXES):
            sys.modules.pop(name, None)


@pytest.fixture(params=BOTS)
def bot(request, tmp_path, monkeypatch):
    purge_modules()
    monkeypatch.syspath_prepend(str(ROOT / "bots" / request.param))
    monkeypatch.setenv(f"{request.param.upper()}_FILES_DIR", str(tmp_path / "files"))
    monkeypatch.setenv(f"{request.param.upper()}_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv(f"{request.param.upper()}_CURR_DATE", "18-09-2026")
    monkeypatch.setenv("ARTIFACT_CLEANUP_ENABLED", "true")
    result = SimpleNamespace(
        name=request.param,
        artifacts=importlib.import_module("common.artifacts"),
        constants=importlib.import_module("common.constants"),
        live=importlib.import_module("oms.order_system_client"),
        mock=importlib.import_module("oms.mock_order_system_client"),
        upload=importlib.import_module("utils.s3_upload_utils"),
    )
    yield result
    purge_modules()


def write_orders(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "status", "timestamp", "pnl"])
        writer.writeheader()
        writer.writerows(rows)


class FakeS3:
    def __init__(self, fail_at=None, corrupt=False, callback=None):
        self.objects = {}
        self.uploads = []
        self.fail_at = fail_at
        self.corrupt = corrupt
        self.callback = callback

    def upload_file(self, filename, bucket, key):
        self.uploads.append(key)
        if len(self.uploads) == self.fail_at:
            raise RuntimeError("upload failed")
        self.objects[key] = Path(filename).read_bytes()
        if self.callback:
            callback, self.callback = self.callback, None
            callback()

    def get_object(self, *, Bucket, Key):
        data = self.objects[Key]
        if self.corrupt and data:
            data = bytes([data[0] ^ 1]) + data[1:]
        return {"Body": io.BytesIO(data)}


def artifact_folder(bot):
    directory = Path(bot.constants.MOCK_FOLDER_PATH)
    write_orders(directory / "order_log.csv", [
        {"id": "mock-closed", "status": "MANUAL EXIT", "timestamp": "2026-09-18T10:00:00+05:30"},
    ])
    (directory / "order_event_log.json").write_text('{"events": []}')
    (directory / "nested").mkdir()
    (directory / "nested" / "report.txt").write_text("all daily artifacts are archived")
    return directory


def test_verified_batch_cleanup_preserves_accounting_and_other_modes(bot):
    directory = artifact_folder(bot)
    accounting = Path(bot.constants.DAILY_MOCK_PNL)
    accounting.parent.mkdir(parents=True)
    accounting.write_text("date,daily_pnl\n2026-09-17,42\n")
    other = Path(bot.constants.ORDER_PROD_LOG)
    other.parent.mkdir(parents=True)
    other.write_text("untouched")
    s3 = FakeS3()
    assert bot.artifacts.archive_directory(s3, "bucket", "bot/180926/mock", directory,
                                          extras={"accounting/daily_pnl.csv": accounting})
    assert not directory.exists()
    assert accounting.read_text().endswith("42\n")
    assert other.read_text() == "untouched"
    manifest = json.loads(s3.objects[s3.uploads[-1]])
    assert s3.uploads[-1].endswith("/manifest.json")
    assert set(manifest) == {
        "execution_results/order_log.csv", "execution_results/order_event_log.json",
        "execution_results/nested/report.txt", "accounting/daily_pnl.csv",
    }


@pytest.mark.parametrize("failure", ["partial_upload", "daily_path_upload", "manifest_upload", "checksum"])
def test_upload_failure_retains_entire_batch_and_retry_works(bot, failure):
    directory = artifact_folder(bot)
    before = {p.relative_to(directory): p.read_bytes() for p in directory.rglob("*") if p.is_file()}
    s3 = FakeS3(fail_at={"partial_upload": 2, "daily_path_upload": 4, "manifest_upload": 7}.get(failure),
                corrupt=failure == "checksum")
    with pytest.raises(RuntimeError):
        bot.artifacts.archive_directory(s3, "bucket", "archive", directory)
    assert before == {p.relative_to(directory): p.read_bytes() for p in directory.rglob("*") if p.is_file()}
    s3.fail_at = None
    s3.corrupt = False
    assert bot.artifacts.archive_directory(s3, "bucket", "archive", directory)
    assert not directory.exists()


@pytest.mark.parametrize("status", ["OPEN", "PLACED", "ENTRY_PLACED", "", "UNKNOWN"])
def test_unresolved_trades_archived_but_never_deleted(bot, status):
    directory = artifact_folder(bot)
    write_orders(directory / "order_log.csv", [{"id": "mock-old", "status": status}])
    s3 = FakeS3()
    assert not bot.artifacts.archive_directory(s3, "bucket", "archive", directory)
    assert (directory / "order_log.csv").exists()
    assert s3.uploads[-1].endswith("/manifest.json")


@pytest.mark.parametrize("mutation", ["modify", "new_file"])
def test_files_changed_during_upload_are_retained(bot, mutation):
    directory = artifact_folder(bot)
    target = directory / ("order_event_log.json" if mutation == "modify" else "new.txt")
    s3 = FakeS3(callback=lambda: target.write_text("new data"))
    with pytest.raises(RuntimeError, match="changed during upload"):
        bot.artifacts.archive_directory(s3, "bucket", "archive", directory)
    assert (directory / "order_log.csv").exists()
    assert target.read_text() == "new data"


def test_symlinks_never_delete_external_files(bot, tmp_path):
    directory = artifact_folder(bot)
    outside = tmp_path / "outside.txt"
    outside.write_text("keep")
    (directory / "link").symlink_to(outside)
    with pytest.raises(ValueError, match="symlinks"):
        bot.artifacts.archive_directory(FakeS3(), "bucket", "archive", directory)
    assert outside.read_text() == "keep"


def test_cleanup_requires_after_market_close_and_enabled_flag(bot, monkeypatch):
    assert not bot.artifacts.cleanup_allowed(datetime(2026, 9, 18, 15, 30, tzinfo=IST))
    assert bot.artifacts.cleanup_allowed(datetime(2026, 9, 18, 15, 31, tzinfo=IST))
    monkeypatch.setenv("ARTIFACT_CLEANUP_ENABLED", "false")
    assert not bot.artifacts.cleanup_allowed(datetime(2026, 9, 18, 16, 0, tzinfo=IST))


def test_legacy_migration_preserves_today_and_cumulative_pnl(bot):
    current = Path(bot.constants.ORDER_MOCK_LOG)
    legacy = current.parent.parent / "order_log.csv"
    write_orders(legacy, [
        {"id": "mock-stale", "status": "OPEN", "timestamp": "2026-09-07T14:55:00+05:30"},
        {"id": "mock-today", "status": "OPEN", "timestamp": "2026-09-18T10:00:00+05:30"},
        {"id": "mock-unknown", "status": "OPEN", "timestamp": ""},
    ])
    old_pnl = legacy.with_name("daily_pnl.csv")
    old_pnl.write_text("date,daily_pnl\n2026-09-07,123\n")
    original = legacy.read_bytes()
    paths = bot.live.initialize_local_ledgers_for_modes(["mock"])["mock"]
    assert current.parent.name == "2026-09-18"
    with current.open() as handle:
        assert [row["id"] for row in csv.DictReader(handle)] == ["mock-today"]
    assert legacy.read_bytes() == original
    assert Path(paths["daily_csv"]).read_bytes() == old_pnl.read_bytes()
    assert "state/mock" in paths["daily_csv"]
    current.write_text("do not overwrite")
    Path(paths["daily_csv"]).write_text("new accounting")
    bot.live.initialize_local_ledgers_for_modes(["mock"])
    assert current.read_text() == "do not overwrite"
    assert Path(paths["daily_csv"]).read_text() == "new accounting"


def test_mock_restore_filters_dates_without_modifying_old_rows(bot, tmp_path):
    orders = tmp_path / "custom.csv"
    write_orders(orders, [
        {"id": "old", "status": "OPEN", "timestamp": "2026-09-07T14:55:00+05:30"},
        {"id": "today", "status": "OPEN", "timestamp": "2026-09-18T10:00:00+05:30"},
        {"id": "utc", "status": "OPEN", "timestamp": "2026-09-17T20:00:00Z"},
        {"id": "naive", "status": "OPEN", "timestamp": "2026-09-18T00:01:00"},
        {"id": "unknown", "status": "OPEN", "timestamp": "garbage"},
        {"id": "old-closed", "status": "MANUAL EXIT", "timestamp": "2026-09-07T10:00:00", "pnl": "1000"},
        {"id": "today-closed", "status": "MANUAL EXIT", "timestamp": "2026-09-18T10:00:00", "pnl": "20"},
    ])
    original = orders.read_bytes()
    client = bot.mock.MockOrderSystemClient(mode="mock", curr_date="18-09-2026", orders_csv=str(orders))
    account = client._local_account_response()
    assert {row["id"] for row in account["trades"]} == {"today", "utc", "naive", "today-closed"}
    assert account["net_profit"] == 20
    assert orders.read_bytes() == original


def test_account_date_selects_matching_default_ledger(bot):
    client = bot.mock.MockOrderSystemClient(mode="mock", curr_date="17-09-2026", local_copy_enabled=False)
    assert Path(client.orders_csv).parent.name == "2026-09-17"
    assert Path(client.events_json_path).parent.name == "2026-09-17"


def test_runtime_and_archiver_cannot_share_active_files(bot, tmp_path):
    with bot.artifacts.artifact_run_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another bot/archiver"):
            with bot.artifacts.artifact_run_lock(tmp_path):
                pytest.fail("second writer entered")
    with bot.artifacts.artifact_run_lock(tmp_path):
        pass


def setup_uploader(bot, monkeypatch, s3, hour=16):
    boto3 = ModuleType("boto3")
    s3.client_calls = []
    def client(*args, **kwargs):
        s3.client_calls.append((args, kwargs))
        return s3
    boto3.client = client
    config = ModuleType("botocore.config")
    config.Config = lambda **kw: kw
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    monkeypatch.setitem(sys.modules, "botocore", ModuleType("botocore"))
    monkeypatch.setitem(sys.modules, "botocore.config", config)
    for name in ("CLOUDPE_S3_ENDPOINT_URL", "CLOUDPE_S3_REGION", "CLOUDPE_S3_ACCESS_KEY_ID", "CLOUDPE_S3_SECRET_ACCESS_KEY", "CLOUDPE_S3_BUCKET_NAME"):
        monkeypatch.setenv(name, "test")
    monkeypatch.setenv("CLOUDPE_S3_ENDPOINT_URL", "https://s3.in-west2.purestore.io")
    monkeypatch.setenv("CLOUDPE_S3_REGION", "in-west2")
    monkeypatch.delenv("CLOUDPE_S3_PREFIX", raising=False)
    monkeypatch.setattr(bot.upload, "datetime", SimpleNamespace(now=lambda tz: datetime(2026, 9, 18, hour, 0, tzinfo=IST)))


def test_uploader_retries_prior_days_and_preserves_legacy_open_trades(bot, monkeypatch):
    directory = artifact_folder(bot)
    previous = directory.parent / "2026-09-17"
    write_orders(previous / "order_log.csv", [{"id": "closed", "status": "EOD_SQUARE_OFF"}])
    legacy = directory.parent / "order_log.csv"
    write_orders(legacy, [{"id": "stale", "status": "OPEN", "timestamp": "2026-09-03T10:00:00"}])
    accounting = Path(bot.constants.DAILY_MOCK_PNL)
    accounting.parent.mkdir(parents=True)
    accounting.write_text("date,daily_pnl\n2026-09-17,42\n")
    future = directory.parent / "2026-09-19"
    write_orders(future / "order_log.csv", [{"id": "future", "status": "MANUAL EXIT"}])
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.setattr(bot.upload, "_custom_artifact_sources", lambda mode: {})
    bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert not directory.exists()
    assert not previous.exists()
    assert legacy.exists()
    assert future.exists()
    assert accounting.exists()
    assert any("/20261709/mock/" in key for key in s3.objects)
    assert any("/20261809/mock/" in key for key in s3.objects)
    assert any("/legacy/mock/" in key for key in s3.objects)


@pytest.mark.parametrize("managed_directory", [True, False])
def test_custom_paths_uploaded_and_retained(bot, monkeypatch, tmp_path, managed_directory):
    directory = artifact_folder(bot) if managed_directory else None
    custom = tmp_path / "custom.csv"
    write_orders(custom, [{"id": "custom", "status": "MANUAL EXIT"}])
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.setenv("OMS_ORDERS_CSV", str(custom))
    bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert custom.exists()
    if directory:
        assert directory.exists()
    assert any(key.endswith("/custom/order_log.csv") for key in s3.objects)


def test_intraday_upload_does_not_delete_restart_state(bot, monkeypatch):
    directory = artifact_folder(bot)
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3, hour=10)
    monkeypatch.setattr(bot.upload, "_custom_artifact_sources", lambda mode: {})
    bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert (directory / "order_log.csv").exists()
    assert s3.uploads[-1].endswith("/manifest.json")


def test_closed_legacy_files_cleanup_preserves_migrated_accounting(bot, monkeypatch):
    mode_dir = Path(bot.constants.ORDER_MOCK_LOG).parent.parent
    write_orders(mode_dir / "order_log.csv", [
        {"id": "closed-old", "status": "MANUAL EXIT", "timestamp": "2026-09-07T10:00:00"},
    ])
    (mode_dir / "daily_pnl.csv").write_text("date,daily_pnl\n2026-09-07,123\n")
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.setattr(bot.upload, "_custom_artifact_sources", lambda mode: {})
    bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert list(mode_dir.iterdir()) == []
    assert Path(bot.constants.DAILY_MOCK_PNL).read_text().endswith("123\n")
    assert any("/legacy/mock/" in key for key in s3.objects)


def test_live_oms_response_is_not_filtered_by_local_mock_rule(bot):
    client = bot.live.OrderSystemClient(mode="production", local_copy_enabled=False)
    old_trade = {"id": "live-trade", "status": "OPEN", "timestamp": "2026-09-07T10:00:00+05:30"}
    client._request = lambda *a, **kw: {"trades": [old_trade]}
    assert client.get_account_details()["trades"] == [old_trade]


@pytest.mark.parametrize("mode", ["mock", "sandbox", "production"])
@pytest.mark.parametrize("prefix", [None, "index-bucket-holder/trades", "/index-bucket/trades/"])
def test_cloudpe_destination_and_exact_daily_order_keys(bot, monkeypatch, mode, prefix):
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.setenv("CLOUDPE_S3_BUCKET_NAME", "index-bucket")
    monkeypatch.setenv("CLOUDPE_S3_ENDPOINT_URL", "https://index-bucket.s3.in-west2.purestore.io")
    # DigitalOcean credentials/settings must never override CloudPE configuration.
    monkeypatch.setenv("DO_S3_ENDPOINT_URL", "https://sgp1.digitaloceanspaces.com")
    if prefix is not None:
        monkeypatch.setenv("CLOUDPE_S3_PREFIX", prefix)
    monkeypatch.setattr(bot.upload, "datetime", SimpleNamespace(
        now=lambda tz: datetime(2026, 9, 28, 16, 0, tzinfo=IST)))
    monkeypatch.setattr(bot.upload, "_custom_artifact_sources", lambda mode: {})
    ledger, _ = bot.upload._order_sources_for_mode(mode)
    directory = ledger.parent.parent / "2026-09-28"
    write_orders(directory / "order_log.csv", [{"id": "closed", "status": "MANUAL EXIT"}])
    events = b'{"events": [{"id": "closed"}]}'
    (directory / "order_event_log.json").write_bytes(events)
    orders = (directory / "order_log.csv").read_bytes()

    bot.upload.upload_trade_artifacts_to_s3(bot.name, mode)

    root = f"trades/{bot.name}/20262809/{mode}"
    assert s3.objects[f"{root}/orders/order_events.json"] == events
    assert s3.objects[f"{root}/orders/order_log.csv"] == orders
    assert all(key.startswith(root + "/") for key in s3.objects)
    assert any("/snapshots/" in key for key in s3.objects)
    assert not directory.exists()
    [(args, kwargs)] = s3.client_calls
    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "https://s3.in-west2.purestore.io"
    assert kwargs["region_name"] == "in-west2"
    assert kwargs["aws_access_key_id"] == "test"
    assert kwargs["aws_secret_access_key"] == "test"
    assert kwargs["config"] == {"s3": {"addressing_style": "path"}}


@pytest.mark.parametrize("endpoint", [
    "https://sgp1.digitaloceanspaces.com",
    "index-bucket.sgp1.digitaloceanspaces.com",
    "https://SGP1.DIGITALOCEANSPACES.COM./",
])
def test_digitalocean_endpoint_rejected_before_upload_or_cleanup(bot, monkeypatch, endpoint):
    directory = artifact_folder(bot)
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.setenv("CLOUDPE_S3_ENDPOINT_URL", endpoint)
    with pytest.raises(ValueError, match="points to DigitalOcean"):
        bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert not s3.client_calls
    assert not s3.uploads
    assert (directory / "order_event_log.json").exists()


def test_missing_cloudpe_credentials_do_not_fall_back_to_digitalocean(bot, monkeypatch):
    directory = artifact_folder(bot)
    s3 = FakeS3()
    setup_uploader(bot, monkeypatch, s3)
    monkeypatch.delenv("CLOUDPE_S3_ACCESS_KEY_ID")
    monkeypatch.delenv("CLOUDPE_S3_SECRET_ACCESS_KEY")
    monkeypatch.setenv("DO_S3_ACCESS_KEY_ID", "old-key")
    monkeypatch.setenv("DO_S3_SECRET_ACCESS_KEY", "old-secret")
    with pytest.raises(RuntimeError, match="Missing S3 configuration: CLOUDPE_S3_ACCESS_KEY_ID, CLOUDPE_S3_SECRET_ACCESS_KEY"):
        bot.upload.upload_trade_artifacts_to_s3(bot.name, "mock")
    assert not s3.client_calls
    assert (directory / "order_event_log.json").exists()
