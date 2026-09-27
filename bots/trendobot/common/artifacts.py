"""Daily artifact storage and verified archival; kept identical across bot images."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import io
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from logger import create_logger

IST = ZoneInfo("Asia/Kolkata")
log = create_logger("ArtifactLifecycleLogger")
CLOSED_STATUSES = {"TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT", "EOD_SQUARE_OFF", "KILL_SWITCH"}


def execution_day(bot_name: str) -> str:
    configured = os.getenv(f"{bot_name.upper()}_CURR_DATE", "").strip()
    if configured:
        for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(configured, fmt).date().isoformat()
            except ValueError:
                pass
        raise ValueError(f"Invalid {bot_name.upper()}_CURR_DATE: {configured!r}")
    return datetime.now(IST).date().isoformat()

def trade_day(row: dict) -> str | None:
    raw = str(row.get("timestamp") or row.get("entry_time") or "").strip()
    try:
        timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=IST)
        return timestamp.astimezone(IST).date().isoformat()
    except ValueError:
        return None


@contextmanager
def artifact_run_lock(files_dir):
    """Prevent another bot run or offline archiver from changing these files."""
    root = Path(files_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".artifact-run.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"Another bot/archiver is using {root}") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _create_once(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
        try:
            os.link(temporary, path)  # Atomic creation, never replace an existing ledger.
        except FileExistsError:
            pass
    finally:
        os.unlink(temporary)


def migrate_legacy_ledgers(orders_csv: str, daily_csv: str) -> None:
    """Copy same-day recovery rows and cumulative P&L; retain legacy originals."""
    orders = Path(orders_csv)
    try:
        day = date.fromisoformat(orders.parent.name).isoformat()
    except ValueError:
        return  # An explicitly configured, undated path remains user managed.
    mode_dir = orders.parent.parent
    if mode_dir.name not in {"mock", "sandbox", "prod"} or mode_dir.parent.name != "execution_results":
        return
    legacy = mode_dir / "order_log.csv"
    if not orders.exists() and legacy.is_file():
        with legacy.open(newline="") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            rows = list(reader)
        if not fields or "status" not in fields:
            raise ValueError(f"Invalid legacy order ledger: {legacy}")
        current = [row for row in rows if trade_day(row) == day]
        stale_open = [row for row in rows if trade_day(row) != day and str(row.get("status") or "").strip().upper() == "OPEN"]
        if stale_open:
            log.warning("Retaining %s stale OPEN trades in %s; excluded from today's ledger", len(stale_open), legacy)
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(current)
        _create_once(orders, output.getvalue().encode())
    old_pnl = mode_dir / "daily_pnl.csv"
    new_pnl = Path(daily_csv)
    if old_pnl.is_file() and not new_pnl.exists():
        _create_once(new_pnl, old_pnl.read_bytes())


def _digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def _files(directory: Path, recursive: bool) -> list[Path]:
    entries = list(directory.rglob("*") if recursive else directory.iterdir())
    if directory.is_symlink() or any(path.is_symlink() for path in entries):
        raise ValueError(f"Refusing to archive symlinks: {directory}")
    return sorted(path for path in entries if path.is_file())


def _verify_remote(s3, bucket: str, key: str, size: int, checksum: str) -> None:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    actual = hashlib.sha256()
    count = 0
    try:
        for chunk in iter(lambda: body.read(1024 * 1024), b""):
            count += len(chunk)
            actual.update(chunk)
    finally:
        body.close()
    if count != size or actual.hexdigest() != checksum:
        raise RuntimeError(f"S3 verification failed: s3://{bucket}/{key}")


def archive_directory(s3, bucket: str, prefix: str, directory: Path, *,
                      extras: dict[str, Path] | None = None, cleanup: bool = True,
                      recursive: bool = True) -> bool:
    """Upload an immutable snapshot and manifest before deleting any source.

    Caller holds artifact_run_lock and only enables cleanup after market close.
    Extra files (accounting/logs) are archived but never deleted here.
    """
    sources = _files(directory, recursive)
    if not sources and not extras:
        return False
    with tempfile.TemporaryDirectory(prefix="bot-artifact-upload-") as temporary:
        snapshot = Path(temporary)
        manifest = {}
        originals = {}
        for source in sources:
            name = "execution_results/" + source.relative_to(directory).as_posix()
            originals[name] = source
        all_sources = {**originals, **(extras or {})}
        for name, source in sorted(all_sources.items()):
            destination = snapshot / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            manifest[name] = {"size": destination.stat().st_size, "sha256": _digest(destination)}

        # Unknown/blank statuses also retain recovery data. Never auto-close trades.
        unresolved = False
        for name in originals:
            if Path(name).name == "order_log.csv":
                with (snapshot / name).open(newline="") as handle:
                    reader = csv.DictReader(handle)
                    if not reader.fieldnames or "status" not in reader.fieldnames:
                        unresolved = True
                    else:
                        unresolved |= any(str(row.get("status") or "").strip().upper() not in CLOSED_STATUSES for row in reader)

        manifest_bytes = json.dumps(manifest, sort_keys=True, indent=2).encode()
        archive_id = hashlib.sha256(manifest_bytes).hexdigest()
        remote_root = f"{prefix.strip('/')}/snapshots/{archive_id}"
        for name, info in manifest.items():
            key = f"{remote_root}/{name}"
            s3.upload_file(str(snapshot / name), bucket, key)
            _verify_remote(s3, bucket, key, info["size"], info["sha256"])
        manifest_path = snapshot / "manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        key = f"{remote_root}/manifest.json"
        s3.upload_file(str(manifest_path), bucket, key)
        _verify_remote(s3, bucket, key, len(manifest_bytes), _digest(manifest_path))
        log.info("Verified archive s3://%s/%s", bucket, remote_root)

        if not cleanup or unresolved:
            log.info("Retaining %s: %s", directory, "unresolved trades" if unresolved else "cleanup disabled")
            return False
        # Check the entire batch before deleting any file, including newly added files.
        if _files(directory, recursive) != sources or any(
            _digest(source) != manifest[name]["sha256"] for name, source in originals.items()
        ):
            raise RuntimeError(f"Artifacts changed during upload; retained {directory}")
        for source in sources:
            source.unlink()
        if recursive:
            for child in sorted(directory.rglob("*"), key=lambda path: len(path.parts), reverse=True):
                if child.is_dir():
                    child.rmdir()
            directory.rmdir()
        log.info("Removed %s verified artifact files from %s", len(sources), directory)
        return True


def cleanup_allowed(now: datetime) -> bool:
    enabled = os.getenv("ARTIFACT_CLEANUP_ENABLED", "true").strip().lower() in {"1", "true", "yes"}
    return enabled and now.astimezone(IST).time() >= time(15, 31)
