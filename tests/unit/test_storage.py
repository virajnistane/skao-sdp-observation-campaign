#!/usr/bin/env python
# Author: Viraj Nistane
# Description: This file contains unit tests for the storage module.

from pathlib import Path

import pytest

from sdp_control.tasks.storage import get_total_ms_size_mb, storage_full


def _make_dir_with_size(base: Path, name: str, size_bytes: int) -> None:
    d = base / name
    d.mkdir()
    (d / "data.bin").write_bytes(b"0" * size_bytes)


def test_get_total_ms_size_mb_ms_only(tmp_path):
    _make_dir_with_size(tmp_path, "obs_000_raw_x.ms", 1024 * 1024)  # 1 MB, counted
    _make_dir_with_size(tmp_path, "obs_000_processed_x", 1024 * 1024)  # 1 MB, not .ms

    total = get_total_ms_size_mb(str(tmp_path), 0.0, storage_count_scope="ms_only")
    assert total == pytest.approx(1.0, rel=1e-3)


def test_get_total_ms_size_mb_all(tmp_path):
    _make_dir_with_size(tmp_path, "obs_000_raw_x.ms", 1024 * 1024)
    _make_dir_with_size(tmp_path, "obs_000_processed_x", 1024 * 1024)

    total = get_total_ms_size_mb(str(tmp_path), 0.0, storage_count_scope="all")
    assert total == pytest.approx(2.0, rel=1e-3)


def test_get_total_ms_size_mb_invalid_scope(tmp_path):
    with pytest.raises(ValueError):
        get_total_ms_size_mb(str(tmp_path), 0.0, storage_count_scope="bogus")


def test_storage_full():
    assert storage_full(current_total_size_mb=100, storage_threshold_mb=50) is True
    assert storage_full(current_total_size_mb=10, storage_threshold_mb=50) is False
    assert storage_full(current_total_size_mb=50, storage_threshold_mb=50) is False  # boundary: strictly greater-than
