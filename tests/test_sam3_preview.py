"""Regression tests for the standalone SAM3 preview script."""

import argparse
import importlib.util
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from types import ModuleType

import pytest


def _load_preview_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load the script without installing its optional image-rendering stack."""
    numpy = ModuleType("numpy")
    numpy_typing = ModuleType("numpy.typing")
    setattr(numpy, "array", lambda *_args, **_kwargs: None)
    setattr(numpy_typing, "NDArray", list)
    setattr(numpy, "typing", numpy_typing)

    class Image:
        pass

    pil = ModuleType("PIL")
    pil_image = ModuleType("PIL.Image")
    setattr(pil_image, "Image", Image)
    setattr(pil_image, "fromarray", lambda _array: None)
    setattr(pil_image, "open", lambda _source: None)
    setattr(pil, "Image", pil_image)

    for name, module in {
        "numpy": numpy,
        "numpy.typing": numpy_typing,
        "PIL": pil,
        "PIL.Image": pil_image,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    path = Path(__file__).parents[1] / "scripts" / "sam3_preview.py"
    spec = importlib.util.spec_from_file_location("sam3_preview_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_batch_preview_skips_invalid_json_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    preview = _load_preview_module(monkeypatch)
    input_tar = tmp_path / "input.tar"
    result_tar = tmp_path / "result.tar"
    with tarfile.open(input_tar, "w"):
        pass
    with tarfile.open(result_tar, "w") as archive:
        for name, content in [
            ("broken.json", b"{not json"),
            ("empty.json", b'{"num_objects": 0}'),
        ]:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, BytesIO(content))

    args = argparse.Namespace(input_tar=input_tar, result_tar=result_tar)
    preview.preview_batch(args)

    output = capsys.readouterr().out
    assert "Skipping broken.json: invalid JSON" in output
    assert "Skipping empty.json: no objects detected." in output
