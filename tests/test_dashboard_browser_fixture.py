"""Source-bound structural tests; rendered Chrome execution is a separate job."""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "dashboard_browser_fixture.py"
spec = spec_from_file_location("industrial_dashboard_browser_fixture", SCRIPT)
module = module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def test_fixture_extracts_original_dashboard_literal():
    tree = ast.parse(module.SOURCE.read_text(encoding="utf-8"))
    value = next(n.value.value for n in tree.body if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == "PAGE" for t in n.targets))
    rendered = module.dashboard_html()
    marker = rendered.index('<output id="ci-render-probe"')
    assert value == rendered[:marker] + rendered[rendered.index("</main></html>", marker):]
    assert '<output id="ci-render-probe" hidden>' in rendered


def test_browser_fixture_is_explicitly_synthetic_and_contains_untrusted_text():
    fixture = module.snapshot()
    assert fixture["counts"]["raw"] > 0
    assert len(fixture["alerts"]) == len(fixture["predictions"]) == 1
    assert "<img src=x onerror=" in fixture["alerts"][0]["site_id"]


def test_browser_harness_reads_production_source_only():
    assert module.SOURCE == ROOT / "src" / "industrial_mlops" / "dashboard.py"
    assert not hasattr(module, "write_original")
