"""import akayu must not claim generic top-level module names.

Its embedded helper module used to be registered as sys.modules['helpers'],
so after `import akayu` a user's own helpers.py was silently shadowed.
"""

import subprocess
import sys


def _run(code, cwd):
    result = subprocess.run([sys.executable, "-c", code], cwd=cwd, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_user_helpers_module_is_not_shadowed(tmp_path):
    (tmp_path / "helpers.py").write_text("def greet():\n    return 'user helpers'\n")
    assert _run("import akayu, helpers; print(helpers.greet())", tmp_path) == "user helpers"


def test_import_adds_no_foreign_top_level_modules(tmp_path):
    code = (
        "import sys; before = set(sys.modules); import akayu; "
        "added = {m.split('.')[0] for m in set(sys.modules) - before}; "
        "print(sorted(added - set(sys.stdlib_module_names)))"
    )
    assert _run(code, tmp_path) == "['akayu']"


def test_helpers_live_under_the_akayu_package():
    import akayu._helpers as helpers

    assert callable(helpers._analyze_lambda)
