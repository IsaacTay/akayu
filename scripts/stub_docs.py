"""Copy akayu's runtime docstrings into its type stub, or check that they match.

The API is documented once, where the code is: Rust `///` comments for the
extension (`help()` shows them) and docstrings in `python/akayu/__init__.py`
for the Python parts. The stub (`python/akayu/__init__.pyi`), which editors
and the API reference (docs/api.md, through mkdocstrings) read, carries
copies, made by this script:

    python scripts/stub_docs.py          # rewrite the stub's docstrings
    python scripts/stub_docs.py --check  # exit 1 if any differs

Every docstring already in the stub is replaced by its object's runtime
docstring; the stub decides which definitions carry one (the last of a set of
overloads, say). Slot methods whose runtime doc is CPython's own (`__repr__`,
`__new__` of an extension type) keep the stub's docstring: there it is the
only copy.
"""

import argparse
import ast
import inspect
import pathlib
import sys

import akayu

ROOT = pathlib.Path(__file__).resolve().parent.parent
STUB = ROOT / "python" / "akayu" / "__init__.pyi"


def runtime_doc(module, qualname):
    """The runtime docstring of `qualname`, or None if it isn't akayu's own."""
    obj = module
    for part in qualname.split("."):
        # Static on classes: a property's own object, not its value.
        obj = inspect.getattr_static(obj, part, None) if isinstance(obj, type) else getattr(obj, part, None)
        if obj is None:
            return None
    if isinstance(obj, (staticmethod, classmethod)):
        obj = obj.__func__
    doc = getattr(obj, "__doc__", None)
    base = getattr(object, qualname.rsplit(".", 1)[-1], None)
    if not doc or (base is not None and doc == getattr(base, "__doc__", None)):
        return None
    return inspect.cleandoc(doc)


def docstring_literal(doc, indent):
    """`doc` as the source lines of a docstring indented by `indent`."""
    prefix = 'r"""' if "\\" in doc else '"""'
    lines = doc.splitlines()
    if len(lines) == 1:
        return [f'{indent}{prefix}{lines[0]}"""']
    out = [f"{indent}{prefix}{lines[0]}"]
    out += [indent + line if line else "" for line in lines[1:]]
    out.append(f'{indent}"""')
    return out


def stub_docstrings(tree):
    """(qualname, docstring node) for every docstring in the stub."""
    found = []

    def walk(body, prefix):
        for node in body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                first = node.body[0] if node.body else None
                if (
                    isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)
                ):
                    found.append((prefix + node.name, first))
                if isinstance(node, ast.ClassDef):
                    walk(node.body, prefix + node.name + ".")

    walk(tree.body, "")
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the stub's docstrings differ")
    args = parser.parse_args()

    source = STUB.read_text(encoding="utf-8")
    lines = source.splitlines()
    differ = []
    for qualname, node in reversed(stub_docstrings(ast.parse(source))):
        doc = runtime_doc(akayu, qualname)
        if doc is None or inspect.cleandoc(node.value.value) == doc:
            continue
        differ.append(qualname)
        indent = " " * node.col_offset
        lines[node.lineno - 1 : node.end_lineno] = docstring_literal(doc, indent)
    if args.check:
        if differ:
            print(
                "The stub's docstrings differ from the runtime's for:\n  "
                + "\n  ".join(sorted(differ))
                + "\nRun `just stub-docs` (python scripts/stub_docs.py) to copy them.",
                file=sys.stderr,
            )
            return 1
        return 0
    if differ:
        STUB.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"updated {len(differ)} docstrings in {STUB.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
