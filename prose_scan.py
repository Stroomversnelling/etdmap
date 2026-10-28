"""Report comment and docstring lines holding a raw data-model id name."""

import ast
import sys
from pathlib import Path

RAW = (
    "HuisIdBSV", "HuisBatchIdBSV", "ProjectIdBSV", "BatchIdBSV",
    "HuisIdLeverancier", "ProjectIdLeverancier", "Dataleverancier",
)

HISTORY = (
    "used to", "previously", "an earlier version", "old behaviour",
    "before this fix", "Action 1", "Plan group", "TODO-",
)

SKIP = {"ids.py", "testing.py", "prose_scan.py"}


def docstring_lines(tree):
    lines = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            lines.update(range(first.lineno, first.end_lineno + 1))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            lines.update(range(node.lineno, node.end_lineno + 1))
    return lines


def main():
    root = Path(__file__).parent / "etdmap"
    mode = sys.argv[1] if len(sys.argv) > 1 else "raw"
    total = 0
    for path in sorted(root.glob("*.py")):
        if path.name in SKIP:
            continue
        source = path.read_text(encoding="utf-8")
        prose = docstring_lines(ast.parse(source))
        for number, line in enumerate(source.split("\n"), start=1):
            is_prose = number in prose or line.strip().startswith("#")
            if not is_prose:
                continue
            needles = RAW if mode == "raw" else HISTORY
            hits = [n for n in needles if n.lower() in line.lower()]
            if hits:
                total += 1
                print(f"{path.name}:{number}: {line.strip()}")
    print(f"TOTAL LINES: {total}")


main()
