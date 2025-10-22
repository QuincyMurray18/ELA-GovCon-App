# x16_docx_fix.py — makes python-docx.Document available globally and guards if missing.
# Usage: add `import x16_docx_fix` near the top of app.py.
import builtins

try:
    from docx import Document  # requires `python-docx` in requirements.txt
    try:
        from docx.shared import Pt as _DocxPt  # optional
        builtins.DocxPt = _DocxPt
    except Exception:
        pass
    builtins.Document = Document
except Exception:
    # Provide a callable stub that raises a clear error instead of NameError
    def _docx_missing(*args, **kwargs):
        raise RuntimeError("DOCX export requires the 'python-docx' package. Add `python-docx` to requirements.txt and redeploy.")
    builtins.Document = lambda *a, **k: _docx_missing()
