"""ExcelLimpio PRO backend (v14)

Goal:
- Output ONE .xlsx with 2 sheets:
  1) ORIGINAL: each PDF page rendered as an image (fidelity: logos, layout)
  2) EDITABLE: tables/text extracted and laid out with column widths + row heights
     derived from PDF geometry (best-effort).

Notes:
- Works best on text-based PDFs with detectable table structure.
- For scanned PDFs without selectable text, table detection will be limited
  (OCR is intentionally not included to keep installs light on Render).
"""

from __future__ import annotations

import io
import os
import re
import statistics
from typing import List, Tuple, Optional

import pdfplumber
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.drawing.image import Image as XLImage
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.pagebreak import Break

# ------------------------
# App
# ------------------------

app = FastAPI(title="ExcelLimpio Backend v1", version="1.0.0")

# In PRO we typically allow CORS for the Netlify domain.
# For simplicity while you test, we allow all; you can lock it later.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "message": "ExcelLimpio backend activo"}


# ------------------------
# Geometry helpers
# ------------------------

THIN = Side(style="thin", color="999999")
BORDER_THIN = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
BASE_FONT = Font(name="Calibri", size=10)


def _cluster_vals(vals: List[float], tol: float = 2.0) -> List[float]:
    """Cluster numeric values within tol and return cluster means."""
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return []
    clusters: List[List[float]] = [[vals[0]]]
    for v in vals[1:]:
        if abs(v - statistics.mean(clusters[-1])) <= tol:
            clusters[-1].append(v)
        else:
            clusters.append([v])
    return [statistics.mean(c) for c in clusters]


def _nearest_index(val: float, lines: List[float]) -> int:
    return min(range(len(lines)), key=lambda i: abs(lines[i] - val))


def _set_column_widths(ws, col_pts: List[float], start_col: int = 1, total_excel_width: float = 150.0) -> None:
    """Set global column widths on the sheet, proportionally to PDF column widths."""
    total_pts = sum(col_pts) or 1.0
    for i, pts in enumerate(col_pts):
        w = (pts / total_pts) * total_excel_width
        ws.column_dimensions[get_column_letter(start_col + i)].width = max(2.5, round(w, 2))


def _set_row_heights(ws, row_pts: List[float], start_row: int, scale: float = 1.05) -> None:
    for i, pts in enumerate(row_pts):
        ws.row_dimensions[start_row + i].height = round(max(9.0, pts * scale), 2)


def _write_table(ws, grid: List[List[str]], merges: List[Tuple[int, int, int, int]], start_row: int, start_col: int = 1) -> None:
    for r, row in enumerate(grid):
        for c, val in enumerate(row):
            cell = ws.cell(row=start_row + r, column=start_col + c, value=val)
            cell.font = BASE_FONT
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = BORDER_THIN

    for r0, c0, r1, c1 in merges:
        ws.merge_cells(
            start_row=start_row + r0,
            start_column=start_col + c0,
            end_row=start_row + r1,
            end_column=start_col + c1,
        )


# ------------------------
# PDF extraction
# ------------------------

TableGrid = Tuple[List[List[str]], List[Tuple[int, int, int, int]], List[float], List[float]]


def _table_to_grid(page: pdfplumber.page.Page, table: pdfplumber.table.Table) -> TableGrid:
    """Rebuild a grid using table cell bboxes so we can match row heights/col widths."""
    cells = table.cells  # list of (x0, top, x1, bottom)

    xs: List[float] = []
    ys: List[float] = []
    for (x0, top, x1, bottom) in cells:
        xs.extend([x0, x1])
        ys.extend([top, bottom])

    x_lines = sorted(_cluster_vals(xs, tol=2.0))
    y_lines = sorted(_cluster_vals(ys, tol=2.0))

    ncols = max(0, len(x_lines) - 1)
    nrows = max(0, len(y_lines) - 1)

    grid = [["" for _ in range(ncols)] for _ in range(nrows)]
    merges: List[Tuple[int, int, int, int]] = []

    for (x0, top, x1, bottom) in cells:
        r0 = _nearest_index(top, y_lines)
        r1 = _nearest_index(bottom, y_lines)
        c0 = _nearest_index(x0, x_lines)
        c1 = _nearest_index(x1, x_lines)
        if r1 <= r0:
            r1 = r0 + 1
        if c1 <= c0:
            c1 = c0 + 1

        bbox = (x0, top, x1, bottom)
        txt = (page.within_bbox(bbox).extract_text() or "").strip()

        if 0 <= r0 < nrows and 0 <= c0 < ncols:
            if grid[r0][c0] and txt and txt not in grid[r0][c0]:
                grid[r0][c0] = (grid[r0][c0] + "\n" + txt).strip()
            else:
                grid[r0][c0] = txt

        if (r1 - r0) > 1 or (c1 - c0) > 1:
            merges.append((r0, c0, r1 - 1, c1 - 1))

    col_pts = [x_lines[i + 1] - x_lines[i] for i in range(ncols)]
    row_pts = [y_lines[i + 1] - y_lines[i] for i in range(nrows)]

    return grid, merges, col_pts, row_pts


def _extract_lines(words: List[dict], y_tol: float = 3.0) -> List[str]:
    """Group words into text lines by y (top) and return as strings."""
    if not words:
        return []

    # Cluster by y
    ys = [w["top"] for w in words]
    y_clusters = _cluster_vals(ys, tol=y_tol)

    # Assign each word to nearest cluster
    buckets = {i: [] for i in range(len(y_clusters))}
    for w in words:
        idx = _nearest_index(w["top"], y_clusters)
        buckets[idx].append(w)

    lines: List[Tuple[float, str]] = []
    for i, ws in buckets.items():
        ws_sorted = sorted(ws, key=lambda x: x["x0"])
        text = " ".join(w["text"] for w in ws_sorted).strip()
        if text:
            lines.append((y_clusters[i], text))

    # Sort top-to-bottom
    lines.sort(key=lambda x: x[0])
    return [t for _, t in lines]


def _pick_tables(page: pdfplumber.page.Page) -> List[pdfplumber.table.Table]:
    """Try to detect tables. Returns tables sorted top-to-bottom."""

    # Try default first
    tables = page.find_tables() or []

    if not tables:
        settings_lines = {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 3,
            "min_words_vertical": 2,
            "min_words_horizontal": 1,
            "text_tolerance": 3,
        }
        tables = page.find_tables(table_settings=settings_lines) or []

    if not tables:
        settings_text = {
            "vertical_strategy": "text",
            "horizontal_strategy": "text",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "min_words_vertical": 3,
            "min_words_horizontal": 1,
            "text_tolerance": 3,
        }
        tables = page.find_tables(table_settings=settings_text) or []

    tables = sorted(tables, key=lambda t: t.bbox[1])  # by top
    return tables


def _add_original_sheet(ws_orig, pdf, resolution: int = 160) -> None:
    """Render pages into ORIGINAL sheet (image fidelity)."""
    y = 1
    for page in pdf.pages:
        im = page.to_image(resolution=resolution).original
        bio = io.BytesIO()
        im.save(bio, format="PNG")
        bio.seek(0)
        xl_img = XLImage(bio)
        xl_img.anchor = f"A{y}"
        ws_orig.add_image(xl_img)

        # Rough row advance based on image height
        pts = im.height * 72 / 96
        y += int(pts / 15) + 4


def _build_workbook(pdf_bytes: bytes) -> bytes:
    """Return an XLSX with:
      - ORIGINAL: page images (faithful PDF view)
      - One EDITABLE sheet per detected table (to avoid tables "breaking" when stacked).
    """
    import io as _io

    def _sanitize_sheet_name(name: str, existing: set[str]) -> str:
        # Excel sheet name constraints: <=31 chars; cannot contain : \ / ? * [ ]
        name = re.sub(r'[:\\/*?\[\]]', '_', name).strip()
        if not name:
            name = "EDITABLE"
        name = name[:31]
        base = name
        k = 2
        while name in existing:
            suffix = f"_{k}"
            name = (base[: (31 - len(suffix))] + suffix) if len(base) + len(suffix) > 31 else (base + suffix)
            k += 1
        return name

    wb = openpyxl.Workbook()
    ws_orig = wb.active
    ws_orig.title = "ORIGINAL"

    with pdfplumber.open(_io.BytesIO(pdf_bytes)) as pdf:
        # 1) ORIGINAL sheet: images of each PDF page
        _add_original_sheet(ws_orig, pdf, resolution=160)

        # 2) Collect all tables (across all pages)
        table_specs = []  # (pageno, tidx, grid, merges, col_pts, row_pts)
        for pageno, page in enumerate(pdf.pages, start=1):
            tables = _pick_tables(page)
            for tidx, t in enumerate(tables, start=1):
                try:
                    grid, merges, col_pts, row_pts = _table_to_grid(page, t)
                except Exception:
                    continue

                # Skip empty grids
                if (not grid) or all(
                    all((c is None) or (str(c).strip() == "") for c in row)
                    for row in grid
                ):
                    continue

                table_specs.append((pageno, tidx, grid, merges, col_pts, row_pts))

        # 3) Write each table in its own sheet
        if not table_specs:
            ws = wb.create_sheet("EDITABLE")
            ws["A1"] = "No se detectaron tablas en este PDF."
        else:
            multi = len(table_specs) > 1
            existing = set(wb.sheetnames)
            for idx, (pageno, tidx, grid, merges, col_pts, row_pts) in enumerate(table_specs, start=1):
                if not multi:
                    raw_name = "EDITABLE"
                else:
                    raw_name = f"EDIT_P{pageno:02d}_T{tidx:02d}"
                name = _sanitize_sheet_name(raw_name, existing)
                existing.add(name)

                ws = wb.create_sheet(name)
                _set_column_widths(ws, col_pts, total_excel_width=150)

                # Set row heights to match the PDF table geometry (best effort)
                _set_row_heights(ws, row_pts, start_row=1)

                _write_table(ws, grid, start_row=1, start_col=1, merges=merges)

    out = _io.BytesIO()
    wb.save(out)
    return out.getvalue()
@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    name = file.filename or "archivo.pdf"
    data = await file.read()

    # Basic guard
    if not (name.lower().endswith('.pdf') or (file.content_type or '').lower() == 'application/pdf'):
        return JSONResponse({"error": "Solo PDF"}, status_code=400)

    try:
        xlsx_bytes = _build_workbook(data, name)
    except Exception as e:
        # Keep error readable for debugging in frontend
        return JSONResponse({"error": f"No se pudo convertir: {type(e).__name__}: {e}"}, status_code=500)

    out_name = re.sub(r"\.pdf$", "", name, flags=re.I) + "_ExcelLimpio_PRO.xlsx"

    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )
