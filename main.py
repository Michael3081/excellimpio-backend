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


def _build_workbook(pdf_bytes: bytes, original_name: str) -> bytes:
    """Create the PRO xlsx as bytes."""

    wb = openpyxl.Workbook()
    ws_orig = wb.active
    ws_orig.title = "ORIGINAL"
    ws_edit = wb.create_sheet("EDITABLE")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # 1) ORIGINAL
        _add_original_sheet(ws_orig, pdf)

        # 2) EDITABLE
        cur_row = 1
        col_pts_master: Optional[List[float]] = None

        for pageno, page in enumerate(pdf.pages, start=1):
            tables = _pick_tables(page)

            # Words for non-table text
            words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []

            # If there is a "main" table, use its bbox to split top/bottom text
            main_table = None
            if tables:
                def _area(t):
                    x0, top, x1, bottom = t.bbox
                    return (x1 - x0) * (bottom - top)
                main_table = max(tables, key=_area)

            if main_table:
                x0, top, x1, bottom = main_table.bbox
                margin = 6
                top_words = [w for w in words if w.get('bottom', w['top']) < top - margin]
                bottom_words = [w for w in words if w['top'] > bottom + margin]
            else:
                top_words = words
                bottom_words = []

            # If this is not the first page, add a print page break without inserting extra text.
            if cur_row > 1:
                ws_edit.row_breaks.append(Break(id=cur_row))
                cur_row += 1

            # --- Top text lines ---
            top_lines = _extract_lines(top_words)
            for line in top_lines:
                ws_edit.cell(row=cur_row, column=1, value=line).font = BASE_FONT
                # merge across a reasonable width (will expand after we know column count)
                ws_edit.row_dimensions[cur_row].height = 15
                cur_row += 1

            # --- Tables ---
            if tables:
                # Choose a representative table for column widths (largest).
                def _area(t):
                    x0, top, x1, bottom = t.bbox
                    return (x1 - x0) * (bottom - top)

                rep = max(tables, key=_area)
                grid, merges, col_pts, row_pts = _table_to_grid(page, rep)

                # Initialize column widths from the first representative table.
                if col_pts_master is None:
                    col_pts_master = col_pts
                    _set_column_widths(ws_edit, col_pts_master, total_excel_width=150)

                start_row = cur_row
                _write_table(ws_edit, grid, merges, start_row=start_row)
                _set_row_heights(ws_edit, row_pts, start_row=start_row)
                cur_row = start_row + len(grid)

                # If there are other tables on the page, append below (best-effort).
                others = [t for t in tables if t is not rep]
                for t in others:
                    cur_row += 2
                    g2, m2, col_pts2, row_pts2 = _table_to_grid(page, t)
                    # If table has more columns than current, extend widths.
                    if col_pts_master is not None and len(col_pts2) > len(col_pts_master):
                        # Extend with proportional widths
                        extra = col_pts2[len(col_pts_master):]
                        col_pts_master = col_pts_master + extra
                        _set_column_widths(ws_edit, col_pts_master, total_excel_width=150)
                    s2 = cur_row
                    _write_table(ws_edit, g2, m2, start_row=s2)
                    _set_row_heights(ws_edit, row_pts2, start_row=s2)
                    cur_row = s2 + len(g2)

            # --- Bottom text lines ---
            bottom_lines = _extract_lines(bottom_words)
            if bottom_lines:
                cur_row += 1
                for line in bottom_lines:
                    ws_edit.cell(row=cur_row, column=1, value=line).font = BASE_FONT
                    ws_edit.row_dimensions[cur_row].height = 15
                    cur_row += 1

        # Merge text lines across all columns (after widths are known)
        max_col = max(1, ws_edit.max_column)
        for r in range(1, ws_edit.max_row + 1):
            # If row has only col A populated and the rest empty, treat as a text line and merge.
            if ws_edit.cell(r, 1).value and all(ws_edit.cell(r, c).value in (None, "") for c in range(2, max_col + 1)):
                if max_col > 1:
                    ws_edit.merge_cells(start_row=r, start_column=1, end_row=r, end_column=max_col)
                ws_edit.cell(r, 1).alignment = Alignment(vertical="top", wrap_text=True)

    # Write to bytes
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.read()


# ------------------------
# API
# ------------------------

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
