# ExcelLimpio PRO v21
# - ORIGINAL: una hoja por página con imagen fiel del PDF
# - EDITABLE: 1 hoja por tabla detectada (EDITABLE_pX_tY) con anchos/altos aproximados al PDF
# - Si una página no tiene tablas detectables, crea EDITABLE_pX_text con el texto.

import io
import os
import re
import tempfile
from typing import List, Tuple, Optional

import pdfplumber
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Border, Side, PatternFill, Font
from openpyxl.drawing.image import Image as XLImage


app = FastAPI(title="ExcelLimpio Backend v1", version="1.0.0")

# CORS: deje '*' para evitar bloqueos con Netlify durante pruebas.
# Si luego quiere endurecerlo, cambie allow_origins a su dominio Netlify.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

thin_side = Side(style="thin", color="000000")
thin_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)


def _cluster(vals: List[float], tol: float = 1.0) -> List[float]:
    if not vals:
        return []
    vals = sorted(vals)
    out: List[float] = []
    cur = [vals[0]]
    for v in vals[1:]:
        if abs(v - cur[-1]) <= tol:
            cur.append(v)
        else:
            out.append(sum(cur) / len(cur))
            cur = [v]
    out.append(sum(cur) / len(cur))
    return out


def _bbox_area(b: Tuple[float, float, float, float]) -> float:
    return max(0.0, (b[2] - b[0])) * max(0.0, (b[3] - b[1]))


def _bbox_contains(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float], margin: float = 2.0) -> bool:
    return (b[0] >= a[0] - margin and b[1] >= a[1] - margin and b[2] <= a[2] + margin and b[3] <= a[3] + margin)


def _iou(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> float:
    ix0 = max(a[0], b[0])
    iy0 = max(a[1], b[1])
    ix1 = min(a[2], b[2])
    iy1 = min(a[3], b[3])
    inter = max(0.0, ix1 - ix0) * max(0.0, iy1 - iy0)
    union = _bbox_area(a) + _bbox_area(b) - inter
    return inter / union if union > 0 else 0.0


def _table_grid_dims(table) -> Tuple[int, int]:
    xs: List[float] = []
    ys: List[float] = []
    for (x0, top, x1, bottom) in table.cells:
        xs.extend([x0, x1])
        ys.extend([top, bottom])
    xs = _cluster(xs, tol=1.0)
    ys = _cluster(ys, tol=1.0)
    return max(0, len(xs) - 1), max(0, len(ys) - 1)


def _points_to_excel_col_width(points: float) -> float:
    # PDF points (1/72") -> pixels @96dpi -> Excel "chars"
    pixels = points * (96.0 / 72.0)
    width = max(2.0, (pixels - 5.0) / 7.0)
    return min(width, 80.0)


def _find_index(edges: List[float], value: float, tol: float = 1.5) -> int:
    # closest edge
    best = 0
    best_d = 1e18
    for i, e in enumerate(edges):
        d = abs(e - value)
        if d < best_d:
            best_d = d
            best = i
    return best


def _extract_text_in_bbox(page, bbox: Tuple[float, float, float, float]) -> str:
    try:
        txt = page.crop(bbox).extract_text(x_tolerance=1, y_tolerance=1)
        if not txt:
            return ""
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
        lines = [ln for ln in lines if ln]
        return "\n".join(lines)
    except Exception:
        return ""


def _extract_tables_candidates(page) -> List:
    settings_variants = [
        {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_tolerance": 5},
        {"vertical_strategy": "lines", "horizontal_strategy": "text",  "intersection_tolerance": 5},
        {"vertical_strategy": "text",  "horizontal_strategy": "lines", "intersection_tolerance": 5},
        {"vertical_strategy": "text",  "horizontal_strategy": "text",  "intersection_tolerance": 5},
    ]
    seen = set()
    out = []
    for s in settings_variants:
        try:
            tables = page.find_tables(table_settings=s)
        except Exception:
            continue
        for t in tables:
            bbox = tuple(round(x, 1) for x in t.bbox)
            if bbox in seen:
                continue
            if (bbox[2] - bbox[0]) < 50 or (bbox[3] - bbox[1]) < 30:
                continue
            seen.add(bbox)
            out.append(t)
    out.sort(key=lambda t: (t.bbox[1], t.bbox[0]))
    return out


def _extract_tables_filtered(page) -> List:
    candidates = _extract_tables_candidates(page)
    if not candidates:
        return []

    page_area = page.width * page.height

    enriched = []
    for t in candidates:
        area = _bbox_area(t.bbox)
        ncols, nrows = _table_grid_dims(t)
        enriched.append((t, area, ncols, nrows))

    # Regla: quedarnos con tablas "reales" (no cajitas sueltas)
    filtered = []
    for t, area, ncols, nrows in enriched:
        grid = ncols * nrows
        if area < 0.03 * page_area and grid < 16:
            continue
        filtered.append((t, area))

    if not filtered:
        # fallback: mayor área
        tmax = max(enriched, key=lambda x: x[1])[0]
        return [tmax]

    # quitar tablas contenidas en otras más grandes
    filtered.sort(key=lambda x: x[1], reverse=True)
    kept: List = []
    for t, _area in filtered:
        b = t.bbox
        skip = False
        for kt in kept:
            if _bbox_contains(kt.bbox, b, margin=1) and _iou(kt.bbox, b) > 0.85:
                skip = True
                break
        if not skip:
            kept.append(t)

    kept.sort(key=lambda t: (t.bbox[1], t.bbox[0]))
    return kept


def _add_original_page_sheet(wb: Workbook, page, page_index: int, dpi: int = 150) -> None:
    ws = wb.create_sheet(title=f"ORIGINAL_p{page_index}")
    ws.sheet_view.showGridLines = False
    pil = page.to_image(resolution=dpi).original
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as f:
        pil.save(f.name, format="PNG")
        img_path = f.name
    xlimg = XLImage(img_path)
    xlimg.anchor = "A1"
    ws.add_image(xlimg)

    # Ajuste mínimo para que se vea sin zoom raro
    ws.column_dimensions["A"].width = 2
    rows_needed = int(pil.height / 20) + 5
    for r in range(1, rows_needed + 1):
        ws.row_dimensions[r].height = 15


def _add_table_sheet(wb: Workbook, page, table, sheet_name: str, style_header: bool = True) -> None:
    ws = wb.create_sheet(title=sheet_name[:31])
    ws.sheet_view.showGridLines = False

    # Grid desde celdas detectadas
    xs: List[float] = []
    ys: List[float] = []
    for (x0, top, x1, bottom) in table.cells:
        xs.extend([x0, x1])
        ys.extend([top, bottom])
    xs = sorted(_cluster(xs, tol=1.0))
    ys = sorted(_cluster(ys, tol=1.0))
    if len(xs) < 2 or len(ys) < 2:
        return

    col_pts = [xs[i + 1] - xs[i] for i in range(len(xs) - 1)]
    row_pts = [ys[j + 1] - ys[j] for j in range(len(ys) - 1)]

    # Set sizes
    for i, wpt in enumerate(col_pts):
        ws.column_dimensions[get_column_letter(1 + i)].width = _points_to_excel_col_width(wpt)
    for j, hpt in enumerate(row_pts):
        ws.row_dimensions[1 + j].height = max(6.0, min(hpt, 409.0))

    wrap = Alignment(wrap_text=True, vertical="top")

    # Pre border & alignment
    for r in range(1, len(ys)):
        for c in range(1, len(xs)):
            cell = ws.cell(row=r, column=c)
            cell.alignment = wrap
            cell.border = thin_border

    # Fill each detected cell (incl. merges)
    for bbox in table.cells:
        x0, top, x1, bottom = bbox
        c0 = _find_index(xs, x0)
        c1 = _find_index(xs, x1)
        r0 = _find_index(ys, top)
        r1 = _find_index(ys, bottom)

        start_col = c0 + 1
        end_col = max(start_col, c1)
        start_row = r0 + 1
        end_row = max(start_row, r1)

        if end_row > start_row or end_col > start_col:
            ws.merge_cells(start_row=start_row, start_column=start_col, end_row=end_row, end_column=end_col)

        txt = _extract_text_in_bbox(page, bbox)
        if txt:
            ws.cell(row=start_row, column=start_col, value=txt)

    # Optional header styling: if first row has many non-empty values
    if style_header:
        vals = [ws.cell(row=1, column=c).value for c in range(1, len(xs))]
        nonempty = sum(1 for v in vals if v and str(v).strip())
        if nonempty >= max(2, (len(xs) - 1) // 2):
            fill = PatternFill("solid", fgColor="0F172A")
            font = Font(bold=True, color="FFFFFF")
            center = Alignment(wrap_text=True, vertical="center", horizontal="center")
            for c in range(1, len(xs)):
                cell = ws.cell(row=1, column=c)
                cell.fill = fill
                cell.font = font
                cell.alignment = center


def _add_text_fallback_sheet(wb: Workbook, page, sheet_name: str) -> None:
    ws = wb.create_sheet(title=sheet_name[:31])
    ws.sheet_view.showGridLines = False
    txt = page.extract_text() or ""
    lines = [ln.rstrip() for ln in txt.splitlines() if ln.strip()]
    ws.column_dimensions["A"].width = 120
    for i, ln in enumerate(lines[:2000], start=1):
        ws.cell(row=i, column=1, value=ln).alignment = Alignment(wrap_text=True, vertical="top")


def convert_pdf_to_xlsx_bytes(pdf_path: str) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)

    with pdfplumber.open(pdf_path) as pdf:
        # ORIGINAL: una hoja por página
        for i, page in enumerate(pdf.pages, start=1):
            _add_original_page_sheet(wb, page, i, dpi=150)

        # EDITABLE: tablas por página
        for p_idx, page in enumerate(pdf.pages, start=1):
            tables = _extract_tables_filtered(page)
            if not tables:
                _add_text_fallback_sheet(wb, page, sheet_name=f"EDITABLE_p{p_idx}_text")
                continue
            for t_idx, t in enumerate(tables, start=1):
                _add_table_sheet(wb, page, t, sheet_name=f"EDITABLE_p{p_idx}_t{t_idx}", style_header=True)

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


@app.get("/health")
def health():
    return {"status": "ok", "message": "ExcelLimpio backend activo"}


@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Solo se acepta PDF.")
    data = await file.read()
    if not data or len(data) < 1000:
        raise HTTPException(status_code=400, detail="PDF vacío o inválido.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
        f.write(data)
        pdf_path = f.name

    try:
        xlsx_bytes = convert_pdf_to_xlsx_bytes(pdf_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error convirtiendo PDF: {e}")
    finally:
        try:
            os.remove(pdf_path)
        except Exception:
            pass

    out_name = re.sub(r"\.pdf$", "", os.path.basename(file.filename), flags=re.I) + "_ExcelLimpio_PRO.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}
    return StreamingResponse(io.BytesIO(xlsx_bytes),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers=headers)
