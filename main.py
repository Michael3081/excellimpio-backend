# ExcelLimpio PRO Backend (v11)
# - 2 hojas: ORIGINAL (imágenes del PDF) + EDITABLE (tablas extraídas)
# - Headers dinámicos por archivo (NO reutiliza plantillas de PDFs anteriores)
# - Soporta multipágina: si la tabla continúa en otra página, la une automáticamente

from __future__ import annotations

import io
import math
import os
import re
import tempfile
from typing import List, Tuple, Dict, Any, Optional

import pdfplumber
import fitz  # PyMuPDF
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage


APP_NAME = "ExcelLimpio PRO"
MAX_MB = int(os.getenv("MAX_MB", "25"))          # límite de subida
RENDER_ZOOM = float(os.getenv("RENDER_ZOOM", "2.0"))  # calidad de imagen para hoja ORIGINAL
MAX_ORIG_WIDTH_PX = int(os.getenv("MAX_ORIG_WIDTH_PX", "1200"))

# Si quiere restringir CORS a su dominio Netlify, ponga:
# CORS_ORIGINS="https://excel-limpio.netlify.app,https://<su-dominio>"
cors_env = os.getenv("CORS_ORIGINS", "*")
if cors_env.strip() == "*":
    ALLOW_ORIGINS = ["*"]
else:
    ALLOW_ORIGINS = [o.strip() for o in cors_env.split(",") if o.strip()]


app = FastAPI(title="ExcelLimpio Backend v1", version="1.0.0", description="Convierte PDFs a un Excel estructurado (ORIGINAL + EDITABLE).")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


_alpha_re = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]")
_digit_re = re.compile(r"\d")


def _clean_cell(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).replace("\x00", "")
    s = s.replace("\n", " ").strip()
    s = re.sub(r"\s{2,}", " ", s)
    return s


def _normalize_header_cell(s: Any) -> str:
    s2 = _clean_cell(s).strip(":")
    s2 = re.sub(r"\s{2,}", " ", s2)
    return s2


def _looks_like_header_row(row: List[Any]) -> bool:
    cells = [_clean_cell(c) for c in row]
    non = [c for c in cells if c]
    if len(non) < max(2, len(cells) // 3):
        return False

    alpha = sum(1 for c in non if _alpha_re.search(c))
    numheavy = sum(
        1 for c in non
        if _digit_re.search(c) and not _alpha_re.search(c) and len(re.sub(r"\D", "", c)) >= 3
    )
    joined = " ".join(non).lower()
    kw = any(k in joined for k in ["sku", "descrip", "cantidad", "precio", "item", "actividad", "responsable", "emisor", "observ"])

    return (alpha >= len(non) * 0.5 and numheavy <= len(non) * 0.4) or kw


def _extract_tables_from_pdf(pdf_path: str) -> List[Tuple[int, List[List[str]]]]:
    """
    Devuelve lista de (page_number, table_rows) donde table_rows es lista de filas (lista de strings).
    """
    all_tables: List[Tuple[int, List[List[str]]]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for pageno, page in enumerate(pdf.pages, start=1):
            settings_list = [
                # 1) Mejor cuando hay líneas/tabla bien definida
                {
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                    "edge_min_length": 3,
                    "min_words_vertical": 1,
                    "min_words_horizontal": 1,
                    "intersection_tolerance": 3,
                },
                # 2) Fallback cuando no hay líneas (tabla “por texto”)
                {
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                    "min_words_vertical": 3,
                    "min_words_horizontal": 1,
                },
            ]

            tables: List[List[List[Any]]] = []
            for st in settings_list:
                try:
                    tables = page.extract_tables(table_settings=st) or []
                except Exception:
                    tables = []
                tables = [
                    t for t in tables
                    if t and len(t) >= 2 and any(any(_clean_cell(c) for c in r) for r in t)
                ]
                if tables:
                    break

            for t in tables:
                t_clean = [[_clean_cell(c) for c in row] for row in t]
                all_tables.append((pageno, t_clean))

    return all_tables


def _render_pdf_pages_to_png_bytes(pdf_path: str, zoom: float) -> List[bytes]:
    doc = fitz.open(pdf_path)
    imgs: List[bytes] = []
    for i in range(doc.page_count):
        page = doc.load_page(i)
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        imgs.append(pix.tobytes("png"))
    doc.close()
    return imgs


def _write_images_sheet(ws, img_bytes_list: List[bytes]) -> None:
    """
    Pega cada página como imagen en la hoja ORIGINAL, una debajo de otra.
    """
    row = 1
    for b in img_bytes_list:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(b)
            tmp_path = tmp.name

        img = XLImage(tmp_path)

        # Escalar a ancho máximo
        if img.width and img.width > MAX_ORIG_WIDTH_PX:
            scale = MAX_ORIG_WIDTH_PX / img.width
            img.width = int(img.width * scale)
            img.height = int(img.height * scale)

        ws.add_image(img, f"A{row}")

        # Estimar cuántas filas consume (20px ~ 1 fila)
        rows_consumed = int(math.ceil((img.height or 800) / 20.0)) + 2
        row += rows_consumed

    ws.sheet_view.showGridLines = False


def _merge_blank_columns_between_actividad_and_ap(header: List[str], rows: List[List[str]]) -> Tuple[List[str], List[List[str]]]:
    """
    Heurística: si hay columnas “vacías” (header == "") entre Actividad y AP/NA,
    se concatenan dentro de Actividad y se eliminan esas columnas.
    """
    def idx_exact(name: str) -> Optional[int]:
        for i, h in enumerate(header):
            if (h or "").strip().lower() == name.lower():
                return i
        return None

    act_idx = idx_exact("Actividad")
    if act_idx is None:
        for i, h in enumerate(header):
            if (h or "").strip().lower().startswith("actividad"):
                act_idx = i
                break

    ap_idx = idx_exact("AP") or idx_exact("NA")
    if act_idx is None or ap_idx is None or ap_idx <= act_idx + 1:
        return header, rows

    to_merge = [j for j in range(act_idx + 1, ap_idx) if (header[j] or "").strip() == ""]
    if not to_merge:
        return header, rows

    keep = [j for j in range(len(header)) if j not in to_merge]
    new_header = [header[j] for j in keep]

    new_rows: List[List[str]] = []
    for r in rows:
        merged = _clean_cell(r[act_idx])
        for j in to_merge:
            extra = _clean_cell(r[j])
            if extra:
                merged = (merged + " " + extra).strip()

        new_r = []
        for j in keep:
            new_r.append(merged if j == act_idx else _clean_cell(r[j]))
        new_rows.append(new_r)

    return new_header, new_rows


def _is_noise_block(header: List[str], rows: List[List[str]]) -> bool:
    joined = " ".join([str(x) for x in header]).lower()
    if any(k in joined for k in ["envelope id", "docusign", "status:"]):
        return True

    non_empty = sum(1 for h in header if str(h).strip())
    if len(header) <= 3 and non_empty <= 2 and len(rows) > 10:
        return True

    return False


def _consolidate_tables(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Devuelve bloques de tablas consolidadas:
    [
      { "page": 1, "header": [...], "rows": [[...], ...] },
      ...
    ]
    Si una tabla continúa en la siguiente página (mismo header), se une.
    """
    raw_tables = _extract_tables_from_pdf(pdf_path)

    blocks: List[Dict[str, Any]] = []
    cur_header: Optional[List[str]] = None
    cur_ncols: Optional[int] = None
    cur_rows: List[List[str]] = []
    cur_page: Optional[int] = None

    def hdr_key(h: List[str]) -> List[str]:
        return [re.sub(r"\s+", " ", _clean_cell(x)).lower() for x in h]

    def flush():
        nonlocal cur_header, cur_ncols, cur_rows, cur_page
        if cur_header and cur_rows and cur_ncols:
            # eliminar columnas totalmente vacías
            cols_keep = [j for j in range(cur_ncols) if any(_clean_cell(r[j]) for r in ([cur_header] + cur_rows))]
            header = [_normalize_header_cell(cur_header[j]) for j in cols_keep]
            data = [[_clean_cell(r[j]) for j in cols_keep] for r in cur_rows]

            # Heurísticas “seguras”:
            # 1) Si el primer header es “Actividades de Cierre” y el siguiente es vacío, poner N° y Actividad
            if header and header[0].lower().startswith("actividades de cierre"):
                if len(header) >= 2 and header[1] == "":
                    header[0] = "N°"
                    header[1] = "Actividad"

            # 2) Normalizar “Actividad…”
            header = [("Actividad" if (h or "").strip().lower().startswith("actividad") else h) for h in header]

            # 3) Si hay “Emisor” + columna vacía, renombrar a (Proyectos) y (Operaciones)
            for j in range(len(header) - 1):
                if header[j].lower() == "emisor" and header[j + 1] == "":
                    header[j] = "Emisor (Proyectos)"
                    header[j + 1] = "Emisor (Operaciones)"

            # 4) Unir columnas vacías entre Actividad y AP/NA (típico en checklists)
            header, data = _merge_blank_columns_between_actividad_and_ap(header, data)

            if not _is_noise_block(header, data):
                blocks.append({"page": cur_page or 1, "header": header, "rows": data})

        cur_header = None
        cur_ncols = None
        cur_rows = []
        cur_page = None

    for pageno, table in raw_tables:
        # Buscar header dentro de las primeras filas (a veces hay títulos arriba)
        header_idx = None
        for i, row in enumerate(table[:8]):
            if _looks_like_header_row(row):
                header_idx = i
                break

        if header_idx is None:
            # No hay header claro: generar columnas genéricas
            hdr = [f"Col {i+1}" for i in range(len(table[0]))]
            if cur_header is None or len(hdr) != (cur_ncols or 0):
                flush()
                cur_header = hdr
                cur_ncols = len(hdr)
                cur_page = pageno

            for row in table:
                if not any(_clean_cell(c) for c in row):
                    continue
                row = row + [""] * ((cur_ncols or 0) - len(row)) if len(row) < (cur_ncols or 0) else row[: (cur_ncols or 0)]
                cur_rows.append(row)
            continue

        hdr_norm = [_normalize_header_cell(c) for c in table[header_idx]]
        ncols = len(hdr_norm)

        if cur_header is None:
            cur_header = hdr_norm
            cur_ncols = ncols
            cur_page = pageno
        else:
            # Si cambia el header (o cambia # columnas), empezamos bloque nuevo
            if ncols != cur_ncols or hdr_key(hdr_norm) != hdr_key(cur_header):
                flush()
                cur_header = hdr_norm
                cur_ncols = ncols
                cur_page = pageno

        # Agregar filas de data
        for row in table[header_idx + 1 :]:
            if not any(_clean_cell(c) for c in row):
                continue
            # saltar si es header repetido
            if _looks_like_header_row(row) and hdr_key([_normalize_header_cell(c) for c in row]) == hdr_key(cur_header):
                continue

            if len(row) < (cur_ncols or 0):
                row = row + [""] * ((cur_ncols or 0) - len(row))
            elif len(row) > (cur_ncols or 0):
                row = row[: (cur_ncols or 0)]
            cur_rows.append(row)

    flush()
    return blocks


def _style_table(ws, start_row: int, header: List[str], rows: List[List[str]]) -> None:
    thin = Side(style="thin", color="D0D0D0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    header_fill = PatternFill("solid", fgColor="0B3954")
    header_font = Font(bold=True, color="FFFFFF")
    wrap = Alignment(wrap_text=True, vertical="top")
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # header
    for j, col in enumerate(header, start=1):
        cell = ws.cell(row=start_row, column=j, value=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center
        cell.border = border

    # data
    for i, r in enumerate(rows, start=1):
        for j, val in enumerate(r, start=1):
            c = ws.cell(row=start_row + i, column=j, value=val)
            c.alignment = wrap
            c.border = border

    # ancho de columnas aproximado
    for j in range(1, len(header) + 1):
        maxlen = len(str(header[j - 1] or ""))
        for i in range(1, min(len(rows), 200) + 1):
            v = rows[i - 1][j - 1]
            maxlen = max(maxlen, len(str(v or "")))
        maxlen = min(maxlen, 60)
        ws.column_dimensions[get_column_letter(j)].width = max(10, maxlen * 0.9)

    ws.freeze_panes = ws["A2"]


def _build_workbook(pdf_path: str) -> openpyxl.Workbook:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    ws_orig = wb.create_sheet("ORIGINAL")
    ws_edit = wb.create_sheet("EDITABLE")

    # ORIGINAL: imágenes
    imgs = _render_pdf_pages_to_png_bytes(pdf_path, zoom=RENDER_ZOOM)
    _write_images_sheet(ws_orig, imgs)

    # EDITABLE: tablas
    blocks = _consolidate_tables(pdf_path)

    row_cursor = 1
    for bi, b in enumerate(blocks, start=1):
        if bi > 1:
            row_cursor += 2
            ws_edit.cell(row=row_cursor, column=1, value=f"Tabla {bi} (desde página {b['page']})").font = Font(bold=True)
            row_cursor += 1

        _style_table(ws_edit, row_cursor, b["header"], b["rows"])
        row_cursor += len(b["rows"]) + 2

    return wb


@app.get("/Health")
def health():
    return {"status": "ok", "message": "ExcelLimpio backend activo"}


@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="Falta el archivo")

    filename = file.filename or "archivo.pdf"
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Solo se acepta PDF")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    size_mb = len(data) / (1024 * 1024)
    if size_mb > MAX_MB:
        raise HTTPException(status_code=413, detail=f"El archivo supera {MAX_MB} MB")

    # Guardar PDF temporal (pdfplumber + fitz trabajan mejor con path)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(data)
        pdf_path = tmp.name

    try:
        wb = _build_workbook(pdf_path)

        out = io.BytesIO()
        wb.save(out)
        out.seek(0)

        out_name = re.sub(r"\.pdf$", "", filename, flags=re.I) + "_ExcelLimpio_PRO.xlsx"

        return Response(
            content=out.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al convertir: {e}")
    finally:
        try:
            os.remove(pdf_path)
        except Exception:
            pass
