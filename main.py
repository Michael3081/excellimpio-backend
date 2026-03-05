"""
ExcelLimpio PRO Backend (FastAPI)
--------------------------------
Entrega un solo XLSX con 2 hojas:
- ORIGINAL (Vista): páginas del PDF renderizadas como imagen (fiel al original, con logos).
- EDITABLE (Tabla): tabla principal extraída y ordenada para editar.

Endpoint:
- GET /health
- POST /convert  (multipart/form-data: file=<pdf>)
"""

import io
import os
import re
from typing import List, Optional, Tuple

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import pdfplumber
import pypdfium2 as pdfium
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter


APP_TITLE = "ExcelLimpio PRO Backend"
APP_VERSION = "9.0.0"

# Seguridad/robustez básica: evita PDFs gigantes en plan gratuito
MAX_PDF_MB = 15
MAX_PAGES_RENDER = 10
RENDER_SCALE = 2  # 2 ~= 144dpi; 2.5 ~= 180dpi (más pesado)

app = FastAPI(title=APP_TITLE, version=APP_VERSION, description="Convierte PDF a Excel PRO (vista fiel + tabla editable).")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # para Netlify. Luego puede cerrarlo a su dominio.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "message": "ExcelLimpio PRO backend activo", "version": APP_VERSION}


def _render_pdf_pages(pdf_bytes: bytes, scale: float = RENDER_SCALE, max_pages: int = MAX_PAGES_RENDER):
    pdf = pdfium.PdfDocument(pdf_bytes)
    n = len(pdf)
    pages = []
    for i in range(min(n, max_pages)):
        page = pdf[i]
        pil_img = page.render(scale=scale).to_pil()
        pages.append(pil_img)
    return pages, n


def _best_table_from_pdf(pdf_bytes: bytes) -> Optional[List[List[Optional[str]]]]:
    """
    Busca la tabla "más grande" (por cantidad de columnas, luego filas).
    Devuelve la tabla como lista de filas (cada fila es lista de celdas).
    """
    best = None  # (cols, rows, table)
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                if not table:
                    continue
                rows = len(table)
                cols = max(len(r) for r in table)
                score = (cols, rows)
                if best is None or score > (best[0], best[1]):
                    best = (cols, rows, table)
    return None if best is None else best[2]


def _map_table_to_editable(table: List[List[Optional[str]]]) -> List[List[str]]:
    """
    Mapea la tabla detectada a columnas estándar:
    [N°, Actividad, AP, NA, Responsable, Emisor(Proyectos), Emisor(Operaciones), Observaciones, Nombre Emisor (Yanacocha)]

    Nota importante:
    En algunos PDFs (como el checklist de prueba), la última columna trae varios valores "apilados" en una fila
    de "título de sección" (p.ej. la fila "2") y deja en blanco las filas 2.1, 2.2, 2.3, 2.4.
    Aquí corregimos ese corrimiento distribuyendo OPA / IT / H&S / SC a sus filas correspondientes.
    """
    mapped: List[List[str]] = []

    def s(x: Optional[str]) -> str:
        return (x or "").strip()

    # 1) mapping base (según posiciones típicas del PDF de prueba)
    for r in table:
        if not r or not r[0]:
            continue
        first = s(r[0])
        if not re.match(r"^\d+(\.\d+)?$", first):
            continue

        rr = list(r) + [""] * (12 - len(r))  # padding
        mapped.append([
            s(rr[0]),   # N°
            s(rr[1]),   # Actividad
            s(rr[5]),   # AP
            s(rr[6]),   # NA
            s(rr[7]),   # Responsable
            s(rr[8]),   # Emisor (Proyectos)
            s(rr[9]),   # Emisor (Operaciones)
            s(rr[10]),  # Observaciones
            s(rr[11]),  # Nombre Emisor (Yanacocha)
        ])

    # 2) corrección de corrimiento en "Nombre Emisor (Yanacocha)" para secciones con sub-items
    def normalize_key(k: str) -> str:
        k = k.strip()
        k = k.replace(" ", "")
        return k.upper()

    def key_from_emisor_proy(txt: str) -> Optional[str]:
        u = txt.upper()
        if "(OPA" in u or u.strip() == "OPA":
            return "OPA"
        if u.strip() == "TI":
            return "IT"
        if "H&S" in u or u.strip() == "HS":
            return "H&S"
        if "SUPPLY" in u or "(SC" in u or u.strip() == "SC":
            return "SC"
        return None

    i = 0
    while i < len(mapped):
        n = mapped[i][0]
        # detecta fila "sección" (entero sin punto) con col. final multi-línea
        if re.match(r"^\d+$", n):
            last = mapped[i][8] or ""
            lines = [ln.strip() for ln in last.splitlines() if ln.strip()]
            looks_like_bundle = len(lines) >= 2 and any((":" in ln) or (ln.upper() in {"OPA"}) for ln in lines)

            # fila sección: muchas columnas vacías + bundle en última col
            empties = sum(1 for x in mapped[i][2:8] if not (x or "").strip())
            if looks_like_bundle and empties >= 5:
                prefix = f"{n}."
                j = i + 1
                sub_idx = []
                while j < len(mapped) and mapped[j][0].startswith(prefix):
                    sub_idx.append(j)
                    j += 1

                # armamos diccionario key->texto
                token_map = {}
                for ln in lines:
                    if ":" in ln:
                        k, v = ln.split(":", 1)
                        token_map[normalize_key(k)] = ln.strip()
                    else:
                        token_map[normalize_key(ln)] = ln.strip()

                # distribuimos sólo si hay subitems y éstos están vacíos en la última col
                if sub_idx:
                    for sj in sub_idx:
                        if (mapped[sj][8] or "").strip():
                            continue
                        k = key_from_emisor_proy(mapped[sj][5])
                        if not k:
                            continue
                        hit = token_map.get(normalize_key(k))
                        if hit:
                            mapped[sj][8] = hit
                            # opcional: eliminar para no reusar
                            token_map.pop(normalize_key(k), None)

                    # la fila de sección no debe cargar valores de emisores
                    mapped[i][8] = ""

        i += 1

    return mapped

def _build_workbook(pdf_filename: str, pdf_bytes: bytes) -> bytes:
    # 1) Render de páginas
    pages, total_pages = _render_pdf_pages(pdf_bytes, scale=RENDER_SCALE, max_pages=MAX_PAGES_RENDER)

    # 2) Tabla editable
    raw_table = _best_table_from_pdf(pdf_bytes)
    editable_rows = _map_table_to_editable(raw_table) if raw_table else []

    wb = Workbook()

    # --------------------
    # Sheet ORIGINAL (Vista)
    # --------------------
    ws_o = wb.active
    ws_o.title = "ORIGINAL (Vista)"

    # “grid” para que el zoom sea cómodo (no afecta la imagen)
    for col in range(1, 15):
        ws_o.column_dimensions[get_column_letter(col)].width = 3.0
    for r in range(1, 500):
        ws_o.row_dimensions[r].height = 12

    start_row = 1
    approx_row_px = 16  # 12pt ≈ 16px
    for pil_img in pages:
        bio = io.BytesIO()
        pil_img.save(bio, format="PNG")
        bio.seek(0)
        xlimg = XLImage(bio)
        ws_o.add_image(xlimg, f"A{start_row}")

        # bajar para la siguiente página
        start_row += int(pil_img.size[1] / approx_row_px) + 8

    if total_pages > MAX_PAGES_RENDER:
        ws_o["A1"] = f"Nota: se renderizaron solo {MAX_PAGES_RENDER} de {total_pages} páginas (para rendimiento)."
        ws_o["A1"].font = Font(bold=True, color="C00000")

    # --------------------
    # Sheet EDITABLE (Tabla)
    # --------------------
    ws = wb.create_sheet("EDITABLE (Tabla)")
    ws["A1"] = "ExcelLimpio PRO - Editable"
    ws["A2"] = f"Archivo: {pdf_filename}"
    ws["A1"].font = Font(size=14, bold=True)
    ws["A2"].font = Font(italic=True, color="666666")
    ws.merge_cells("A1:I1")
    ws.merge_cells("A2:I2")

    headers = [
        "N°",
        "Actividad",
        "AP",
        "NA",
        "Responsable",
        "Emisor (Proyectos)",
        "Emisor (Operaciones)",
        "Observaciones",
        "Nombre Emisor (Yanacocha)",
    ]

    header_row = 4
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF")
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for c, h in enumerate(headers, start=1):
        cell = ws.cell(header_row, c, h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_align
    ws.row_dimensions[header_row].height = 28

    # data
    start = header_row + 1
    for i, row in enumerate(editable_rows, start=start):
        for c, val in enumerate(row, start=1):
            cell = ws.cell(i, c, val)
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    # grid / borders
    thin = Side(style="thin", color="999999")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    last_row = max(start, start + len(editable_rows) - 1)
    for r in range(header_row, last_row + 1):
        ws.row_dimensions[r].height = 18
        for c in range(1, 10):
            ws.cell(r, c).border = border

    # column widths
    widths = [6, 60, 4, 4, 18, 22, 22, 60, 26]
    for c, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(c)].width = w

    # center small cols
    for r in range(start, last_row + 1):
        for c in (1, 3, 4):
            ws.cell(r, c).alignment = Alignment(horizontal="center", vertical="top", wrap_text=True)

    ws.freeze_panes = "A5"

    # output bytes
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Suba un archivo .pdf")

    pdf_bytes = await file.read()
    size_mb = len(pdf_bytes) / (1024 * 1024)
    if size_mb > MAX_PDF_MB:
        raise HTTPException(status_code=413, detail=f"PDF demasiado grande ({size_mb:.1f} MB). Límite: {MAX_PDF_MB} MB")

    try:
        xlsx_bytes = _build_workbook(file.filename, pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando el PDF: {e}")

    out_name = os.path.splitext(file.filename)[0] + "_ExcelLimpio_PRO.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{out_name}"'}

    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
