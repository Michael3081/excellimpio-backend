from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
import re
from datetime import datetime

import pdfplumber
import fitz  # PyMuPDF
from PIL import Image as PILImage

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage

APP_NAME = "ExcelLimpio Pro"
APP_VERSION = "1.2.0"
MAX_UPLOAD_MB = 15
MAX_PAGES_VISTA = 12
RENDER_SCALE = 1.6
MAX_IMG_WIDTH_PX = 1200

app = FastAPI(title=f"{APP_NAME} Backend", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


@app.get("/")
def root():
    return {"status": "ok", "message": f"{APP_NAME} backend activo"}

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


def _safe_filename(name: str) -> str:
    name = name or "archivo"
    name = re.sub(r"[^\w\-. ]+", "_", name, flags=re.UNICODE).strip()
    return name[:80] if len(name) > 80 else name


def _pdf_to_images(pdf_bytes: bytes):
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        for i, page in enumerate(doc):
            if i >= MAX_PAGES_VISTA:
                break
            mat = fitz.Matrix(RENDER_SCALE, RENDER_SCALE)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img = PILImage.open(io.BytesIO(pix.tobytes("png")))
            if img.width > MAX_IMG_WIDTH_PX:
                ratio = MAX_IMG_WIDTH_PX / float(img.width)
                new_h = int(img.height * ratio)
                img = img.resize((MAX_IMG_WIDTH_PX, new_h))
            images.append(img)
    finally:
        doc.close()
    return images


def _extract_tables(pdf_bytes: bytes):
    tables_out = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "intersection_tolerance": 5,
                    "snap_tolerance": 3,
                    "join_tolerance": 3,
                    "edge_min_length": 10,
                    "min_words_vertical": 1,
                    "min_words_horizontal": 1,
                    "keep_blank_chars": False,
                    "text_tolerance": 2,
                }) or []
            except Exception:
                tables = []
            if not tables:
                try:
                    tables = page.extract_tables({
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text",
                        "snap_tolerance": 3,
                        "join_tolerance": 3,
                        "min_words_vertical": 1,
                        "min_words_horizontal": 1,
                        "keep_blank_chars": False,
                        "text_tolerance": 2,
                    }) or []
                except Exception:
                    tables = []
            for t in tables:
                if t and any(any(c not in (None, "", " ") for c in row) for row in t):
                    tables_out.append(t)
    return tables_out


def _clean_table(table):
    norm = []
    for row in table:
        r = []
        for c in row:
            if c is None:
                r.append("")
            else:
                s = str(c)
                s = re.sub(r"\s+", " ", s).strip()
                r.append(s)
        norm.append(r)
    norm = [r for r in norm if any(cell != "" for cell in r)]
    if not norm:
        return []
    max_len = max(len(r) for r in norm)
    norm = [r + [""] * (max_len - len(r)) for r in norm]
    keep_cols = []
    for j in range(max_len):
        col = [r[j] for r in norm]
        if any(v != "" for v in col):
            keep_cols.append(j)
    if not keep_cols:
        return []
    return [[r[j] for j in keep_cols] for r in norm]


def _choose_best_table(tables):
    best = []
    best_score = 0
    for t in tables:
        ct = _clean_table(t)
        if not ct:
            continue
        score = sum(1 for r in ct for c in r if c != "") + len(ct[0]) * 10
        if score > best_score:
            best_score = score
            best = ct
    return best


def _style_header(ws, row=1, ncols=1):
    fill = PatternFill("solid", fgColor="1F4E79")
    font = Font(color="FFFFFF", bold=True)
    align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin = Side(style="thin", color="9E9E9E")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    ws.row_dimensions[row].height = 22
    for c in range(1, ncols + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = fill
        cell.font = font
        cell.alignment = align
        cell.border = border


def _style_body(ws, start_row, end_row, ncols):
    thin = Side(style="thin", color="D0D0D0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for r in range(start_row, end_row + 1):
        for c in range(1, ncols + 1):
            cell = ws.cell(row=r, column=c)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.border = border


def _autosize_cols(ws, ncols, max_width=55):
    for c in range(1, ncols + 1):
        col_letter = get_column_letter(c)
        max_len = 0
        for cell in ws[col_letter]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), max_width)




def _extract_layout_table(pdf_bytes: bytes):
    """Fallback extractor for PDFs that are not detected as classic tables.
    Builds a row list from layout (word coordinates) using PyMuPDF.
    Output: list-of-lists with header in first row.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        rows = []
        for pno in range(len(doc)):
            page = doc[pno]
            words = page.get_text("words")  # x0,y0,x1,y1,word,block,line,word_no

            # Find header AP / NA near top to anchor checkbox columns
            ap_candidates = [w for w in words if w[4] == "AP"]
            na_candidates = [w for w in words if w[4] == "NA"]
            ap_header = None
            na_header = None
            if ap_candidates and na_candidates:
                ap_header = min(ap_candidates, key=lambda w: w[1])
                na_same_band = [w for w in na_candidates if abs(w[1] - ap_header[1]) < 20]
                na_header = min(na_same_band, key=lambda w: w[1]) if na_same_band else min(na_candidates, key=lambda w: w[1])

            header_y = min(ap_header[1], na_header[1]) if (ap_header and na_header) else 0
            apx = ((ap_header[0] + ap_header[2]) / 2) if ap_header else None
            nax = ((na_header[0] + na_header[2]) / 2) if na_header else None

            # Item tokens: numbers like 1, 2.1, 3.2.5 at left margin
            item_tokens = []
            for w in words:
                txt = w[4]
                if re.match(r"^\d+(\.\d+)*$", txt) and w[0] < 120 and w[1] > header_y + 5:
                    item_tokens.append(w)

            item_tokens = sorted(item_tokens, key=lambda w: (w[1], w[0]))

            # De-duplicate almost identical tokens
            filtered = []
            for t in item_tokens:
                if not filtered:
                    filtered.append(t)
                    continue
                prev = filtered[-1]
                if abs(t[1] - prev[1]) < 1.5 and abs(t[0] - prev[0]) < 3 and t[4] == prev[4]:
                    continue
                filtered.append(t)
            item_tokens = filtered

            if not item_tokens:
                continue

            for idx, t in enumerate(item_tokens):
                y0 = t[1] - 2
                y1 = (item_tokens[idx + 1][1] - 2) if idx + 1 < len(item_tokens) else (page.rect.height + 2)

                band = [w for w in words if (w[1] >= y0 and w[1] < y1)]

                # Detect checkmarks near AP/NA columns
                xs = [w for w in band if w[4].lower() == "x" and 140 < w[0] < 260]
                ap_mark = ""
                na_mark = ""
                if apx and nax:
                    for xw in xs:
                        cx = (xw[0] + xw[2]) / 2
                        if abs(cx - apx) <= abs(cx - nax) and abs(cx - apx) < 25:
                            ap_mark = "x"
                        elif abs(cx - nax) < 25:
                            na_mark = "x"

                # Build text lines excluding the item token and standalone 'x'
                band2 = [
                    w for w in band
                    if w[4].lower() != "x"
                    and not (w[4] == t[4] and abs(w[0] - t[0]) < 3 and abs(w[1] - t[1]) < 2)
                ]
                band2 = sorted(band2, key=lambda w: (w[1], w[0]))

                lines = []
                current = []
                current_y = None
                for w in band2:
                    y = w[1]
                    if current_y is None or abs(y - current_y) <= 2.5:
                        current.append(w)
                        if current_y is None:
                            current_y = y
                    else:
                        line = " ".join([cw[4] for cw in sorted(current, key=lambda z: z[0])]).strip()
                        if line:
                            lines.append(line)
                        current = [w]
                        current_y = y
                if current:
                    line = " ".join([cw[4] for cw in sorted(current, key=lambda z: z[0])]).strip()
                    if line:
                        lines.append(line)

                # Heuristic: extract “responsables” lines
                resp_tags = ("H&S", "MA", "RS", "IT", "OPA", "QA", "QC", "SSOMA", "HSE")
                resp_lines = [ln for ln in lines if (":" in ln) and any(tag in ln for tag in resp_tags)]
                resp = "\n".join(resp_lines).strip()
                detail = "\n".join([ln for ln in lines if ln not in resp_lines]).strip()

                rows.append([t[4], ap_mark, na_mark, resp, detail])

        if rows:
            header = ["Ítem", "AP", "NA", "Responsable", "Detalle"]
            return [header] + rows
        return []
    finally:
        doc.close()

def _write_original_sheet(wb: Workbook, images):
    ws = wb.create_sheet("ORIGINAL (Vista)")
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 180
    row = 1
    for idx, img in enumerate(images, start=1):
        ws.cell(row=row, column=1, value=f"Página {idx}").font = Font(bold=True)
        row += 1
        ximg = XLImage(img)
        ws.add_image(ximg, f"A{row}")
        img_h = img.height
        points = img_h * 0.75
        ws.row_dimensions[row].height = min(max(points, 15), 409)
        row += int(max(20, img_h / 18)) + 2
    return ws


def _write_editable_sheet(wb: Workbook, table):
    ws = wb.create_sheet("EDITABLE (Tabla)")
    if not table:
        ws["A1"] = "No se detectó una tabla editable clara en este PDF."
        return ws
    header = table[0]
    body = table[1:] if len(table) > 1 else []
    non_empty = sum(1 for x in header if x)
    if non_empty < max(2, len(header)//3):
        header = [f"Col {i+1}" for i in range(len(header))]
        body = table
    for c, val in enumerate(header, start=1):
        ws.cell(row=1, column=c, value=val)
    for r_idx, row in enumerate(body, start=2):
        for c_idx, val in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=val)
    ncols = len(header)
    _style_header(ws, row=1, ncols=ncols)
    _style_body(ws, start_row=2, end_row=max(2, len(body)+1), ncols=ncols)
    _autosize_cols(ws, ncols=ncols)
    ws.freeze_panes = "A2"
    return ws


@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="Archivo no recibido")
    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Archivo vacío")
    if len(pdf_bytes) > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Archivo supera {MAX_UPLOAD_MB} MB")
    if not (file.filename.lower().endswith(".pdf") or pdf_bytes[:4] == b"%PDF"):
        raise HTTPException(status_code=400, detail="Solo se acepta PDF")

    wb = Workbook()
    wb.remove(wb.active)
    images = _pdf_to_images(pdf_bytes)
    _write_original_sheet(wb, images)
    tables = _extract_tables(pdf_bytes)
    best_table = _choose_best_table(tables)
    if not best_table:
        best_table = _extract_layout_table(pdf_bytes)
    _write_editable_sheet(wb, best_table)
    wb._sheets = [wb["ORIGINAL (Vista)"], wb["EDITABLE (Tabla)"]]

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    base = _safe_filename(file.filename).rsplit(".", 1)[0]
    out_name = f"{base}_ExcelLimpio_PRO.xlsx"

    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'}
    )
