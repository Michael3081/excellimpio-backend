from __future__ import annotations

import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pdfplumber
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from openpyxl import Workbook

app = FastAPI(
    title="ExcelLimpio Backend v1",
    description="Convierte PDFs con texto/tablas simples a un Excel estructurado.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATE_RE = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PHONE_RE = re.compile(r"\b(?:\+?\d{1,3}[\s-]?)?(?:\d[\s-]?){7,14}\b")
TOTAL_RE = re.compile(
    r"(?i)\b(?:total|importe total|monto total|total a pagar)\b\s*[:\-]?\s*(?:USD|US\$|S\/\.?|PEN|EUR|€)?\s*([0-9][0-9.,]*)"
)
DOCNUM_RE = re.compile(
    r"(?i)\b(?:n[úu]mero|no\.?|nro\.?|documento|factura|recibo|cotizaci[óo]n|orden)\b\s*[:#-]?\s*([A-Z0-9\-\/]{4,})"
)
CURRENCY_RE = re.compile(r"(?i)\b(?:USD|US\$|S\/\.?|PEN|EUR|€)\b")


@app.get("/")
def health() -> dict[str, str]:
    return {"status": "ok", "message": "ExcelLimpio backend activo"}


@app.post("/convert")
async def convert(file: UploadFile = File(...)) -> FileResponse:
    filename = file.filename or "archivo"
    suffix = Path(filename).suffix.lower()

    if suffix != ".pdf":
        raise HTTPException(
            status_code=415,
            detail="La v1 solo acepta PDF con texto. Las imágenes se agregan en la siguiente fase.",
        )

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="El archivo llegó vacío.")

    pdf_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf_tmp.write(raw)
    pdf_tmp.close()

    try:
        parsed = parse_pdf(Path(pdf_tmp.name), filename)
        xlsx_path = build_excel(parsed, filename)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error procesando el PDF: {exc}") from exc

    download_name = f"{Path(filename).stem}_ExcelLimpio.xlsx"
    return FileResponse(
        path=xlsx_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=download_name,
    )


def parse_pdf(pdf_path: Path, original_name: str) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    tables_rows: list[list[Any]] = []
    text_blocks: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    total_pages = 0
    total_tables = 0
    detected_doc_type = "Documento"
    max_cols = 0

    with pdfplumber.open(str(pdf_path)) as pdf:
        total_pages = len(pdf.pages)
        all_text_joined = []

        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            clean_text = normalize_text(text)
            all_text_joined.append(clean_text)

            if clean_text.strip():
                text_blocks.append(
                    {
                        "bloque": f"Página {page_num}",
                        "contenido": clean_text,
                        "pagina": page_num,
                    }
                )
            else:
                review.append(
                    {
                        "tipo": "texto",
                        "detalle": "Página sin texto extraíble",
                        "pagina": page_num,
                        "observacion": "Puede ser un PDF escaneado o una imagen incrustada.",
                    }
                )

            fields.extend(extract_fields(clean_text, page_num))

            page_tables = page.extract_tables() or []
            for table in page_tables:
                normalized_rows = normalize_table(table)
                if not normalized_rows:
                    continue
                total_tables += 1
                max_cols = max(max_cols, max(len(r) for r in normalized_rows))
                for row_idx, row in enumerate(normalized_rows, start=1):
                    tables_rows.append([f"T{total_tables}", page_num, row_idx, *row])

        detected_doc_type = detect_document_type(" ".join(all_text_joined))

        if total_tables == 0:
            review.append(
                {
                    "tipo": "tablas",
                    "detalle": "No se detectaron tablas",
                    "pagina": "",
                    "observacion": "El archivo puede tener solo texto o tablas como imagen.",
                }
            )

        if not fields:
            review.append(
                {
                    "tipo": "campos",
                    "detalle": "No se detectaron campos clave",
                    "pagina": "",
                    "observacion": "Se extraerá solo texto y tablas disponibles.",
                }
            )

    return {
        "original_name": original_name,
        "detected_doc_type": detected_doc_type,
        "total_pages": total_pages,
        "total_tables": total_tables,
        "fields": fields,
        "tables_rows": tables_rows,
        "text_blocks": text_blocks,
        "review": review,
        "max_cols": max_cols,
    }


def normalize_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def detect_document_type(text: str) -> str:
    lower = text.lower()
    if any(k in lower for k in ["factura", "invoice", "subtotal", "total a pagar"]):
        return "Factura"
    if any(k in lower for k in ["cotización", "cotizacion", "quote", "propuesta"]):
        return "Cotización"
    if any(k in lower for k in ["estado de cuenta", "statement", "saldo anterior"]):
        return "Estado de cuenta"
    if any(k in lower for k in ["formulario", "nombre", "firma", "correo electrónico"]):
        return "Formulario"
    if any(k in lower for k in ["inventario", "stock", "sku", "producto"]):
        return "Lista / Inventario"
    if any(k in lower for k in ["recibo", "receipt"]):
        return "Recibo"
    return "Documento general"


def extract_fields(text: str, page_num: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for match in DATE_RE.finditer(text):
        out.append({"campo": "fecha", "valor": match.group(1), "pagina": page_num, "confianza": "alta"})

    for match in EMAIL_RE.finditer(text):
        out.append({"campo": "email", "valor": match.group(0), "pagina": page_num, "confianza": "alta"})

    for match in PHONE_RE.finditer(text):
        val = match.group(0).strip()
        digits = re.sub(r"\D", "", val)
        if len(digits) >= 7:
            out.append({"campo": "telefono", "valor": val, "pagina": page_num, "confianza": "media"})

    for match in CURRENCY_RE.finditer(text):
        out.append({"campo": "moneda", "valor": match.group(0), "pagina": page_num, "confianza": "media"})

    for match in TOTAL_RE.finditer(text):
        out.append({"campo": "total_detectado", "valor": match.group(1), "pagina": page_num, "confianza": "media"})

    for match in DOCNUM_RE.finditer(text):
        out.append({"campo": "numero_documento", "valor": match.group(1), "pagina": page_num, "confianza": "media"})

    return dedupe_fields(out)


def dedupe_fields(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    unique = []
    for item in items:
        key = (item["campo"], item["valor"], item["pagina"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def normalize_table(table: list[list[Any]]) -> list[list[str]]:
    cleaned: list[list[str]] = []
    for row in table:
        if row is None:
            continue
        cells = []
        for cell in row:
            value = "" if cell is None else str(cell).strip()
            cells.append(value)
        if any(cells):
            cleaned.append(cells)
    return cleaned


def build_excel(parsed: dict[str, Any], original_name: str) -> str:
    wb = Workbook()

    ws_res = wb.active
    ws_res.title = "Resumen"
    ws_res.append(["Campo", "Valor"])
    ws_res.append(["archivo_original", original_name])
    ws_res.append(["tipo_documento", parsed["detected_doc_type"]])
    ws_res.append(["paginas", parsed["total_pages"]])
    ws_res.append(["tablas_detectadas", parsed["total_tables"]])
    ws_res.append(["fecha_procesamiento", datetime.now().isoformat(timespec="seconds")])

    ws_fields = wb.create_sheet("Campos")
    ws_fields.append(["campo", "valor", "pagina", "confianza"])
    for item in parsed["fields"]:
        ws_fields.append([item["campo"], item["valor"], item["pagina"], item["confianza"]])

    ws_tables = wb.create_sheet("Tablas")
    max_cols = max(parsed["max_cols"], 1)
    headers = ["tabla_id", "pagina", "fila"] + [f"columna_{i}" for i in range(1, max_cols + 1)]
    ws_tables.append(headers)
    for row in parsed["tables_rows"]:
        padded = row + [""] * (len(headers) - len(row))
        ws_tables.append(padded[: len(headers)])

    ws_text = wb.create_sheet("Texto")
    ws_text.append(["bloque", "contenido", "pagina"])
    for item in parsed["text_blocks"]:
        ws_text.append([item["bloque"], item["contenido"], item["pagina"]])

    ws_rev = wb.create_sheet("Revision")
    ws_rev.append(["tipo", "detalle", "pagina", "observacion"])
    for item in parsed["review"]:
        ws_rev.append([item["tipo"], item["detalle"], item["pagina"], item["observacion"]])

    for ws in wb.worksheets:
        for col in ws.columns:
            max_length = 0
            col_letter = col[0].column_letter
            for cell in col:
                val = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, min(len(val), 60))
            ws.column_dimensions[col_letter].width = max(14, max_length + 2)

    tmp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp_xlsx.name)
    tmp_xlsx.close()
    return tmp_xlsx.name
