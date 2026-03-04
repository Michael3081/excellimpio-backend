from __future__ import annotations

import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pdfplumber
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

app = FastAPI(
    title="ExcelLimpio Backend v1",
    description="Convierte PDFs con texto/tablas simples a un Excel estructurado (con limpieza extra para checklists).",
    version="1.1.0",
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
            detail="Esta versión acepta PDF. (OCR/imagenes se agrega después).",
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
    raw_tables: list[dict[str, Any]] = []
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

            # Intento 1: extracción estándar
            page_tables = page.extract_tables() or []
            # Si no encuentra tablas, no forzamos; OCR vendrá después.
            for table in page_tables:
                normalized_rows = normalize_table(table)
                if not normalized_rows:
                    continue
                total_tables += 1
                max_cols = max(max_cols, max(len(r) for r in normalized_rows))
                raw_tables.append({"table_id": f"T{total_tables}", "pagina": page_num, "rows": normalized_rows})

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

    # Limpieza extra para ciertos formatos (ej. checklist)
    clean_outputs: list[dict[str, Any]] = []
    for t in raw_tables:
        cleaned = try_clean_checklist_table(t["rows"])
        if cleaned is not None and len(cleaned) >= 3:
            clean_outputs.append({"table_id": t["table_id"], "pagina": t["pagina"], "kind": "checklist", "rows": cleaned})

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
        "clean_outputs": clean_outputs,
    }


def normalize_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def detect_document_type(text: str) -> str:
    lower = text.lower()
    if any(k in lower for k in ["check list", "checklist", "actividades de cierre"]):
        return "Checklist"
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
        # Mantener filas que tengan algo útil
        if any(cells):
            cleaned.append(cells)
    return cleaned


def try_clean_checklist_table(rows: list[list[str]]) -> Optional[list[dict[str, str]]]:
    """
    Detecta tablas tipo 'check list' y las normaliza a columnas humanas.
    Retorna lista de dicts (filas) o None si no aplica.
    """
    if not rows or len(rows) < 8:
        return None

    norm = [[("" if c is None else str(c)).strip() for c in r] for r in rows]

    header_idx = None
    for i, r in enumerate(norm):
        joined = " ".join(r).lower()
        if "ap" in joined and "na" in joined and "responsable" in joined:
            header_idx = i
            break
    if header_idx is None or header_idx + 2 >= len(norm):
        return None

    header = norm[header_idx]
    sub = norm[header_idx + 1]

    # Indices típicos
    # Item y Actividad casi siempre están al inicio en estas tablas
    item_i = 0
    act_i = 1

    def find_col(label: str) -> Optional[int]:
        label_low = label.lower()
        for idx, val in enumerate(header):
            if (val or "").strip().lower() == label_low:
                return idx
        return None

    ap_i = find_col("AP")
    na_i = find_col("NA")
    resp_i = find_col("Responsable")
    obs_i = None
    for idx, val in enumerate(header):
        if "observ" in (val or "").lower():
            obs_i = idx
            break

    # Emisor puede abarcar 1 o 2 columnas
    emisor_i = None
    for idx, val in enumerate(header):
        if (val or "").strip().lower() == "emisor":
            emisor_i = idx
            break

    nombre_emisor_i = None
    for idx, val in enumerate(header):
        if "nombre" in (val or "").lower() and "emisor" in (val or "").lower():
            nombre_emisor_i = idx
            break

    if ap_i is None or na_i is None or resp_i is None or obs_i is None:
        return None

    # Construir columnas
    cols: list[tuple[str, int]] = [
        ("Item", item_i),
        ("Actividad", act_i),
        ("AP", ap_i),
        ("NA", na_i),
        ("Responsable", resp_i),
    ]

    # Emisor: si hay subheaders, usar dos columnas
    if emisor_i is not None:
        # Buscar si el subheader trae 'Proyectos'/'Operaciones' en un rango cercano
        emisor_cols = []
        for j in range(emisor_i, min(emisor_i + 3, len(sub))):
            if sub[j].strip():
                emisor_cols.append((f"Emisor ({sub[j].strip()})", j))
        if emisor_cols:
            cols.extend(emisor_cols)
        else:
            cols.append(("Emisor", emisor_i))

    cols.append(("Observaciones", obs_i))
    if nombre_emisor_i is not None:
        cols.append(("Nombre emisor", nombre_emisor_i))

    # Filas data: generalmente después del subheader
    data = norm[header_idx + 2 :]
    out: list[dict[str, str]] = []
    for r in data:
        row = {}
        for name, idx in cols:
            row[name] = r[idx].strip() if idx < len(r) else ""
        if not row.get("Item") and not row.get("Actividad"):
            continue
        # Normalizar marcas
        if "AP" in row:
            row["AP"] = row["AP"].replace("x", "X").strip()
        if "NA" in row:
            row["NA"] = row["NA"].replace("x", "X").strip()
        out.append(row)

    # Si no encontró suficientes filas, no aplicar
    return out if len(out) >= 5 else None


def build_excel(parsed: dict[str, Any], original_name: str) -> str:
    wb = Workbook()

    # Si detectamos una salida limpia (checklist), ponemos una hoja principal más “vendible”
    clean_outputs = parsed.get("clean_outputs") or []
    if clean_outputs:
        ws_main = wb.active
        ws_main.title = "ExcelLimpio"

        ws_main.append(["ExcelLimpio Pro - Tabla limpia"])
        ws_main.append([f"Archivo: {original_name}"])
        ws_main.append([f"Generado: {datetime.now().isoformat(timespec='seconds')}"])
        ws_main.append([])

        first = clean_outputs[0]
        rows = first["rows"]
        headers = list(rows[0].keys())
        ws_main.append(headers)
        for hcell in ws_main[ws_main.max_row]:
            hcell.font = Font(bold=True)
            hcell.alignment = Alignment(wrap_text=True, vertical="top")

        for r in rows:
            ws_main.append([r.get(h, "") for h in headers])

        ws_main.freeze_panes = "A6"

        # Wrap & widths
        for row in ws_main.iter_rows(min_row=5, max_row=ws_main.max_row):
            for cell in row:
                cell.alignment = Alignment(wrap_text=True, vertical="top")

        for col in ws_main.columns:
            col_letter = col[0].column_letter
            max_len = 0
            for cell in col:
                if cell.value is None:
                    continue
                max_len = max(max_len, min(len(str(cell.value)), 60))
            ws_main.column_dimensions[col_letter].width = max(12, max_len + 2)
    else:
        ws_res = wb.active
        ws_res.title = "Resumen"
        ws_res.append(["Campo", "Valor"])
        ws_res.append(["archivo_original", original_name])
        ws_res.append(["tipo_documento", parsed["detected_doc_type"]])
        ws_res.append(["paginas", parsed["total_pages"]])
        ws_res.append(["tablas_detectadas", parsed["total_tables"]])
        ws_res.append(["fecha_procesamiento", datetime.now().isoformat(timespec="seconds")])

    # Siempre dejamos “auditoría” (útil para debugging)
    ws_resumen = wb["ExcelLimpio"] if "ExcelLimpio" in wb.sheetnames else wb["Resumen"]

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

    # Ajuste de anchos básico
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
