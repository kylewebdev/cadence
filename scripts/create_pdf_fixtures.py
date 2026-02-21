"""Create minimal PDF fixtures for parser tests.

Run from the project root:
    python scripts/create_pdf_fixtures.py
"""
from pathlib import Path


def build_hello_world_pdf() -> bytes:
    """Build a minimal valid PDF with 'Hello World' text extractable by pdfplumber."""
    parts: list[bytes] = []
    offsets: dict[int, int] = {}

    def add(obj_num: int, data: bytes) -> None:
        offsets[obj_num] = sum(len(p) for p in parts)
        parts.append(f"{obj_num} 0 obj\n".encode())
        parts.append(data)
        parts.append(b"\nendobj\n")

    parts.append(b"%PDF-1.4\n")

    add(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    add(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add(
        3,
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
    )

    stream = b"BT /F1 12 Tf 100 700 Td (Hello World) Tj ET"
    add(
        4,
        f"<< /Length {len(stream)} >>\nstream\n".encode()
        + stream
        + b"\nendstream",
    )
    add(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    xref_start = sum(len(p) for p in parts)
    parts.append(b"xref\n")
    parts.append(b"0 6\n")
    parts.append(b"0000000000 65535 f \n")
    for i in [1, 2, 3, 4, 5]:
        parts.append(f"{offsets[i]:010d} 00000 n \n".encode())

    parts.append(b"trailer\n")
    parts.append(b"<< /Size 6 /Root 1 0 R >>\n")
    parts.append(b"startxref\n")
    parts.append(f"{xref_start}\n".encode())
    parts.append(b"%%EOF\n")

    return b"".join(parts)


if __name__ == "__main__":
    out_dir = Path("tests/fixtures/pdf")
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_bytes = build_hello_world_pdf()
    out_path = out_dir / "text_pdf.pdf"
    out_path.write_bytes(pdf_bytes)
    print(f"Created {out_path} ({len(pdf_bytes)} bytes)")
