from typing import Any
def format_value(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, (list, tuple)):
        return type(v)(round(x, 4) if isinstance(x, float) else x for x in v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return bytes(v).decode("utf-8", "ignore").rstrip("\x00").strip()
        except Exception:
            return str(v)
    return v
def format_time(ms: float) -> str:
    if ms < 1:
        return f"{ms:.3f} ms"
    elif ms < 1000:
        return f"{ms:.2f} ms"
    else:
        return f"{ms/1000:.2f} s"
def format_record(record) -> dict:
    try:
        names = [n for (n, _, _) in record.value_type_size]
    except (AttributeError, IndexError):
        return {}
    return {col: format_value(getattr(record, col, None)) for col in names}
