S7_TCODE_MAP = {
    0x0000: "OK",
    0x0005: "Address error",
    0x0006: "Data type mismatch",
    0x0007: "Data length mismatch",
    0x00A0: "Memory area not available / out of range",
    0x00B0: "Access denied / write protected",
    0x00C0: "Data type not supported",
    0x00D0: "Object not accessible",
    0x8104: "Invalid transport size",
    0x8204: "Length error",
    0x8304: "Write protection error",
    0x8404: "PLC timeout or communication failure",
}

S7_ECODE_MAP = {
    0x0000: "No error",
    0x0001: "Hardware fault",
    0x0003: "Item not available",
    0x0005: "Address error",
    0x0006: "Data type conflict",
    0x0007: "Length mismatch",
    0x000A: "Write protection",
}


def parse_s7_result(result: int) -> str:
    tcode = (result >> 16) & 0xFFFF
    ecode = result & 0xFFFF
    tcode_desc = S7_TCODE_MAP.get(tcode, f"Unknown TCode 0x{tcode:04X}")
    ecode_desc = S7_ECODE_MAP.get(ecode, f"Unknown ECode 0x{ecode:04X}")
    return f"Result=[0x{result:08X}], TCode=[0x{tcode:04X}|{tcode_desc}], ECode=[0x{ecode:04X}|{ecode_desc}]"
