"""WAL 段文件名 / LSN 换算（规则与 pg_walfile_name 一致）。"""


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.strip().split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def wal_file_name(lsn: int, seg_size: int, timeline: int = 1) -> str:
    segs_per_id = 0x100000000 // seg_size
    seg = lsn // seg_size
    return f"{timeline:08X}{seg // segs_per_id:08X}{seg % segs_per_id:08X}"
