#!/usr/bin/env python3
"""开发期校验脚本：盯住三件事
1. 省份识别是否与预期一致（拆几个省）
2. 压盖面积是否过大／过小（数据摆放是否合理）
3. 跨省后「相加口径」与「并集口径」差异是否合理
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from core import geo, province  # noqa: E402
from core.overlay import overlay_province  # noqa: E402

CASES = [
    # (名称, 多边形, 期望省份列表)
    ("廊坊-压城镇边界",
     [[116.50, 39.30], [116.88, 39.30], [116.88, 39.55], [116.50, 39.55], [116.50, 39.30]],
     ["河北省"]),
    ("北京朝阳-小面", [[116.42, 39.90], [116.52, 39.90], [116.52, 39.98], [116.42, 39.98], [116.42, 39.90]], ["北京市"]),
    ("晋冀交界-太行", [[113.85, 37.50], [114.30, 37.50], [114.30, 38.10], [113.85, 38.10], [113.85, 37.50]],
     ["河北省", "山西省"]),
    ("京冀交界-对照", [[116.50, 39.88], [117.10, 39.88], [117.10, 40.06], [116.50, 40.06], [116.50, 39.88]],
     ["北京市", "河北省"]),
]


def main() -> int:
    failed = 0
    for name, coords, expected in CASES:
        g = geo.parse_geojson_geometry({"type": "Polygon", "coordinates": [coords + [coords[0]]]})
        area = geo.area_m2(g)
        d = province.detect_provinces(g)
        hits = [p for p in d.provinces if p.match == "geom"]
        names = sorted(p.name for p in hits)
        ok = names == sorted(expected)
        print(f"\n=== {name} ===")
        print(f"  面积       : {area / 1e6:.2f} km²  (椭球 {geo.ellipsoid_area_m2(g) / 1e6:.2f})")
        print(f"  识别省份   : {names}  期望 {sorted(expected)}  {'OK' if ok else 'MISMATCH'}")
        if d.warnings:
            print(f"  警告       : {d.warnings}")
        total_sum = 0.0
        for p in hits:
            r = overlay_province(g, p.code, p.name, 50, area)
            total_sum += r["province_overlap_area"]
            print(f"  - {p.name}: 扫描 {r['patches_scanned']} 图斑, 命中 {r['patch_count']} 个, "
                  f"面积 {r['province_overlap_area'] / 1e6:.2f} km²")
            for t in r["by_type"]:
                print(f"      {t['redline_type']}: {t['overlap_area'] / 1e6:.2f} km² "
                      f"({t['overlap_ratio']:.1f}%)")
        print(f"  相加口径   : {total_sum / 1e6:.2f} km² ({total_sum / area * 100:.1f}%)")
        if not ok:
            failed += 1
    return failed


if __name__ == "__main__":
    sys.exit(main())
