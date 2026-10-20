#!/usr/bin/env python3
"""校准脚本：输入若干个面，打印它真正落在各省的面积（用作测试期望值）。

    /usr/bin/python3 scripts/calibrate.py
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from core import geo, province  # noqa: E402

CASES = {
    "A_廊坊大城_压城镇边界": [[116.55, 39.30], [116.80, 39.30], [116.80, 39.45], [116.55, 39.45]],
    "B_朝阳区小面": [[116.42, 39.90], [116.52, 39.90], [116.52, 39.98], [116.42, 39.98]],
    "C_北京东南耕地": [[116.50, 39.72], [116.80, 39.72], [116.80, 39.95], [116.50, 39.95]],
    "D_北京东部山区_跨省": [[116.85, 40.50], [117.20, 40.50], [117.20, 40.75], [116.85, 40.75]],
    "E_河北南部空白": [[117.10, 37.00], [117.35, 37.00], [117.35, 37.25], [117.10, 37.25]],
    "F_晋冀太行": [[113.90, 37.60], [114.25, 37.60], [114.25, 38.00], [113.90, 38.00]],
    "G_天津北部_包围盒跨省": [[117.20, 39.90], [117.55, 39.90], [117.55, 40.05], [117.20, 40.05]],
    "H_雄安": [[115.80, 38.90], [116.15, 38.90], [116.15, 39.12], [115.80, 39.12]],
    "I_华北多省": [[114.20, 36.20], [117.40, 36.20], [117.40, 40.40], [114.20, 40.40]],
}


def main() -> None:
    for name, coords in CASES.items():
        g = geo.parse_geojson_geometry({"type": "Polygon", "coordinates": [coords + [coords[0]]]})
        area = geo.area_m2(g)
        print(f"\n{name}: 总面积 {area / 1e6:.2f} km²")
        parts = []
        for p in province.load_provinces():
            i = g.Intersection(p["geom"])
            if not i.IsEmpty():
                a = geo.area_m2(i)
                if a > 1000:
                    parts.append(f"{p['name']} {a / 1e6:.2f}({a / area * 100:.1f}%)")
        print("  " + " | ".join(parts) if parts else "  （无）")


if __name__ == "__main__":
    main()
