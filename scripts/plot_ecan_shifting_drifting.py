#!/usr/bin/env python3
"""Render the recorded ECAN trace as a dependency-free SVG."""

import argparse
import csv
import html
from collections import Counter, defaultdict
from pathlib import Path

WIDTH, HEIGHT = 1200, 930
LEFT, RIGHT = 90, 30
PLOT_WIDTH = WIDTH - LEFT - RIGHT
COLORS = {
    "insect": "#0072B2",
    "bridge": "#E69F00",
    "poison": "#CC79A7",
    "noise": "#009E73",
}


def load_trace(path: Path):
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise SystemExit("trace is empty")
    return rows


def line(points, color, width=3):
    coords = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    return f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="{width}"/>'


def panel(parts, top, height, title, y_label, rounds, series, y_max, phase_starts):
    bottom = top + height
    x = lambda value: LEFT + value / max(rounds) * PLOT_WIDTH
    y = lambda value: bottom - value / y_max * height
    parts.append(f'<text x="{LEFT}" y="{top-18}" class="title">{html.escape(title)}</text>')
    parts.append(f'<rect x="{LEFT}" y="{top}" width="{PLOT_WIDTH}" height="{height}" class="frame"/>')
    for tick in range(5):
        value = y_max * tick / 4
        yy = y(value)
        parts.append(f'<line x1="{LEFT}" y1="{yy}" x2="{WIDTH-RIGHT}" y2="{yy}" class="grid"/>')
        parts.append(f'<text x="{LEFT-12}" y="{yy+4}" text-anchor="end" class="tick">{value:g}</text>')
    for value in range(0, max(rounds) + 1, 5):
        xx = x(value)
        parts.append(f'<text x="{xx}" y="{bottom+22}" text-anchor="middle" class="tick">{value}</text>')
    for value, label in phase_starts:
        xx = x(value)
        parts.append(f'<line x1="{xx}" y1="{top}" x2="{xx}" y2="{bottom}" class="phase"/>')
        parts.append(f'<text x="{xx+5}" y="{top+16}" class="phase-label">{label}</text>')
    for index, (label, values, color) in enumerate(series):
        panel_points = [(x(round_), y(values[round_])) for round_ in rounds]
        parts.append(line(panel_points, color))
        last_x, last_y = panel_points[-1]
        parts.append(f'<circle cx="{last_x}" cy="{last_y}" r="4" fill="{color}"/>')
        label_y = last_y + (index - (len(series) - 1) / 2) * 14 - 3
        parts.append(f'<text x="{last_x-8}" y="{label_y}" text-anchor="end" fill="{color}" class="series-label">{html.escape(label)}</text>')
    parts.append(f'<text x="{LEFT + PLOT_WIDTH/2}" y="{bottom+42}" text-anchor="middle" class="axis">round</text>')
    parts.append(f'<text transform="translate(24 {top+height/2}) rotate(-90)" text-anchor="middle" class="axis">{html.escape(y_label)}</text>')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", type=Path)
    parser.add_argument("--output", type=Path, default=Path("artifacts/ecan-shifting-drifting.svg"))
    args = parser.parse_args()
    rows = load_trace(args.trace)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    rounds = sorted({int(row["round"]) for row in rows})
    af_counts = defaultdict(Counter)
    sti = defaultdict(dict)
    banks = {}
    totals = {}
    for row in rows:
        round_ = int(row["round"])
        if row["in_af"] == "1":
            af_counts[round_][row["category"]] += 1
        sti[row["atom"]][round_] = float(row["sti"])
        banks[round_] = float(row["bank_sti"])
        totals[round_] = float(row["total_sti_plus_bank"])

    af_fraction = {
        category: {round_: af_counts[round_][category] / 12 for round_ in rounds}
        for category in COLORS
    }
    representatives = [
        ("spider · insect", sti["spider"], COLORS["insect"]),
        ("abamectin · bridge", sti["abamectin"], COLORS["bridge"]),
        ("aconite · poison", sti["aconite"], COLORS["poison"]),
        ("noise-001 · noise", sti["noise-001"], COLORS["noise"]),
    ]
    phase_starts = [(1, "insect input"), (11, "poison input"), (19, "settling")]

    parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="title desc">
<title id="title">ECAN shifting and drifting experiment</title>
<desc id="desc">Attentional-focus category fractions, representative STI trajectories, and conserved STI plus bank totals over 23 rounds.</desc>
<style>
text {{ font-family: system-ui, sans-serif; fill: #222; }}
.heading {{ font-size: 24px; font-weight: 600; }} .title {{ font-size: 17px; font-weight: 600; }}
.tick,.phase-label,.series-label {{ font-size: 12px; }} .axis {{ font-size: 13px; }}
.frame {{ fill: #fff; stroke: #777; }} .grid {{ stroke: #ddd; stroke-width: 1; }}
.phase {{ stroke: #777; stroke-dasharray: 5 5; }} .phase-label {{ fill: #555; }}
</style>
<rect width="100%" height="100%" fill="#fff"/>
<text x="{LEFT}" y="38" class="heading">ECAN shifting and drifting</text>''']
    panel(parts, 85, 220, "Attentional-focus composition", "fraction of AF", rounds,
          [(name, af_fraction[name], color) for name, color in COLORS.items()], 1.0, phase_starts)
    max_sti = max(max(values.values()) for _, values, _ in representatives) * 1.08
    panel(parts, 385, 220, "Representative STI trajectories", "STI", rounds,
          representatives, max_sti, phase_starts)
    conservation_series = [
        ("Bank STI", banks, COLORS["insect"]),
        ("Total STI + bank", totals, COLORS["poison"]),
    ]
    panel(parts, 685, 170, "STI bank and conservation", "STI", rounds,
          conservation_series, max(totals.values()) * 1.01, phase_starts)
    parts.append('</svg>')
    args.output.write_text("\n".join(parts))
    print(args.output)


if __name__ == "__main__":
    main()
