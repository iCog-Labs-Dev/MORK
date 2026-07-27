#!/usr/bin/env python3

from pathlib import Path


PEOPLE = 50
FRIEND_OFFSETS = (1, 7, 19)
ROOT = Path(__file__).resolve().parent
SMALL_MODEL = ROOT / "mln_social_network.mm2"
OUTPUT = ROOT / "mln_social_network_big.mm2"


def person(index: int) -> str:
    return f"P{index:03d}"


def smokes(index: int) -> bool:
    return index % 7 == 0


def cancer(index: int) -> bool:
    return index % 11 == 0


def truth(value: bool) -> str:
    return "true" if value else "false"


def main() -> None:
    people = [person(index) for index in range(PEOPLE)]
    friendships = [
        (index, (index + offset) % PEOPLE)
        for index in range(PEOPLE)
        for offset in FRIEND_OFFSETS
    ]
    lines: list[str] = [
        "; Generated serial grounded Boolean MLN for scaling measurements.",
        f"; people={PEOPLE}",
        f"; directed_friendships={len(friendships)}",
        f"; mutable_variables={PEOPLE * 2}",
        f"; grounded_clauses={PEOPLE * 3 + len(friendships)}",
        "; Regenerate with: python3 kernel/resources/generate_mln_social_network_big.py",
        "",
    ]

    for name in people:
        lines.append(f"(mln-person {name})")
    lines.append("")

    for name in people:
        lines.append(f"(mln-var (Smokes {name}))")
        lines.append(f"(mln-var (Cancer {name}))")
    for source, target in friendships:
        lines.append(f"(mln-var (Friends {people[source]} {people[target]}))")
    lines.append("")

    for name in people:
        lines.append(f"(mln-mutable (Smokes {name}))")
        lines.append(f"(mln-mutable (Cancer {name}))")
    for source, target in friendships:
        lines.append(
            f"(mln-evidence (Friends {people[source]} {people[target]}) true)"
        )
    lines.append("")

    for index, name in enumerate(people):
        lines.append(f"(mln-val (Smokes {name}) {truth(smokes(index))})")
        lines.append(f"(mln-val (Cancer {name}) {truth(cancer(index))})")
    for source, target in friendships:
        lines.append(f"(mln-val (Friends {people[source]} {people[target]}) true)")
    lines.append("")

    for index, name in enumerate(people):
        clause = f"health-{name}"
        count = int(not smokes(index)) + int(cancer(index))
        lines.extend(
            [
                f"(mln-clause {clause} 1.5)",
                f"(mln-lit {clause} (Smokes {name}) negative)",
                f"(mln-lit {clause} (Cancer {name}) positive)",
                f"(mln-in-clause (Smokes {name}) {clause} negative)",
                f"(mln-in-clause (Cancer {name}) {clause} positive)",
                f"(mln-sat-count {clause} {count})",
                "",
            ]
        )

    for source, target in friendships:
        source_name = people[source]
        target_name = people[target]
        clause = f"influence-{source_name}-{target_name}"
        count = int(not smokes(source)) + int(smokes(target))
        lines.extend(
            [
                f"(mln-clause {clause} 1.1)",
                f"(mln-lit {clause} (Friends {source_name} {target_name}) negative)",
                f"(mln-lit {clause} (Smokes {source_name}) negative)",
                f"(mln-lit {clause} (Smokes {target_name}) positive)",
                f"(mln-in-clause (Friends {source_name} {target_name}) {clause} negative)",
                f"(mln-in-clause (Smokes {source_name}) {clause} negative)",
                f"(mln-in-clause (Smokes {target_name}) {clause} positive)",
                f"(mln-sat-count {clause} {count})",
                "",
            ]
        )

    for index, name in enumerate(people):
        clause = f"smoke-prior-{name}"
        lines.extend(
            [
                f"(mln-clause {clause} -0.35)",
                f"(mln-lit {clause} (Smokes {name}) positive)",
                f"(mln-in-clause (Smokes {name}) {clause} positive)",
                f"(mln-sat-count {clause} {int(smokes(index))})",
                "",
            ]
        )

    for index, name in enumerate(people):
        clause = f"cancer-prior-{name}"
        lines.extend(
            [
                f"(mln-clause {clause} -0.8)",
                f"(mln-lit {clause} (Cancer {name}) positive)",
                f"(mln-in-clause (Cancer {name}) {clause} positive)",
                f"(mln-sat-count {clause} {int(cancer(index))})",
                "",
            ]
        )

    lines.extend(
        [
            "; Each Smokes site has 8 incident clauses; each Cancer site has 2.",
            "; These concrete values keep weighted selection on the agg_w fast path.",
        ]
    )
    for name in people:
        lines.append(f"(mln-site (Smokes {name}) (# 8))")
        lines.append(f"(mln-site (Cancer {name}) (# 2))")
    lines.extend(["", "(mln-rng 987654321 0)", ""])

    sampler = SMALL_MODEL.read_text(encoding="ascii")
    marker = "(mln-flip false true)"
    sampler = sampler[sampler.index(marker) :]
    sampler = sampler.replace("social-mln", "social-mln-big")
    lines.append(sampler.rstrip())
    lines.append("")

    OUTPUT.write_text("\n".join(lines), encoding="ascii")


if __name__ == "__main__":
    main()
