"""Draws the charts in the README from bench/results.json, in a light and a
dark version each.

    python3 bench/plot.py
"""

import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("svg")

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator  # noqa: E402

HERE = Path(__file__).resolve().parent
PERCENTILES = [("p50", "p50"), ("p90", "p90"), ("p99", "p99"), ("p999", "p99.9")]
LATENCY_AT = 16


@dataclass
class Theme:
    name: str
    postino: str
    compared: str
    surface: str
    primary: str
    secondary: str
    muted: str
    grid: str
    baseline: str


# Validated against GitHub's page colors, light and dark: Postino in the
# brand's gold, the server it is compared with in a gray that recedes.
THEMES = [
    Theme(
        "light",
        postino="#8a5f17",
        compared="#a9a8a2",
        surface="#ffffff",
        primary="#0b0b0b",
        secondary="#52514e",
        muted="#898781",
        grid="#e1e0d9",
        baseline="#c3c2b7",
    ),
    Theme(
        "dark",
        postino="#bb8a31",
        compared="#62615c",
        surface="#0d1117",
        primary="#f0efec",
        secondary="#c3c2b7",
        muted="#898781",
        grid="#21262d",
        baseline="#30363d",
    ),
]

plt.rcParams.update(
    {
        "font.family": "Helvetica Neue",
        "font.size": 10,
        "svg.fonttype": "path",
        "svg.hashsalt": "postino",
        "axes.linewidth": 0.75,
    }
)


def compact(value):
    return "0" if value == 0 else f"{value / 1000:.0f}K"


def duration(micros):
    if micros < 1000:
        return f"{micros:.0f} µs"
    return f"{micros / 1000:.1f} ms" if micros < 10_000 else f"{micros / 1000:.0f} ms"


def step(micros):
    return f"{micros:g} µs" if micros < 1000 else f"{micros / 1000:g} ms"


# 1, 2, 5, 10, 20, 50, ...: where a logarithmic axis puts its gridlines.
STEPS = [m * 10**k for k in range(0, 8) for m in (1, 2, 5)]


def series(report, op):
    """Each server's rows for `op`, Postino last so its line is drawn on top."""
    names = [n for n in dict.fromkeys(r["server"] for r in report["results"])]
    names.sort(key=lambda n: n == "Postino")
    rows = {}
    for name in names:
        picked = [r for r in report["results"] if r["server"] == name and r["op"] == op]
        rows[name] = sorted(picked, key=lambda r: r["clients"])
    return rows


def label(report, name):
    return f"Postino {report['postino']}" if name == "Postino" else name


def style(ax, theme):
    ax.set_facecolor("none")
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(theme.baseline)
    ax.grid(axis="y", color=theme.grid, linewidth=0.75)
    ax.set_axisbelow(True)
    ax.tick_params(length=0, labelsize=9, labelcolor=theme.muted, pad=6)


def color(theme, name):
    return theme.postino if name == "Postino" else theme.compared


def line(ax, theme, name, xs, ys):
    ax.plot(
        xs,
        ys,
        color=color(theme, name),
        linewidth=1.5,
        solid_capstyle="round",
        solid_joinstyle="round",
        marker="o",
        markersize=6,
        markeredgecolor=theme.surface,
        markeredgewidth=1.5,
        zorder=3 if name == "Postino" else 2,
        clip_on=False,
    )


def spread(wanted, gap):
    """Positions as near `wanted` as they can be, each at least `gap` apart."""
    order = sorted(range(len(wanted)), key=lambda i: wanted[i])
    # Runs of labels that touch, each centred on the mean of what it wants.
    runs = []
    for i in order:
        runs.append([i])
        while len(runs) > 1:
            below, above = runs[-2], runs[-1]
            top = (
                sum(wanted[j] for j in below) / len(below) + gap * (len(below) - 1) / 2
            )
            bottom = (
                sum(wanted[j] for j in above) / len(above) - gap * (len(above) - 1) / 2
            )
            if bottom - top >= gap:
                break
            runs[-2:] = [below + above]
    placed = [0.0] * len(wanted)
    for run in runs:
        centre = sum(wanted[j] for j in run) / len(run)
        for k, j in enumerate(run):
            placed[j] = centre + gap * (k - (len(run) - 1) / 2)
    return placed


def end_labels(ax, theme, ends):
    """Labels the last point of each line with its value, without overlaps.
    Needs the axes' final limits, to know where in the figure each point is."""
    points = [
        ax.transData.transform((x, y))[1] * 72 / ax.figure.dpi for _, x, y in ends
    ]
    for (text, x, y), wanted, placed in zip(ends, points, spread(points, 12)):
        ax.annotate(
            text,
            xy=(x, y),
            xytext=(9, placed - wanted),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=theme.secondary,
        )


def frame(report, theme, title, subtitle):
    fig, axes = plt.subplots(1, 2, figsize=(9.4, 3.9), sharey=True)
    fig.subplots_adjust(left=0.075, right=0.985, top=0.71, bottom=0.15, wspace=0.12)
    fig.patch.set_alpha(0)
    fig.text(0.075, 0.935, title, fontsize=13.5, weight="medium", color=theme.primary)
    fig.text(0.075, 0.865, subtitle, fontsize=9.5, color=theme.secondary)
    names = list(reversed(series(report, "set")))
    handles = [
        Line2D(
            [],
            [],
            color=color(theme, name),
            linewidth=1.5,
            marker="o",
            markersize=6,
            markeredgecolor=theme.surface,
            markeredgewidth=1.5,
        )
        for name in names
    ]
    fig.legend(
        handles,
        [label(report, name) for name in names],
        loc="upper right",
        bbox_to_anchor=(0.985, 0.975),
        ncol=len(names),
        frameon=False,
        fontsize=9.5,
        labelcolor=theme.secondary,
        handlelength=1.8,
        columnspacing=1.6,
    )
    for ax in axes:
        style(ax, theme)
    return fig, axes


def throughput(report, theme):
    fig, axes = frame(
        report,
        theme,
        "Requests per second",
        "More is better. Each client holds one connection and sends one command"
        " at a time.",
    )
    ops = {op: series(report, op) for op in ["set", "get"]}
    top = max(
        r["throughput"] for op in ops.values() for rows in op.values() for r in rows
    )
    step = 50_000
    axes[0].set_ylim(0, (int(top / step) + 1) * step)
    axes[0].yaxis.set_major_locator(FixedLocator(range(0, int(top) + step, step)))
    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: compact(v)))
    for ax, (op, lines) in zip(axes, ops.items()):
        ends = []
        for name, rows in lines.items():
            xs = list(range(len(rows)))
            ys = [r["throughput"] for r in rows]
            line(ax, theme, name, xs, ys)
            ends.append((compact(ys[-1]), xs[-1], ys[-1]))
        end_labels(ax, theme, ends)
        clients = [r["clients"] for r in lines["Postino"]]
        ax.set_xticks(range(len(clients)), [str(c) for c in clients])
        ax.set_xlim(-0.35, len(clients) - 0.25)
        ax.set_title(op.upper(), loc="left", fontsize=10.5, color=theme.primary, pad=10)
        ax.set_xlabel("Concurrent clients", fontsize=9, color=theme.muted, labelpad=8)
    return fig


def latency(report, theme):
    fig, axes = frame(
        report,
        theme,
        f"Latency at {LATENCY_AT} clients",
        "Less is better. From sending a command to the last byte of its reply,"
        " by percentile.",
    )
    ops = {}
    for op in ["set", "get"]:
        ops[op] = {}
        for name, rows in series(report, op).items():
            row = next(r for r in rows if r["clients"] == LATENCY_AT)
            ops[op][name] = [row["latency_us"][key] for key, _ in PERCENTILES]
    every = [y for op in ops.values() for ys in op.values() for y in ys]
    bottom = max(v for v in STEPS if v <= min(every) / 1.25)
    top = min(v for v in STEPS if v >= max(every) * 1.25)
    axes[0].set_yscale("log")
    axes[0].set_ylim(bottom, top)
    axes[0].yaxis.set_major_locator(
        FixedLocator([v for v in STEPS if bottom <= v <= top])
    )
    axes[0].yaxis.set_minor_locator(NullLocator())
    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: step(v)))
    for ax, (op, lines) in zip(axes, ops.items()):
        ends = []
        for name, ys in lines.items():
            xs = list(range(len(ys)))
            line(ax, theme, name, xs, ys)
            ends.append((duration(ys[-1]), xs[-1], ys[-1]))
        end_labels(ax, theme, ends)
        ax.set_xticks(range(len(PERCENTILES)), [text for _, text in PERCENTILES])
        ax.set_xlim(-0.3, len(PERCENTILES) - 0.35)
        ax.set_title(op.upper(), loc="left", fontsize=10.5, color=theme.primary, pad=10)
        ax.set_xlabel("Percentile", fontsize=9, color=theme.muted, labelpad=8)
    return fig


def main():
    report = json.loads((HERE / "results.json").read_text())
    for theme in THEMES:
        for chart, draw in [("throughput", throughput), ("latency", latency)]:
            fig = draw(report, theme)
            path = HERE / f"{chart}-{theme.name}.svg"
            fig.savefig(path, format="svg", transparent=True, metadata={"Date": None})
            plt.close(fig)
            print(f"wrote {path.relative_to(HERE.parent)}")


if __name__ == "__main__":
    main()
