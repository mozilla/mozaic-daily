"""Build the on-disk artifacts for an ingested headwind/tailwind curve.

The write half of the ingest step. Given a confirmed :class:`IngestPlan` it produces,
under ``data-official/{cycle}/{name}/``: a byte-for-byte copy of the delivered file in
``source_data/``, the horizon-spanning curve parquet the pipeline loads, its sidecar
meta, the spec JSON, a shape plot under ``plots/``, an ``_index.md`` skeleton for the
skill to finish, plus the registry entry in ``adjustment_codes.yaml`` and the ``.gitignore`` exception that
keeps the parquet tracked. It never runs the model.

Two families share this path and differ only in where the spec goes:

- ``per_tile_overlay`` — ``{name}/{name}.json`` (a ``desktop_overlay`` spec), gated on
  ``applies_to_forecast_start``. Needs a model re-run.
- ``display_layer`` — ``adjustments/{name}.json`` (a ``daily_file`` spec pointing back
  at ``../{name}/``), live by presence. No re-run.

Horizon rule (agreed 2026-09-04): zero before the file starts, the file's daily values
verbatim, then held flat at the final 28-day mean from the day after the file ends
through 31 December of the year after the seam. A file must already reach 31 December
of the seam year; that is checked upstream by ``ingest_inspect``.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import textwrap
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from .adjustments import load_code_registry, write_meta
from .ingest_inspect import coerce_numeric, normalize_type_labels

MA_WINDOW_DAYS = 28
REGISTRY_WRAP_WIDTH = 92  # folded-scalar line width in adjustment_codes.yaml
PER_TILE_OVERLAY = "per_tile_overlay"
DISPLAY_LAYER = "display_layer"
FAMILIES = (PER_TILE_OVERLAY, DISPLAY_LAYER)
ALLOCATIONS = ("trailing_dau_share", "fixed_country_shares")
SEGMENT_FLAG_BY_DATA_SOURCE = {"legacy_desktop": "modern_windows", "glean_desktop": "modern_windows", "glean_mobile": "fenix_android"}


@dataclass
class IngestPlan:
    """Everything the user confirmed. Built by the CLI, consumed by :func:`build`."""

    source_path: str
    name: str
    code: str
    family: str
    platform: str                 # "desktop" | "mobile"
    data_source: str              # e.g. "legacy_desktop"; ignored for display_layer
    forecast_start: str           # the seam, YYYY-MM-DD
    cycle: str                    # YYYY-MM
    date_column: str
    value_column: str
    type_column: Optional[str] = None
    actuals_through: Optional[str] = None  # required when type_column is None
    ma_column: Optional[str] = None        # validated, never used
    sheet: Optional[str] = None
    sign: int = 1                          # -1 flips the file's sign (e.g. a headwind delivered as positive numbers)
    allocation: str = "trailing_dau_share"
    shares: Optional[dict] = None          # fixed_country_shares only
    exclude_countries: list[str] = field(default_factory=lambda: ["IR"])
    flag_column: Optional[str] = None      # defaults from data_source
    description: str = ""
    notes: str = ""
    replace: bool = False                  # required to overwrite an existing spec for this code
    root: Optional[str] = None             # repo root override (tests)
    values_are_28d_ma: bool = False        # display_layer only: the file already carries the 28-day series
    rebase_to_seam: bool = False           # shift the curve so it is 0 at forecast_start (pre-seam effect assumed in the base model)
    dir_name: Optional[str] = None         # directory under the cycle; defaults to name, or to the registered layout
    spec_filename: Optional[str] = None    # spec JSON filename; defaults to <name>.json, or to the registered layout

    def __post_init__(self):
        if self.family not in FAMILIES:
            raise ValueError(f"family must be one of {FAMILIES}, got {self.family!r}")
        if not re.fullmatch(r"[a-z]", self.code):
            raise ValueError(f"code must be a single lowercase letter, got {self.code!r}")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.name):
            raise ValueError(f"name must be snake_case, got {self.name!r}")
        if self.platform not in ("desktop", "mobile"):
            raise ValueError(f"platform must be desktop or mobile, got {self.platform!r}")
        if self.sign not in (1, -1):
            raise ValueError("sign must be 1 or -1")
        if self.allocation not in ALLOCATIONS:
            raise ValueError(f"allocation must be one of {ALLOCATIONS}")
        if self.allocation == "fixed_country_shares" and not self.shares:
            raise ValueError("fixed_country_shares needs a shares dict")
        if self.type_column is None and self.actuals_through is None:
            raise ValueError("either type_column or actuals_through is required")
        if self.flag_column is None:
            self.flag_column = SEGMENT_FLAG_BY_DATA_SOURCE.get(self.data_source, "modern_windows")
        if self.dir_name is None or self.spec_filename is None:
            registered = registered_layout(self.code, self.repo_root)
            self.dir_name = self.dir_name or (registered[0] if registered else self.name)
            self.spec_filename = self.spec_filename or (registered[1] if registered else f"{self.name}.json")

    @property
    def repo_root(self) -> Path:
        return Path(self.root) if self.root else Path(__file__).resolve().parents[2]

    @property
    def cycle_dir(self) -> Path:
        return self.repo_root / "data-official" / self.cycle

    @property
    def curve_dir(self) -> Path:
        # Display-layer specs live in adjustments/ (live by presence), but their curve, source and
        # plot get the code's own directory so adjustments/ stays a directory of specs only.
        if self.family == DISPLAY_LAYER:
            return self.cycle_dir / self.name
        return self.cycle_dir / self.dir_name

    @property
    def spec_path(self) -> Path:
        if self.family == DISPLAY_LAYER:
            return self.cycle_dir / "adjustments" / self.spec_filename
        return self.curve_dir / self.spec_filename

    @property
    def spec_glob(self) -> str:
        if self.family == DISPLAY_LAYER:
            return f"data-official/*/adjustments/{self.spec_filename}"
        return f"data-official/*/{self.dir_name}/{self.spec_filename}"

    @property
    def horizon(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        seam = pd.Timestamp(self.forecast_start)
        return pd.Timestamp(year=seam.year, month=1, day=1), pd.Timestamp(year=seam.year + 1, month=12, day=31)


def registered_layout(code: str, root: Path) -> Optional[tuple[str, str]]:
    """``(dir_name, spec_filename)`` an already-registered code uses, read off its ``spec_glob``.

    Older codes predate the ``<name>/<name>.json`` convention (``o`` is ``mozillaonline_migration``
    but lives in ``mozillaonline/mozillaonline.json``), so an update must follow the glob the
    dispatcher actually searches, not the registry name. When ``spec_glob`` is a list the first
    pattern is the current layout. ``None`` for an unregistered code.
    """
    registry_path = root / "data-official" / "adjustment_codes.yaml"
    if not registry_path.exists():
        return None
    entry = load_code_registry(registry_path).get(code)
    if entry is None or "spec_glob" not in entry:
        return None
    globs = entry["spec_glob"]
    current = globs if isinstance(globs, str) else globs[0]  # first pattern is the current layout
    parts = current.split("/")
    if len(parts) < 4 or parts[0] != "data-official":
        raise ValueError(f"unexpected spec_glob for code {code!r}: {entry['spec_glob']!r}")
    return parts[-2], parts[-1]


# --- curve -----------------------------------------------------------------------------

def normalize_curve(frame: pd.DataFrame, plan: IngestPlan) -> pd.DataFrame:
    """The delivered rows as a clean daily frame: DatetimeIndex, ``dau`` float, ``type``."""
    dates = pd.DatetimeIndex(pd.to_datetime(frame[plan.date_column], errors="coerce")).normalize()
    values = coerce_numeric(frame[plan.value_column]).to_numpy()
    if plan.type_column:
        types = normalize_type_labels(frame[plan.type_column]).to_numpy()
    else:
        types = pd.Series(pd.Timestamp(plan.actuals_through) >= dates).map({True: "actuals", False: "forecast"}).to_numpy()
    curve = pd.DataFrame({"dau": values, "type": types}, index=dates).dropna(subset=["dau"]).sort_index()
    curve = curve[~curve.index.isna()]
    if curve.index.has_duplicates:
        raise ValueError("duplicate dates in the delivered file")
    full = pd.date_range(curve.index.min(), curve.index.max(), freq="D")
    if len(full) != len(curve):
        curve = curve.reindex(full)
        curve["dau"] = curve["dau"].interpolate(method="linear")
        curve["type"] = curve["type"].ffill()
    curve["dau"] = curve["dau"].astype(float) * plan.sign
    curve.index.name = "target_date"
    return curve


def build_horizon_curve(curve: pd.DataFrame, plan: IngestPlan) -> tuple[pd.DataFrame, dict]:
    """Zero before the file, verbatim inside it, held flat at the final 28d mean after it."""
    horizon_start, horizon_end = plan.horizon
    index = pd.date_range(horizon_start, horizon_end, freq="D", name="target_date")
    daily = pd.Series(0.0, index=index)
    source = pd.Series("pre-onset", index=index, dtype="object")

    covered = curve.index.intersection(index)
    daily.loc[covered] = curve.loc[covered, "dau"]
    source.loc[covered] = curve.loc[covered, "type"].map({"actuals": "measured", "forecast": "projected"})

    delivered = daily.copy()
    rebase_offset = None
    if plan.rebase_to_seam:
        # The part of the effect accrued before the seam is taken to be inside the model's own
        # fit of the actuals, so only the increment from the seam on is applied.
        seam = pd.Timestamp(plan.forecast_start)
        if seam not in curve.index:
            raise ValueError(f"--rebase-to-seam needs the file to cover the seam {seam.date()}")
        rebase_offset = float(curve.loc[seam, "dau"])
        daily = (daily - rebase_offset).where(index >= seam, 0.0)
        source.loc[index < seam] = "pre-seam (in base model)"

    last_delivered = curve.index.max()
    in_file = daily[index <= last_delivered]
    hold_value = float(in_file.iloc[-1]) if plan.values_are_28d_ma else float(in_file.tail(MA_WINDOW_DAYS).mean())
    tail = index > last_delivered
    daily.loc[tail] = hold_value
    delivered.loc[tail] = hold_value + (rebase_offset or 0.0)
    source.loc[tail] = "held"

    ma = daily.copy() if plan.values_are_28d_ma else daily.rolling(MA_WINDOW_DAYS, min_periods=1).mean()
    out = pd.DataFrame({f"{plan.name}_dau_daily": daily, f"{plan.name}_dau_ma": ma, "source": source})
    if plan.rebase_to_seam:
        out[f"{plan.name}_dau_delivered"] = delivered  # the producer's series before the shift, for the record
    measured = curve.index[curve["type"] == "actuals"]
    summary = {
        "delivered_start": str(curve.index.min().date()),
        "delivered_end": str(last_delivered.date()),
        "actuals_through": str(measured.max().date()) if len(measured) else None,
        "hold_flat_from": str((last_delivered + pd.Timedelta(days=1)).date()) if tail.any() else None,
        "hold_flat_value": hold_value if tail.any() else None,
        "hold_flat_basis": ("final delivered value (file is already a 28-day series)" if plan.values_are_28d_ma
                            else f"mean of the final {MA_WINDOW_DAYS} delivered daily values"),
        "horizon": [str(horizon_start.date()), str(horizon_end.date())],
        "sign_applied": plan.sign,
        "rebase_to_seam": plan.rebase_to_seam,
        "rebase_offset_at_seam": rebase_offset,
    }
    dec15 = pd.Timestamp(year=pd.Timestamp(plan.forecast_start).year, month=12, day=15)
    if dec15 in out.index:
        summary["dec15_daily"] = float(out.loc[dec15, f"{plan.name}_dau_daily"])
        summary["dec15_ma28"] = float(out.loc[dec15, f"{plan.name}_dau_ma"])
    return out, summary


# --- specs ---------------------------------------------------------------------------------

def overlay_spec(plan: IngestPlan, data_file: str, meta_file: str) -> dict:
    allocation = {"key": plan.allocation, "flag_column": plan.flag_column, "window_days": MA_WINDOW_DAYS}
    if plan.allocation == "fixed_country_shares":
        allocation["shares"] = dict(plan.shares)
    return {
        "type": "desktop_overlay",
        "platform": plan.platform,
        "data_file": data_file,
        "value_column": f"{plan.name}_dau_daily",
        "allocation": allocation,
        "scope": {"exclude_countries": list(plan.exclude_countries)},
        "model_meta_file": meta_file,
        "applies_to_forecast_start": plan.forecast_start,
        "applies_to_data_source": plan.data_source,
        "placeholder": False,
        "notes": plan.notes,
    }


def display_spec(plan: IngestPlan, data_file_relative: str) -> dict:
    return {
        "type": "daily_file",
        "platform": plan.platform,
        "data_file": data_file_relative,
        "value_column": f"{plan.name}_dau_daily",
        "values_are_28d_ma": plan.values_are_28d_ma,
        "adjustment_code": plan.code,
        "notes": plan.notes,
    }


# --- registry, gitignore, index -----------------------------------------------------------

def registry_entry_text(plan: IngestPlan) -> str:
    description = plan.description.strip() or f"{plan.name} ({plan.family}) ingested {date.today().isoformat()}."
    wrapped = "\n".join(textwrap.fill(paragraph, width=REGISTRY_WRAP_WIDTH) for paragraph in description.splitlines())
    indented = "\n".join(f"      {line}" for line in wrapped.splitlines())
    return (f"  {plan.code}:\n    name: {plan.name}\n    applier: {plan.family}\n"
            f"    description: >\n{indented}\n    spec_glob: \"{plan.spec_glob}\"\n")


def registry_status(plan: IngestPlan) -> str:
    """``new`` (code free), ``same`` (code already registered to this name) or raises on a collision."""
    registry = load_code_registry(plan.repo_root / "data-official" / "adjustment_codes.yaml")
    entry = registry.get(plan.code)
    if entry is None:
        taken_by_name = [c for c, e in registry.items() if e.get("name") == plan.name]
        if taken_by_name:
            raise ValueError(f"name {plan.name!r} is already registered under code {taken_by_name[0]!r}")
        return "new"
    if entry.get("name") != plan.name:
        raise ValueError(f"code {plan.code!r} is already registered as {entry.get('name')!r}")
    return "same"


def append_registry_entry(plan: IngestPlan) -> None:
    path = plan.repo_root / "data-official" / "adjustment_codes.yaml"
    text = path.read_text()
    if not text.endswith("\n"):
        text += "\n"
    path.write_text(text + registry_entry_text(plan))


def ensure_gitignore_exceptions(plan: IngestPlan) -> list[str]:
    """Track the curve parquet and the delivered source file despite the global ignores."""
    path = plan.repo_root / ".gitignore"
    lines = path.read_text().splitlines() if path.exists() else []
    curve_dir_name = plan.curve_dir.name  # the directory the parquet and source actually land in
    wanted = [f"!data-official/*/{curve_dir_name}/*.parquet", f"!data-official/*/{curve_dir_name}/source_data/*"]
    added = [w for w in wanted if w not in lines]
    if added:
        block = [f"# {curve_dir_name} (`{plan.code}`): curve parquet + delivered source, ingested {date.today().isoformat()}"] + added
        path.write_text("\n".join(lines + [""] + block) + "\n")
    return added


def render_index_md(plan: IngestPlan, summary: dict, files: dict) -> str:
    dec15 = summary.get("dec15_ma28")
    dec15_text = f"{dec15:,.0f}" if dec15 is not None else "n/a"
    family_text = ("per-tile overlay: subtracted from training rows before mozaic and added back after; "
                   "**needs a model re-run**" if plan.family == PER_TILE_OVERLAY
                   else "display layer: its trailing 28-day mean is summed onto the published 28d MA; **no model re-run**, "
                        "live by presence in `../adjustments/`")
    return f"""# `{plan.code}` — {plan.name}, cycle {plan.cycle}

<!-- Drafted by scripts/ingest_adjustment.py on {date.today().isoformat()}. Fill in the WHAT/WHY sections. -->

**What it is:** _one paragraph: the real-world effect, who measured or modelled it, and why it belongs in the forecast._

**Family:** {family_text}. **Platform:** {plan.platform} (`{plan.data_source}`). **Sign:** {"tailwind (+)" if summary["sign_applied"] * (1 if dec15 is None or dec15 >= 0 else -1) > 0 else "headwind (−)"}.

## Files

| file | role |
|---|---|
| `{Path(files["spec"]).relative_to(plan.cycle_dir)}` | the spec, gated on `applies_to_forecast_start: {plan.forecast_start}` |
| `{Path(files["parquet"]).name}` | what the pipeline loads: `{plan.name}_dau_daily` on a `target_date` DatetimeIndex, `{plan.name}_dau_ma`, `source` |
| `{Path(files["meta"]).name}` | provenance: source sha1, column mapping, coverage, hold-flat rule, checks |
| `source_data/{Path(files["source_copy"]).name}` | the delivered file, byte for byte |
| `plots/{Path(files["plot"]).name}` | the curve's shape: daily + 28d mean, measured / projected / held, seam and Dec-15 marked |

## Coverage

| | |
|---|---|
| delivered | {summary["delivered_start"]} → {summary["delivered_end"]} |
| actuals through | {summary["actuals_through"] or "not flagged"} |
| held flat from | {f'{summary["hold_flat_from"]} at {summary["hold_flat_value"]:,.0f}/day ({summary["hold_flat_basis"]})' if summary["hold_flat_from"] else "not needed — the delivered file already reaches the horizon"} |
| horizon | {summary["horizon"][0]} → {summary["horizon"][1]} |
| Dec-15 28d MA | {dec15_text} |

## Allocation

{"Proportional to population: `trailing_dau_share` over the last 28 days of `" + plan.flag_column + "` DAU" if plan.allocation == "trailing_dau_share" else "Localized: fixed country shares " + json.dumps(plan.shares)}; excluded: {plan.exclude_countries or "none"}.

## What is measured and what is assumed

_Fill in: which part of the curve is telemetry, which is a model, which is a planning choice._

## Where new files go

A refreshed curve for this cycle: re-run the ingest with `--replace`; the previous build moves to `{plan.name}_REVERT_<date>/`. Cross-cycle analysis of this effect goes to `research/`.
"""


# --- plot -----------------------------------------------------------------------------------

# Slots 1 and 2 of the validated categorical palette (dataviz skill, light surface).
PLOT_DAILY_COLOR = "#2a78d6"
PLOT_MA_COLOR = "#eb6834"
PLOT_SURFACE = "#fcfcfb"
PLOT_REGION_SHADES = {"pre-seam (in base model)": "#dcdbd6", "measured": "#e6e5e1", "projected": "#f3f2ee", "held": "#faf9f6"}


def render_curve_plot(horizon: pd.DataFrame, plan: IngestPlan, summary: dict, out_path: Path) -> Path:
    """One picture of the curve's shape: daily values, trailing 28d mean, and which stretch is
    measured / projected / held flat, with the seam and Dec-15 marked. Opened during validation
    so a wrong sign, a moving average passed off as daily, or a cliff at the file's end is seen
    before the model runs.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    daily = horizon[f"{plan.name}_dau_daily"]
    ma = horizon[f"{plan.name}_dau_ma"]
    source = horizon["source"]
    delivered_column = f"{plan.name}_dau_delivered"
    delivered = horizon[delivered_column] if delivered_column in horizon.columns else None
    reference = delivered if delivered is not None else daily
    first_nonzero = reference[reference != 0].index.min()
    plot_from = min(first_nonzero, pd.Timestamp(plan.forecast_start)) - pd.Timedelta(days=14) if pd.notna(first_nonzero) else horizon.index.min()
    shown = horizon.index >= plot_from

    fig, ax = plt.subplots(figsize=(12, 5.5), facecolor=PLOT_SURFACE)
    ax.set_facecolor(PLOT_SURFACE)
    for label, shade in PLOT_REGION_SHADES.items():
        block = horizon.index[(source == label) & shown]
        if len(block):
            ax.axvspan(block.min(), block.max() + pd.Timedelta(days=1), color=shade, lw=0, zorder=0)
            ax.text(block.min() + (block.max() - block.min()) / 2, 0.09, label, transform=ax.get_xaxis_transform(),
                    ha="center", va="bottom", fontsize=9, color="#52514e", style="italic")
    if delivered is not None:
        ax.plot(delivered.index[shown], delivered[shown], color="#52514e", lw=1.2, ls="--",
                label="as delivered, before the shift to the seam")
    ax.plot(daily.index[shown], daily[shown], color=PLOT_DAILY_COLOR, lw=1.2, label="daily (what the pipeline subtracts / adds)")
    ax.plot(ma.index[shown], ma[shown], color=PLOT_MA_COLOR, lw=2, label="trailing 28-day mean (display layer)")
    seam = pd.Timestamp(plan.forecast_start)
    dec15 = pd.Timestamp(year=seam.year, month=12, day=15)
    for when, text in ((seam, f"seam {seam.date()}"), (dec15, f"Dec-15: {summary.get('dec15_ma28', float('nan')):,.0f}")):
        ax.axvline(when, color="#52514e", ls=":", lw=1)
        ax.text(when, 0.02, f" {text}", transform=ax.get_xaxis_transform(), fontsize=9, color="#52514e", va="bottom")
    ax.axhline(0, color="#52514e", lw=0.8)
    ax.yaxis.set_major_formatter(FuncFormatter(_thousands_formatter(daily[shown])))
    ax.set_ylabel(f"incremental {plan.platform} DAU")
    sign_word = "tailwind (+)" if daily[shown].sum() >= 0 else "headwind (−)"
    ax.set_title(f"`{plan.code}` {plan.name} — {sign_word}, {plan.family.replace('_', ' ')}, {plan.data_source}", loc="left", fontsize=12)
    ax.legend(loc="upper left", fontsize=9, frameon=False)
    ax.grid(alpha=0.25)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=130, facecolor=PLOT_SURFACE)
    plt.close(fig)
    return out_path


def _thousands_formatter(values: pd.Series):
    """Tick labels with enough digits to differ from each other (45.10K, not 45K / 45K / 45K)."""
    span = float(values.max() - values.min()) if len(values) else 0.0
    decimals = 0 if span >= 20_000 else (1 if span >= 2_000 else 2)
    return lambda v, _pos: f"{v / 1e3:,.{decimals}f}K"


# --- revert ------------------------------------------------------------------------------

def stash_previous_build(plan: IngestPlan) -> Optional[Path]:
    """Move the live spec + data file + meta for this code into a REVERT dir with restore steps."""
    if not plan.spec_path.exists():
        return None
    previous_spec = json.loads(plan.spec_path.read_text())
    revert_dir = plan.cycle_dir / f"{plan.name}_REVERT_{date.today().isoformat()}"
    revert_dir.mkdir(parents=True, exist_ok=True)
    moved = [plan.spec_path]
    for key in ("data_file", "model_meta_file"):
        if key in previous_spec:
            candidate = (plan.spec_path.parent / previous_spec[key]).resolve()
            if candidate.exists():
                moved.append(candidate)
    for src in moved:
        shutil.move(str(src), str(revert_dir / src.name))
    previous_index = plan.curve_dir / "_index.md"
    if previous_index.exists():  # the build rewrites _index.md; keep the previous wording with the revert target
        shutil.copyfile(previous_index, revert_dir / "_index.previous.md")
    (revert_dir / "REVERT.md").write_text(
        f"# Revert target for `{plan.code}` ({plan.name}), stashed {date.today().isoformat()}\n\n"
        f"The build that was live before the {date.today().isoformat()} re-ingest. Not an archive: delete only after the cycle closes.\n\n"
        f"To restore, move these files back and re-run the model:\n\n"
        + "".join(f"- `{p.name}` → `{p.parent.relative_to(plan.repo_root)}/`\n" for p in moved)
        + "\nThe registry entry and `.gitignore` exception were not changed by the re-ingest and need no revert.\n"
    )
    return revert_dir


# --- orchestration ---------------------------------------------------------------------------

def build(plan: IngestPlan, frame: pd.DataFrame) -> dict:
    """Write every artifact for the plan. Returns a summary dict the CLI prints as JSON."""
    status = registry_status(plan)
    if plan.spec_path.exists() and not plan.replace:
        raise FileExistsError(f"{plan.spec_path} exists; pass --replace to stash it in a REVERT dir and overwrite")
    revert_dir = stash_previous_build(plan) if plan.replace else None

    curve = normalize_curve(frame, plan)
    horizon, summary = build_horizon_curve(curve, plan)
    edge = summary["actuals_through"] or summary["delivered_end"]

    plan.curve_dir.mkdir(parents=True, exist_ok=True)
    source_dir = plan.curve_dir / "source_data"
    source_dir.mkdir(exist_ok=True)
    source_copy = source_dir / Path(plan.source_path).name
    shutil.copyfile(plan.source_path, source_copy)

    parquet_path = plan.curve_dir / f"{plan.name}.{edge}.parquet"
    horizon.to_parquet(parquet_path)
    horizon.to_csv(parquet_path.with_suffix(".csv"))
    meta_name = f"{plan.name}.{edge}.meta.json"
    meta_path = write_meta(
        parquet_path,
        forecast_start_date=plan.forecast_start,
        data_source=plan.data_source if plan.family == PER_TILE_OVERLAY else None,
        produced_by="scripts/ingest_adjustment.py",
        model_config=None,
        adjustments_applied=[],
        extra={
            "artifact_type": "adjustment_curve",
            "adjustment_code": plan.code,
            "adjustment_name": plan.name,
            "family": plan.family,
            "platform": plan.platform,
            "source_file": str(source_copy.relative_to(plan.repo_root)),
            "source_sha1": hashlib.sha1(source_copy.read_bytes()).hexdigest(),
            "source_sheet": plan.sheet,
            "column_mapping": {"date": plan.date_column, "value": plan.value_column,
                               "type": plan.type_column, "ma_validated_not_used": plan.ma_column},
            "coverage": summary,
            "allocation": {"key": plan.allocation, "shares": plan.shares, "exclude_countries": plan.exclude_countries},
        },
    )
    meta_path = meta_path.rename(plan.curve_dir / meta_name)

    plan.spec_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.family == PER_TILE_OVERLAY:
        spec = overlay_spec(plan, parquet_path.name, meta_name)
    else:
        spec = display_spec(plan, f"../{plan.name}/{parquet_path.name}")
    plan.spec_path.write_text(json.dumps(spec, indent=2) + "\n")

    if status == "new":
        append_registry_entry(plan)
    gitignore_added = ensure_gitignore_exceptions(plan)

    plot_path = render_curve_plot(horizon, plan, summary, plan.curve_dir / "plots" / f"{plan.name}.{edge}.curve.png")
    files = {"spec": str(plan.spec_path), "parquet": str(parquet_path), "csv": str(parquet_path.with_suffix(".csv")),
             "meta": str(meta_path), "source_copy": str(source_copy), "plot": str(plot_path)}
    index_path = plan.curve_dir / "_index.md"
    if not index_path.exists() or plan.replace:
        index_path.write_text(render_index_md(plan, summary, files))
    files["index"] = str(index_path)

    return {
        "code": plan.code, "name": plan.name, "family": plan.family, "registry": status,
        "revert_dir": str(revert_dir) if revert_dir else None, "gitignore_added": gitignore_added,
        "files": files, "coverage": summary, "plan": asdict(plan),
        "next": ("model re-run required (curve is subtracted from training rows); verify with "
                 f"scripts/verify_overlay.py --code {plan.code} --cycle {plan.cycle}" if plan.family == PER_TILE_OVERLAY
                 else "no re-run: the canonical notebook picks this up from adjustments/ on its next run"),
    }
