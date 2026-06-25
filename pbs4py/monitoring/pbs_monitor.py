#!/usr/bin/env python3
"""
Dash app for monitoring my PBS jobs.

Run on the login node:
    python -m pbs_dashboard.app
Then open http://localhost:8050 (or SSH-tunnel to it).
"""

from __future__ import annotations

import os
from datetime import timedelta

import dash
from dash import Input, Output, State, dash_table, dcc, html, no_update

from pbs4py.job import PBSJob

from pbs4py.monitoring.job_collector import (
    dog_out_path,
    get_user_jobs,
    has_dog_out,
    split_active_and_finished,
)
from pbs4py.monitoring.queue_stats import get_queue_stats

REFRESH_SECONDS = 15
N_RECENT_FINISHED = 10
TAIL_LINES = 500
TAIL_REFRESH_SECONDS = 2


# ----------------------------------------------------------------------
# Formatting helpers
# ----------------------------------------------------------------------
def _fmt_walltime(seconds: int | None) -> str:
    if seconds is None:
        return ""
    return str(timedelta(seconds=int(seconds)))


def _job_to_row(job: PBSJob) -> dict:
    tail_available = has_dog_out(job)
    return {
        "id": job.id.split(".")[0],
        "name": job.name,
        "state": job.state,
        "model": job.model,
        "nodes": job.requested_number_of_nodes,
        "time_running": _fmt_walltime(job.walltime_used),
        "time_limit": _fmt_walltime(job.walltime_requested),
        "workdir": job.workdir,
        "exit": job.exit_status if job.exit_status is not None else "",
        "tail": "📄 dog.out" if tail_available else "",
        "_dog_path": dog_out_path(job) if tail_available else "",
    }


JOB_COLUMNS = [
    {"name": "Job ID", "id": "id"},
    {"name": "Name", "id": "name"},
    {"name": "State", "id": "state"},
    {"name": "Model", "id": "model"},
    {"name": "Nodes", "id": "nodes"},
    {"name": "Time Running", "id": "time_running"},
    {"name": "Time Limit", "id": "time_limit"},
    {"name": "Workdir", "id": "workdir"},
    {"name": "Exit", "id": "exit"},
    {"name": "Output", "id": "tail"},
]


def _queue_stats_children(jobs: list[PBSJob]) -> list:
    queue_names = sorted({j.queue for j in jobs if j.queue})
    if not queue_names:
        return [html.Em("No active queues.")]

    cards = []
    for qname in queue_names:
        stats = get_queue_stats(qname)
        if stats is None:
            cards.append(
                html.Div(
                    f"{qname}: (unavailable)",
                    style={"padding": "0.5rem", "color": "#999"},
                )
            )
            continue

        pct = stats.percent_cpus_free
        pct_str = f"{pct:.1f}% free" if pct is not None else "n/a"
        bar_pct = max(0.0, min(100.0, 100.0 - pct)) if pct is not None else 0.0

        cards.append(
            html.Div(
                style={
                    "border": "1px solid #ccc",
                    "borderRadius": "6px",
                    "padding": "0.5rem 1rem",
                    "marginRight": "0.75rem",
                    "minWidth": "240px",
                    "display": "inline-block",
                    "verticalAlign": "top",
                },
                children=[
                    html.Div(qname, style={"fontWeight": "bold", "marginBottom": "0.25rem"}),
                    html.Div(
                        f"Running {stats.running} · Queued {stats.queued}",
                        style={"fontSize": "0.9em"},
                    ),
                    html.Div(
                        f"CPUs: {stats.ncpus_assigned}/{stats.ncpus_max} ({pct_str})",
                        style={"fontSize": "0.9em"},
                    ),
                    html.Div(
                        style={
                            "marginTop": "0.4rem",
                            "height": "6px",
                            "width": "100%",
                            "backgroundColor": "#e0e0e0",
                            "borderRadius": "3px",
                            "overflow": "hidden",
                        },
                        children=html.Div(
                            style={
                                "height": "100%",
                                "width": f"{bar_pct:.1f}%",
                                "backgroundColor": (
                                    "#66bb6a"
                                    if bar_pct < 70
                                    else "#ffa726" if bar_pct < 90 else "#ef5350"
                                ),
                            }
                        ),
                    ),
                ],
            )
        )
    return cards


# ----------------------------------------------------------------------
# Tail helpers
# ----------------------------------------------------------------------
def _tail_file(path: str, n: int = TAIL_LINES) -> str:
    if not path or not os.path.isfile(path):
        return f"(file not found: {path})"
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 8192
            data = b""
            while size > 0 and data.count(b"\n") <= n:
                read_size = min(block, size)
                size -= read_size
                f.seek(size)
                data = f.read(read_size) + data
        text = data.decode("utf-8", errors="replace")
        return "\n".join(text.splitlines()[-n:])
    except OSError as exc:
        return f"(error reading {path}: {exc})"


# ----------------------------------------------------------------------
# Layout
# ----------------------------------------------------------------------
TABLE_STYLE = dict(
    style_cell={
        "fontFamily": "monospace",
        "padding": "4px 8px",
        "textAlign": "left",
        "maxWidth": "400px",
        "overflow": "hidden",
        "textOverflow": "ellipsis",
    },
    style_header={"fontWeight": "bold", "backgroundColor": "#eee"},
    style_data_conditional=[
        {"if": {"filter_query": '{state} = "R"'}, "backgroundColor": "#e8f5e9"},
        {"if": {"filter_query": '{state} = "Q"'}, "backgroundColor": "#fff8e1"},
        {"if": {"filter_query": '{state} = "H"'}, "backgroundColor": "#ffebee"},
        {
            "if": {"filter_query": '{state} = "F" && {exit} != 0 && {exit} != ""'},
            "backgroundColor": "#ffcdd2",
        },
        {"if": {"column_id": "tail"}, "cursor": "pointer", "color": "#1565c0"},
    ],
    cell_selectable=True,
)


app = dash.Dash(__name__, title="PBS Dashboard")

app.layout = html.Div(
    style={"fontFamily": "sans-serif", "margin": "1rem", "maxWidth": "1600px"},
    children=[
        html.H2("PBS Dashboard"),
        html.Div(id="last-refresh", style={"color": "#666", "fontSize": "0.85em"}),
        dcc.Interval(id="refresh", interval=REFRESH_SECONDS * 1000, n_intervals=0),
        dcc.Interval(
            id="tail-refresh",
            interval=TAIL_REFRESH_SECONDS * 1000,
            n_intervals=0,
            disabled=True,
        ),
        dcc.Store(id="tail-target", data=""),
        html.H3("Queue Availability"),
        html.Div(id="queue-stats-card", style={"marginBottom": "1.5rem"}),
        html.H3("Active Jobs"),
        html.Div(
            "Click the Output cell on a job to tail its dog.out.",
            style={"color": "#666", "marginBottom": "0.5rem", "fontSize": "0.9em"},
        ),
        dash_table.DataTable(id="active-table", columns=JOB_COLUMNS, data=[], **TABLE_STYLE),
        html.H3("Recently Finished Jobs", style={"marginTop": "1.5rem"}),
        dash_table.DataTable(id="finished-table", columns=JOB_COLUMNS, data=[], **TABLE_STYLE),
        # Tail panel
        html.Div(
            id="tail-panel",
            style={"marginTop": "1.5rem", "display": "none"},
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "0.75rem"},
                    children=[
                        html.H3("Tail: ", style={"display": "inline", "margin": 0}),
                        html.Code(id="tail-path"),
                        html.Button(
                            "Close", id="tail-close", n_clicks=0, style={"marginLeft": "auto"}
                        ),
                    ],
                ),
                html.Pre(
                    id="tail-content",
                    style={
                        "backgroundColor": "#1e1e1e",
                        "color": "#dcdcdc",
                        "padding": "0.75rem",
                        "borderRadius": "4px",
                        "maxHeight": "500px",
                        "overflow": "auto",
                        "fontSize": "0.85em",
                        "marginTop": "0.5rem",
                        "whiteSpace": "pre-wrap",
                    },
                ),
            ],
        ),
    ],
)


# ----------------------------------------------------------------------
# Callbacks
# ----------------------------------------------------------------------
@app.callback(
    Output("active-table", "data"),
    Output("finished-table", "data"),
    Output("queue-stats-card", "children"),
    Output("last-refresh", "children"),
    Input("refresh", "n_intervals"),
)
def refresh_jobs(_n):
    from datetime import datetime

    jobs = get_user_jobs()
    active, finished = split_active_and_finished(jobs, N_RECENT_FINISHED)
    active_rows = [_job_to_row(j) for j in active]
    finished_rows = [_job_to_row(j) for j in finished]
    stats = _queue_stats_children(jobs)
    stamp = f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    return active_rows, finished_rows, stats, stamp


@app.callback(
    Output("tail-target", "data"),
    Output("tail-panel", "style"),
    Output("tail-path", "children"),
    Output("tail-refresh", "disabled"),
    Input("active-table", "active_cell"),
    Input("active-table", "data"),
    Input("finished-table", "active_cell"),
    Input("finished-table", "data"),
    Input("tail-close", "n_clicks"),
    State("tail-panel", "style"),
    prevent_initial_call=True,
)
def handle_tail_click(
    active_cell, active_data, finished_cell, finished_data, close_clicks, current_style
):
    triggered = dash.callback_context.triggered_id
    hidden_style = {"marginTop": "1.5rem", "display": "none"}
    visible_style = {"marginTop": "1.5rem", "display": "block"}

    # Close button → hide panel and stop tail refresh.
    if triggered == "tail-close":
        return "", hidden_style, "", True

    # Determine which table fired and pick its row.
    if triggered == "active-table" and active_cell:
        cell, rows = active_cell, active_data
    elif triggered == "finished-table" and finished_cell:
        cell, rows = finished_cell, finished_data
    else:
        return no_update, no_update, no_update, no_update

    # Only react when the user clicks the "Output" column.
    if cell.get("column_id") != "tail":
        return no_update, no_update, no_update, no_update

    row = rows[cell["row"]]
    path = row.get("_dog_path", "")
    if not path:
        # Cell was clicked but no dog.out exists for that job.
        return no_update, no_update, no_update, no_update

    return path, visible_style, path, False


@app.callback(
    Output("tail-content", "children"),
    Input("tail-target", "data"),
    Input("tail-refresh", "n_intervals"),
)
def update_tail_content(path, _n):
    if not path:
        return ""
    return _tail_file(path, TAIL_LINES)


# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="PBS Dashboard")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind (default: 127.0.0.1; use 0.0.0.0 to expose)",
    )
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
