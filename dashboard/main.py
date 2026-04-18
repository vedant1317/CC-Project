"""
dashboard/main.py
FastAPI backend — serves benchmark results from CSV and the frontend HTML.

Run:  uvicorn dashboard.main:app --reload
Open: http://localhost:8000
"""

import os
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="PitLane Dashboard", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR    = Path(__file__).resolve().parent.parent
CSV_PATH    = BASE_DIR / "results" / "results.csv"
HTML_PATH   = Path(__file__).resolve().parent / "index.html"


@app.get("/api/results", response_model=List[Dict[str, Any]])
def get_results():
    """Return all benchmark results as a JSON array."""
    if not CSV_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail=f"results.csv not found at {CSV_PATH}. Run 'python run.py' first."
        )
    df = pd.read_csv(CSV_PATH)
    return df.to_dict(orient="records")


@app.get("/api/summary")
def get_summary():
    """Return per-DB and per-scenario summary stats."""
    if not CSV_PATH.exists():
        raise HTTPException(status_code=404, detail="No results yet. Run 'python run.py' first.")

    df = pd.read_csv(CSV_PATH)

    # Best avg per DB
    by_db = (
        df.groupby("db")[["avg", "p99"]]
        .min()
        .reset_index()
        .rename(columns={"avg": "best_avg", "p99": "best_p99"})
        .to_dict(orient="records")
    )

    # Winner per scenario
    winners = {}
    for scenario, group in df.groupby("scenario"):
        best = group.loc[group["avg"].idxmin()]
        winners[scenario] = best["db"]

    return {"by_db": by_db, "winners": winners}


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    """Serve the single-file frontend dashboard."""
    if not HTML_PATH.exists():
        raise HTTPException(status_code=500, detail="index.html not found.")
    return HTML_PATH.read_text(encoding="utf-8")
