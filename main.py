"""
Pasture Predictions — Backend API
===================================
Run with:
    uvicorn main:app --reload --port 8001

Interactive docs at: http://localhost:8001/docs

Environment variables (set in .env):
    OPENWEATHER_API_KEY — no longer required (now uses Open-Meteo, free, no key needed)
    DATABASE_URL        — sqlite path, default "pasture.db"
    FARM_LAT            — farm latitude, default 38.03
    FARM_LON            — farm longitude, default -78.48
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import select

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

from app import db
from app.models import (
    Animal, AnimalCreate,
    Breed,
    Farm, FarmCreate,
    Paddock, PaddockCreate,
    SensorReading, SensorIngestPayload,
    WeatherRecord,
    GrassPrediction,
)
from app.grass_model import predict_7day, predict_growth_rate, rotation_advice
from app.ingest import ingest_reading, run_predictions_for_all_paddocks
from app.scheduler import start_scheduler, stop_scheduler
from app.weather import poll_and_store

FARM_LAT = float(os.getenv("FARM_LAT", "38.03"))
FARM_LON = float(os.getenv("FARM_LON", "-78.48"))
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")

# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.create_db_and_tables()
    log.info("Database ready — %s", db.DB_PATH)
    start_scheduler()
    yield
    stop_scheduler()

app = FastAPI(
    title="Pasture Predictions API",
    version="2.0.0",
    description="Live sensor ingestion, automated weather polling, grass growth predictions.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your dashboard URL in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    """Quick liveness check."""
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "db": str(db.DB_PATH),
    }

# ── File upload ───────────────────────────────────────────────────────────────

import io
import pandas as pd
from fastapi import Request

@app.post("/import/upload", tags=["Import"])
async def upload_file(request: Request):
    """
    Upload a CSV or XLSX file as raw bytes.
    Headers:
      X-Filename      — original filename (used to detect .csv vs .xlsx)
      X-Import-Type   — 'grass', 'cow', or 'auto' (default)
      X-Farm-Id       — optional int, assigns all imported animals to this farm
    """
    content = await request.body()
    fname = request.headers.get("x-filename", "upload").strip()
    import_type = request.headers.get("x-import-type", "auto").strip().lower()
    farm_id_hdr = request.headers.get("x-farm-id", "").strip()
    farm_id: Optional[int] = int(farm_id_hdr) if farm_id_hdr.isdigit() else None
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""

    summary = {"filename": fname, "import_type": import_type, "sheets": {}}

    if not content:
        raise HTTPException(status_code=400, detail="Empty file body received")

    def df_to_rows(df: pd.DataFrame) -> list[dict]:
        df.columns = [str(c).strip() for c in df.columns]
        return [
            {k: (None if (isinstance(v, float) and pd.isna(v)) else v)
             for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]

    def route_csv(df: pd.DataFrame, hint: str) -> dict:
        cols = {c.lower() for c in df.columns}
        rows = df_to_rows(df)
        if hint == "grass":
            return {"Paddocks": import_paddocks(rows)}
        if hint == "cow":
            if any(c in cols for c in ["tag number", "tag_number", "condition score (bcs)", "bcs", "sex"]):
                return {"Animals": import_animals(rows, farm_id=farm_id)}
            return {"Herd": import_breeds(rows)}
        # auto-detect
        if any(c in cols for c in ["grass_height_cm", "grass height (cm)", "soil_moisture_pct"]):
            return {"Paddocks": import_paddocks(rows)}
        if any(c in cols for c in ["temperature (°f)", "temperature_f", "weather note"]):
            return {"Weather": import_weather(rows)}
        if any(c in cols for c in ["tag number", "tag_number", "condition score (bcs)"]):
            return {"Animals": import_animals(rows, farm_id=farm_id)}
        if any(c in cols for c in ["avg weight (lb)", "daily feed (lb)", "adg_lb_day", "breed_type"]):
            return {"Herd": import_breeds(rows)}
        return {"Paddocks": import_paddocks(rows)}

    try:
        if ext == "csv":
            df = pd.read_csv(io.BytesIO(content), dtype=str)
            summary["sheets"] = route_csv(df, import_type)
        elif ext in ("xlsx", "xls"):
            xf = pd.ExcelFile(io.BytesIO(content))
            cow_keywords   = ["animal", "herd", "cow", "breed"]
            grass_keywords = ["paddock", "weather", "sensor", "grass", "field"]
            sheet_dispatch = {
                "paddock": lambda rows: import_paddocks(rows),
                "weather": lambda rows: import_weather(rows),
                "herd":    lambda rows: import_breeds(rows),
                "animal":  lambda rows: import_animals(rows, farm_id=farm_id),
                "cow":     lambda rows: import_animals(rows, farm_id=farm_id),
                "breed":   lambda rows: import_breeds(rows),
            }
            for sheet_name in xf.sheet_names:
                key = sheet_name.lower().strip()
                if import_type == "grass" and any(w in key for w in cow_keywords):
                    continue
                if import_type == "cow" and any(w in key for w in grass_keywords):
                    continue
                for keyword, fn in sheet_dispatch.items():
                    if keyword in key:
                        df = xf.parse(sheet_name, dtype=str)
                        rows = df_to_rows(df)
                        summary["sheets"][sheet_name] = fn(rows)
                        break
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: .{ext} — upload a .csv or .xlsx file")
    except HTTPException:
        raise
    except Exception as e:
        log.error("File import error (%s): %s", fname, e)
        raise HTTPException(status_code=500, detail=f"Parse error: {e}")

    summary["total_upserted"] = sum(
        v.get("upserted", 0) for v in summary["sheets"].values()
    )
    log.info("File import complete: %s (%s) — %d rows upserted", fname, import_type, summary["total_upserted"])
    return summary

# ══════════════════════════════════════════════════════════════════════════════
# WEATHER
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/weather/latest", tags=["Weather"])
def get_latest_weather():
    """Most recent observed weather row + next 7 forecast days."""
    with db.get_session() as sess:
        current = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == False)
            .order_by(WeatherRecord.timestamp.desc())
        ).first()
        forecasts = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == True)
            .order_by(WeatherRecord.record_date.asc())
            .limit(7)
        ).all()
    return {
        "current":   current.model_dump() if current else None,
        "forecasts": [f.model_dump() for f in forecasts],
    }

@app.get("/weather/history", tags=["Weather"])
def get_weather_history(days: int = Query(14, ge=1, le=90)):
    """Return last N days of observed weather rows."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with db.get_session() as sess:
        rows = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == False)
            .where(WeatherRecord.timestamp >= cutoff)
            .order_by(WeatherRecord.record_date.asc())
        ).all()
    return [r.model_dump() for r in rows]

@app.post("/weather/poll", tags=["Weather"])
def trigger_weather_poll():
    """Manually trigger a weather poll right now (outside the hourly schedule)."""
    result = poll_and_store(FARM_LAT, FARM_LON)
    if result.get("error"):
        raise HTTPException(status_code=502, detail=result["error"])
    return result

@app.get("/weather/current", tags=["Weather"])
async def weather_current(location: str):
    """
    Current conditions for any city string.
    Geocodes via Nominatim (free), then fetches from Open-Meteo (free, no API key).
    Response is shaped to match the OWM /data/2.5/weather structure the frontend expects.
    """
    async with httpx.AsyncClient() as client:
        # 1. Geocode
        geo = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "limit": 1, "q": location},
            headers={"User-Agent": "GreenPastures/1.0", "Accept-Language": "en"},
            timeout=10,
        )
        geo.raise_for_status()
        results = geo.json()
        if not results:
            raise HTTPException(status_code=404, detail=f"Location not found: {location}")
        lat  = float(results[0]["lat"])
        lon  = float(results[0]["lon"])
        name = results[0].get("display_name", location).split(",")[0].strip()
        country = results[0].get("display_name", "").split(",")[-1].strip()[:2].upper()

        # 2. Current weather from Open-Meteo
        wx = await client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":            lat,
                "longitude":           lon,
                "current":             "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,wind_speed_10m,cloud_cover,surface_pressure,visibility",
                "temperature_unit":    "fahrenheit",
                "wind_speed_unit":     "mph",
                "precipitation_unit":  "inch",
                "timezone":            "auto",
                "forecast_days":       1,
            },
            timeout=10,
        )
        wx.raise_for_status()
        d = wx.json()
        c = d.get("current", {})

    temp_f    = c.get("temperature_2m", 0)
    feels_f   = c.get("apparent_temperature", temp_f)
    humidity  = c.get("relative_humidity_2m", 0)
    wind_mph  = c.get("wind_speed_10m", 0)
    clouds    = c.get("cloud_cover", 0)
    pressure  = c.get("surface_pressure", 1013)
    precip_in = c.get("precipitation", 0)
    vis_m     = int(c.get("visibility", 10000) or 10000)

    # Pick a simple weather description from cloud cover
    if clouds < 20:   desc, icon = "clear sky",         "01d"
    elif clouds < 50: desc, icon = "partly cloudy",     "02d"
    elif clouds < 85: desc, icon = "cloudy",            "03d"
    else:             desc, icon = "overcast",           "04d"
    if precip_in > 0.01: desc, icon = "light rain",    "10d"
    if precip_in > 0.1:  desc, icon = "rain",          "09d"

    # Return OWM-compatible shape so frontend needs no changes
    return {
        "name": name,
        "sys":  {"country": country},
        "coord": {"lat": lat, "lon": lon},
        "main": {
            "temp":       temp_f,
            "feels_like": feels_f,
            "humidity":   humidity,
            "pressure":   pressure,
        },
        "wind":   {"speed": wind_mph},
        "clouds": {"all": clouds},
        "visibility": vis_m,
        "rain":   {"1h": precip_in},
        "weather": [{"description": desc, "icon": icon}],
    }


@app.get("/weather/forecast", tags=["Weather"])
async def weather_forecast(location: str):
    """
    7-day forecast for any city string.
    Geocodes via Nominatim (free), then fetches from Open-Meteo (free, no API key).
    Response is shaped to match the OWM /data/2.5/forecast structure the frontend expects.
    """
    async with httpx.AsyncClient() as client:
        # 1. Geocode
        geo = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "limit": 1, "q": location},
            headers={"User-Agent": "GreenPastures/1.0", "Accept-Language": "en"},
            timeout=10,
        )
        geo.raise_for_status()
        results = geo.json()
        if not results:
            raise HTTPException(status_code=404, detail=f"Location not found: {location}")
        lat = float(results[0]["lat"])
        lon = float(results[0]["lon"])

        # 2. Daily forecast from Open-Meteo
        fc = await client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":           lat,
                "longitude":          lon,
                "daily":              "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,cloud_cover_mean",
                "temperature_unit":   "fahrenheit",
                "wind_speed_unit":    "mph",
                "precipitation_unit": "inch",
                "timezone":           "auto",
                "forecast_days":      8,
            },
            timeout=10,
        )
        fc.raise_for_status()
        d = fc.json().get("daily", {})

    # Build a list that matches OWM forecast list shape (one entry per day at noon)
    items = []
    dates      = d.get("time", [])
    temp_max   = d.get("temperature_2m_max", [])
    temp_min   = d.get("temperature_2m_min", [])
    precip     = d.get("precipitation_sum", [])
    wind       = d.get("wind_speed_10m_max", [])
    cloud      = d.get("cloud_cover_mean", [])

    for i, date in enumerate(dates):
        avg_temp  = ((temp_max[i] or 0) + (temp_min[i] or 0)) / 2
        precip_in = precip[i] or 0
        clouds    = cloud[i] or 0

        if clouds < 20:      desc, icon = "clear sky",   "01d"
        elif clouds < 50:    desc, icon = "partly cloudy","02d"
        elif clouds < 85:    desc, icon = "cloudy",      "03d"
        else:                desc, icon = "overcast",     "04d"
        if precip_in > 0.01: desc, icon = "light rain",  "10d"
        if precip_in > 0.1:  desc, icon = "rain",        "09d"

        items.append({
            "dt_txt":  f"{date} 12:00:00",
            "main":    {"temp": avg_temp},
            "weather": [{"description": desc, "icon": icon}],
            "wind":    {"speed": wind[i] or 0},
            "rain":    {"3h": precip_in / 8},
        })

    return {"list": items}



# ══════════════════════════════════════════════════════════════════════════════
# SENSORS
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/sensors/ingest", tags=["Sensors"])
def sensor_ingest(payload: SensorIngestPayload):
    """Receive one sensor reading from a field device."""
    return ingest_reading(payload)

@app.get("/sensors/latest", tags=["Sensors"])
def get_latest_sensors():
    """Latest sensor reading per paddock."""
    with db.get_session() as sess:
        paddocks = sess.exec(select(Paddock)).all()
        result = []
        for pk in paddocks:
            reading = sess.exec(
                select(SensorReading)
                .where(SensorReading.paddock_id == pk.id)
                .order_by(SensorReading.timestamp.desc())
            ).first()
            result.append({
                "paddock_id":      pk.id,
                "paddock_name":    pk.name,
                "acres":           pk.acres,
                "status":          pk.status,
                "assigned_breed":  pk.assigned_breed,
                "reading":         reading.model_dump() if reading else None,
            })
    return result

@app.get("/sensors/history/{paddock_name}", tags=["Sensors"])
def get_sensor_history(paddock_name: str, days: int = Query(14, ge=1, le=90)):
    """Return all sensor readings for a paddock over the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with db.get_session() as sess:
        rows = sess.exec(
            select(SensorReading)
            .where(SensorReading.paddock_name == paddock_name)
            .where(SensorReading.timestamp >= cutoff)
            .order_by(SensorReading.timestamp.asc())
        ).all()
    return [r.model_dump() for r in rows]

# ══════════════════════════════════════════════════════════════════════════════
# GRASS MODEL
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/grass/predict", tags=["Grass Model"])
def grass_predict(
    height_cm:     float = Query(..., description="Current grass height in cm"),
    soil_moisture: float = Query(30.0, description="Soil moisture % VWC"),
    temp_c:        float = Query(..., description="Air temperature °C"),
):
    """On-demand grass growth prediction — 7-day height forecast."""
    from app.ingest import _forecast_temps
    fc = _forecast_temps()
    fc[0] = temp_c
    result = predict_7day(height_cm, soil_moisture, fc)
    result["input"] = {"height_cm": height_cm, "soil_moisture": soil_moisture, "temp_c": temp_c}
    return result

@app.get("/grass/predictions", tags=["Grass Model"])
def get_all_predictions():
    """Latest stored GrassPrediction per paddock."""
    with db.get_session() as sess:
        paddocks = sess.exec(select(Paddock)).all()
        result = []
        for pk in paddocks:
            pred = sess.exec(
                select(GrassPrediction)
                .where(GrassPrediction.paddock_id == pk.id)
                .order_by(GrassPrediction.created_at.desc())
            ).first()
            result.append({
                "paddock_id":   pk.id,
                "paddock_name": pk.name,
                "status":       pk.status,
                "prediction":   pred.model_dump() if pred else None,
            })
    return result

@app.post("/grass/refresh", tags=["Grass Model"])
def trigger_prediction_refresh():
    """Force an immediate prediction refresh for all paddocks."""
    results = run_predictions_for_all_paddocks()
    return {"paddocks_updated": len(results), "results": results}

@app.get("/grass/rotation-advice", tags=["Grass Model"])
def get_rotation_advice():
    """Automated grazing rotation advice based on current sensor readings."""
    with db.get_session() as sess:
        paddocks = sess.exec(select(Paddock)).all()
        payload = []
        for pk in paddocks:
            reading = sess.exec(
                select(SensorReading)
                .where(SensorReading.paddock_id == pk.id)
                .order_by(SensorReading.timestamp.desc())
            ).first()
            payload.append({
                "name":            pk.name,
                "status":          pk.status,
                "grass_height_cm": reading.grass_height_cm if reading else 5.0,
                "soil_moisture":   reading.soil_moisture   if reading else 30.0,
                "temp_c":          reading.air_temp_c      if reading else 16.0,
            })
    return rotation_advice(payload)

# ══════════════════════════════════════════════════════════════════════════════
# PADDOCKS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/paddocks", tags=["Paddocks"])
def list_paddocks():
    with db.get_session() as sess:
        return [p.model_dump() for p in sess.exec(select(Paddock)).all()]

@app.post("/paddocks", tags=["Paddocks"])
def create_paddock(payload: PaddockCreate):
    pk = Paddock(**payload.model_dump())
    with db.get_session() as sess:
        sess.add(pk); sess.commit(); sess.refresh(pk)
    return pk.model_dump()

@app.patch("/paddocks/{paddock_id}", tags=["Paddocks"])
def update_paddock(paddock_id: int, payload: PaddockCreate):
    with db.get_session() as sess:
        pk = sess.get(Paddock, paddock_id)
        if not pk:
            raise HTTPException(404, "Paddock not found")
        for k, v in payload.model_dump(exclude_unset=True).items():
            setattr(pk, k, v)
        sess.add(pk); sess.commit(); sess.refresh(pk)
    return pk.model_dump()

@app.delete("/paddocks/{paddock_id}", tags=["Paddocks"])
def delete_paddock(paddock_id: int):
    with db.get_session() as sess:
        pk = sess.get(Paddock, paddock_id)
        if not pk:
            raise HTTPException(404, "Paddock not found")
        sess.delete(pk); sess.commit()
    return {"deleted": paddock_id}

# ══════════════════════════════════════════════════════════════════════════════
# ANIMALS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/animals", tags=["Animals"])
def list_animals(paddock_id: Optional[int] = None):
    with db.get_session() as sess:
        q = select(Animal)
        if paddock_id:
            q = q.where(Animal.paddock_id == paddock_id)
        return [a.model_dump() for a in sess.exec(q).all()]

@app.post("/animals", tags=["Animals"])
def create_animal(payload: AnimalCreate):
    a = Animal(**payload.model_dump())
    with db.get_session() as sess:
        sess.add(a); sess.commit(); sess.refresh(a)
    return a.model_dump()

@app.patch("/animals/{animal_id}", tags=["Animals"])
def update_animal(animal_id: int, payload: AnimalCreate):
    with db.get_session() as sess:
        a = sess.get(Animal, animal_id)
        if not a:
            raise HTTPException(404, "Animal not found")
        a.updated_at = datetime.now(timezone.utc)
        for k, v in payload.model_dump(exclude_unset=True).items():
            setattr(a, k, v)
        sess.add(a); sess.commit(); sess.refresh(a)
    return a.model_dump()

@app.delete("/animals/{animal_id}", tags=["Animals"])
def delete_animal(animal_id: int):
    with db.get_session() as sess:
        a = sess.get(Animal, animal_id)
        if not a:
            raise HTTPException(404, "Animal not found")
        sess.delete(a); sess.commit()
    return {"deleted": animal_id}

# ══════════════════════════════════════════════════════════════════════════════
# BREEDS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/breeds", tags=["Breeds"])
def list_breeds():
    with db.get_session() as sess:
        return [b.model_dump() for b in sess.exec(select(Breed)).all()]

@app.post("/breeds", tags=["Breeds"])
def create_breed(payload: Breed):
    with db.get_session() as sess:
        sess.add(payload); sess.commit(); sess.refresh(payload)
    return payload.model_dump()

# ══════════════════════════════════════════════════════════════════════════════
# FARMS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/farms", tags=["Farms"])
def list_farms():
    """Return all saved farm locations."""
    with db.get_session() as sess:
        farms = sess.exec(select(Farm)).all()
    return [f.model_dump() for f in farms]

@app.get("/farms/count", tags=["Farms"])
def count_farms():
    """
    Returns the number of farms currently in the database.
    The frontend uses this to decide whether to prompt for farm selection
    when uploading a cow/animal file.
    """
    with db.get_session() as sess:
        farms = sess.exec(select(Farm)).all()
    return {"count": len(farms), "farms": [{"id": f.id, "name": f.name, "address": f"{f.street}, {f.city}, {f.state}"} for f in farms]}

@app.post("/farms", tags=["Farms"])
def create_farm(payload: FarmCreate):
    """Save a new farm address."""
    farm = Farm(**payload.model_dump())
    with db.get_session() as sess:
        sess.add(farm); sess.commit(); sess.refresh(farm)
    log.info("Farm created: %s — %s, %s, %s", farm.name, farm.street, farm.city, farm.state)
    return farm.model_dump()

@app.patch("/farms/{farm_id}", tags=["Farms"])
def update_farm(farm_id: int, payload: FarmCreate):
    """Update an existing farm's name or address."""
    with db.get_session() as sess:
        farm = sess.get(Farm, farm_id)
        if not farm:
            raise HTTPException(404, "Farm not found")
        for k, v in payload.model_dump(exclude_unset=True).items():
            setattr(farm, k, v)
        sess.add(farm); sess.commit(); sess.refresh(farm)
    return farm.model_dump()

@app.delete("/farms/{farm_id}", tags=["Farms"])
def delete_farm(farm_id: int):
    """Delete a farm. Animals assigned to it will have farm_id set to null."""
    with db.get_session() as sess:
        farm = sess.get(Farm, farm_id)
        if not farm:
            raise HTTPException(404, "Farm not found")
        # Unlink animals before deleting
        animals = sess.exec(select(Animal).where(Animal.farm_id == farm_id)).all()
        for a in animals:
            a.farm_id = None
            sess.add(a)
        sess.delete(farm); sess.commit()
    return {"deleted": farm_id}

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD EXPORT
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/dashboard/export", tags=["Dashboard"])
def dashboard_export():
    """All data in one call — paddocks + sensor readings + predictions + weather + breeds + animals + farms."""
    with db.get_session() as sess:
        paddocks  = sess.exec(select(Paddock)).all()
        breeds    = sess.exec(select(Breed)).all()
        animals   = sess.exec(select(Animal)).all()
        farms     = sess.exec(select(Farm)).all()
        hist_w    = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == False)
            .order_by(WeatherRecord.record_date.asc())
        ).all()
        fc_w = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == True)
            .order_by(WeatherRecord.record_date.asc())
            .limit(7)
        ).all()
        current_w = sess.exec(
            select(WeatherRecord)
            .where(WeatherRecord.is_forecast == False)
            .order_by(WeatherRecord.timestamp.desc())
        ).first()

        paddock_data = []
        for pk in paddocks:
            reading = sess.exec(
                select(SensorReading)
                .where(SensorReading.paddock_id == pk.id)
                .order_by(SensorReading.timestamp.desc())
            ).first()
            pred = sess.exec(
                select(GrassPrediction)
                .where(GrassPrediction.paddock_id == pk.id)
                .order_by(GrassPrediction.created_at.desc())
            ).first()
            paddock_data.append({
                **pk.model_dump(),
                "latest_reading":    reading.model_dump() if reading else None,
                "latest_prediction": pred.model_dump()    if pred    else None,
            })

    return {
        "paddocks":         paddock_data,
        "weather_history":  [w.model_dump() for w in hist_w],
        "weather_forecast": [w.model_dump() for w in fc_w],
        "current_weather":  current_w.model_dump() if current_w else None,
        "breeds":           [b.model_dump() for b in breeds],
        "animals":          [a.model_dump() for a in animals],
        "farms":            [f.model_dump() for f in farms],
    }

# ══════════════════════════════════════════════════════════════════════════════
# BULK IMPORT
# ══════════════════════════════════════════════════════════════════════════════

from typing import Any

@app.post("/import/paddocks", tags=["Import"])
def import_paddocks(rows: List[dict]):
    """Upsert paddock + sensor readings from a CSV or XLSX Paddock sheet."""
    inserted = updated = 0
    with db.get_session() as sess:
        for row in rows:
            name = str(row.get("name") or row.get("paddock_name") or "").strip()
            if not name or name == "?":
                continue
            pk = sess.exec(select(Paddock).where(Paddock.name == name)).first()
            if pk:
                pk.acres          = float(row.get("acres") or pk.acres or 0) or pk.acres
                pk.status         = str(row.get("status") or pk.status or "ready").lower()
                pk.assigned_breed = str(row.get("breed") or row.get("assigned_breed") or pk.assigned_breed or "")
                sess.add(pk); sess.commit(); sess.refresh(pk)
                updated += 1
            else:
                pk = Paddock(
                    name          = name,
                    acres         = float(row.get("acres") or 0),
                    status        = str(row.get("status") or "ready").lower(),
                    assigned_breed= str(row.get("breed") or row.get("assigned_breed") or ""),
                )
                sess.add(pk); sess.commit(); sess.refresh(pk)
                inserted += 1

            grass    = row.get("grass")    or row.get("grass_height_cm")
            moisture = row.get("moisture") or row.get("soil_moisture_pct")
            temp     = row.get("temp")     or row.get("temperature_c")
            if any(v is not None for v in [grass, moisture, temp]):
                sr = SensorReading(
                    paddock_id      = pk.id,
                    paddock_name    = pk.name,
                    grass_height_cm = float(grass)    if grass    is not None else None,
                    soil_moisture   = float(moisture) if moisture is not None else None,
                    air_temp_c      = float(temp)     if temp     is not None else None,
                    source          = "file_import",
                )
                sess.add(sr); sess.commit()
    return {"upserted": inserted + updated, "inserted": inserted, "updated": updated}

@app.post("/import/weather", tags=["Import"])
def import_weather(rows: List[dict]):
    """Upsert weather rows from a CSV or XLSX Weather sheet."""
    inserted = updated = 0
    with db.get_session() as sess:
        for row in rows:
            date = str(row.get("date") or "").strip()
            if not date:
                continue
            note  = str(row.get("note") or row.get("description") or "")
            is_fc = "forecast" in note.lower()
            existing = sess.exec(
                select(WeatherRecord)
                .where(WeatherRecord.record_date == date)
                .where(WeatherRecord.is_forecast  == is_fc)
            ).first()
            fields: dict[str, Any] = dict(
                record_date      = date,
                is_forecast      = is_fc,
                temperature_c    = float(row.get("temp")   or row.get("temperature_c")    or 0),
                precipitation_mm = float(row.get("precip") or row.get("precipitation_mm") or 0),
                wind_ms          = float(row.get("wind")   or row.get("wind_speed_ms")    or 0) or None,
                humidity_pct     = float(row.get("humidity") or 0) or None,
                description      = note or None,
                source           = "file_import",
            )
            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                existing.timestamp = datetime.now(timezone.utc)
                sess.add(existing); updated += 1
            else:
                sess.add(WeatherRecord(timestamp=datetime.now(timezone.utc), **fields))
                inserted += 1
        sess.commit()
    return {"upserted": inserted + updated, "inserted": inserted, "updated": updated}

@app.post("/import/breeds", tags=["Import"])
def import_breeds(rows: List[dict]):
    """Upsert breed averages from a CSV or XLSX Herd sheet."""
    inserted = updated = 0
    with db.get_session() as sess:
        for row in rows:
            name = str(row.get("breed") or "").strip()
            if not name:
                continue
            existing = sess.exec(select(Breed).where(Breed.name == name)).first()
            fields: dict[str, Any] = dict(
                name          = name,
                breed_type    = str(row.get("type")       or row.get("breed_type")    or "Beef"),
                avg_weight_kg = float(row.get("weight")   or row.get("avg_weight_kg") or 0) or None,
                daily_feed_kg = float(row.get("dm")       or row.get("daily_feed_kg") or 0) or None,
                milk_l_day    = float(row.get("milk")     or row.get("milk_l_day")    or 0) or None,
                adg_kg_day    = float(row.get("adg")      or row.get("adg_kg_day")    or 0) or None,
                stocking_h_ac = float(row.get("stocking") or row.get("stocking_h_ac") or 0) or None,
                us_prevalence = float(row.get("prevalence") or row.get("us_prevalence") or 0) or None,
                notes         = str(row.get("notes") or "") or None,
            )
            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                sess.add(existing); updated += 1
            else:
                sess.add(Breed(**fields)); inserted += 1
        sess.commit()
    return {"upserted": inserted + updated, "inserted": inserted, "updated": updated}

@app.post("/import/animals", tags=["Import"])
def import_animals(
    rows: List[dict],
    farm_id: Optional[int] = None,
):
    """
    Upsert individual animal records from a CSV or XLSX Animals sheet.
    Matched by tag_number (primary) or name (fallback).

    Farm assignment rules:
    - If farm_id is provided → use it.
    - If farm_id is omitted and exactly one farm exists → auto-assign to that farm.
    - If farm_id is omitted and multiple farms exist → farm_id stays null
      (the frontend prompts and re-uploads with X-Farm-Id header).
    """
    # Resolve farm_id: auto-assign when there's exactly one farm
    resolved_farm_id = farm_id
    if resolved_farm_id is None:
        with db.get_session() as check_sess:
            all_farms = check_sess.exec(select(Farm)).all()
            if len(all_farms) == 1:
                resolved_farm_id = all_farms[0].id

    inserted = updated = 0
    with db.get_session() as sess:
        for row in rows:
            tag  = str(row.get("tag")  or row.get("tag_number") or "").strip()
            name = str(row.get("name") or "").strip()
            if not name and not tag:
                continue
            existing = None
            if tag:
                existing = sess.exec(select(Animal).where(Animal.tag_number == tag)).first()
            if not existing and name:
                existing = sess.exec(select(Animal).where(Animal.name == name)).first()

            bd_raw     = row.get("birthDate") or row.get("birth_date") or ""
            birth_date = None
            if bd_raw:
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%B %d, %Y"):
                    try:
                        birth_date = datetime.strptime(str(bd_raw), fmt); break
                    except ValueError:
                        pass

            paddock_name = str(row.get("paddock") or "").strip()
            paddock_id: Optional[int] = None
            if paddock_name:
                pk = sess.exec(select(Paddock).where(Paddock.name == paddock_name)).first()
                if pk:
                    paddock_id = pk.id

            fields: dict[str, Any] = dict(
                name        = name or (existing.name if existing else "Unknown"),
                animal_type = str(row.get("animalType") or row.get("animal_type") or "Cattle"),
                tag_number  = tag or None,
                status      = str(row.get("status") or "Active"),
                farm_id     = resolved_farm_id,
                paddock_id  = paddock_id,
                sex         = str(row.get("sex")       or "") or None,
                breed       = str(row.get("breed")     or "") or None,
                coloring    = str(row.get("coloring")  or "") or None,
                weight_lb   = float(row.get("weight")  or 0) or None,
                height_in   = float(row.get("height")  or 0) or None,
                framescore  = float(row.get("framescore") or 0) or None,
                bcs         = float(row.get("bcs")     or 0) or None,
                birth_date  = birth_date,
                dam         = str(row.get("dam")       or "") or None,
                sire        = str(row.get("sire")      or "") or None,
                offspring   = str(row.get("offspring") or "") or None,
                notes       = str(row.get("notes")     or "") or None,
                updated_at  = datetime.now(timezone.utc),
            )
            if existing:
                for k, v in fields.items():
                    setattr(existing, k, v)
                sess.add(existing); updated += 1
            else:
                sess.add(Animal(**fields)); inserted += 1
        sess.commit()
    return {
        "upserted":     inserted + updated,
        "inserted":     inserted,
        "updated":      updated,
        "farm_id":      resolved_farm_id,
    }
