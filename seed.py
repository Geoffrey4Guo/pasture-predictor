"""
Seed the database with sample paddocks, breeds, and animals.
Run once:  python seed.py
"""
from datetime import datetime
from app.db import create_db_and_tables, get_session
from app.models import Paddock, Breed, Animal, SensorReading

def seed():
    create_db_and_tables()
    with get_session() as sess:

        # ── Paddocks ──────────────────────────────────────────────
        paddocks = [
            Paddock(name="North Field 1",  acres=8.2,  lat=38.031, lon=-78.481, status="ready",   assigned_breed="Simmental/Angus"),
            Paddock(name="North Field 2",  acres=7.5,  lat=38.032, lon=-78.480, status="ready",   assigned_breed="Hereford/Charolais"),
            Paddock(name="North Meadow",   acres=9.1,  lat=38.033, lon=-78.479, status="ready",   assigned_breed="Angus"),
            Paddock(name="East Slope",     acres=6.8,  lat=38.030, lon=-78.478, status="grazing", assigned_breed="Angus/Jersey"),
            Paddock(name="Creek Bottom",   acres=5.4,  lat=38.029, lon=-78.477, status="grazing", assigned_breed="Jersey/Simmental"),
            Paddock(name="South Field 1",  acres=8.8,  lat=38.028, lon=-78.476, status="resting", assigned_breed=None),
            Paddock(name="South Field 2",  acres=7.1,  lat=38.027, lon=-78.475, status="resting", assigned_breed=None),
            Paddock(name="West Reserve",   acres=10.2, lat=38.030, lon=-78.485, status="hay",     assigned_breed=None),
        ]
        for p in paddocks:
            sess.add(p)
        sess.commit()
        for p in paddocks:
            sess.refresh(p)

        # ── Sensor readings ───────────────────────────────────────
        readings_data = [
            (paddocks[0], 7.2, 24.0, 16.2),
            (paddocks[1], 6.8, 27.0, 16.1),
            (paddocks[2], 8.1, 31.0, 16.5),
            (paddocks[3], 6.0, 28.0, 16.3),
            (paddocks[4], 5.5, 38.0, 15.8),
            (paddocks[5], 4.2, 35.0, 16.7),
            (paddocks[6], 3.9, 33.0, 16.9),
            (paddocks[7], 9.2, 29.0, 16.4),
        ]
        for pk, height, moisture, temp in readings_data:
            sess.add(SensorReading(
                paddock_id=pk.id, paddock_name=pk.name,
                grass_height_cm=height, soil_moisture=moisture,
                air_temp_c=temp, source="seed",
            ))
        sess.commit()

        # ── Breeds ────────────────────────────────────────────────
        breeds = [
            Breed(name="Angus",     breed_type="Beef",         avg_weight_kg=640, daily_feed_kg=16.0, milk_l_day=8,  adg_kg_day=1.25, stocking_h_ac=0.53, us_prevalence=38.0, notes="Dominant US beef; excellent foraging"),
            Breed(name="Hereford",  breed_type="Beef",         avg_weight_kg=644, daily_feed_kg=16.1, milk_l_day=7,  adg_kg_day=1.18, stocking_h_ac=0.51, us_prevalence=22.0, notes="Hardy; well-suited to extensive systems"),
            Breed(name="Simmental", breed_type="Dual-purpose", avg_weight_kg=637, daily_feed_kg=15.9, milk_l_day=20, adg_kg_day=1.52, stocking_h_ac=0.55, us_prevalence=8.0,  notes="Fast growth; good milk; versatile"),
            Breed(name="Holstein",  breed_type="Dairy",        avg_weight_kg=680, daily_feed_kg=17.0, milk_l_day=34, adg_kg_day=1.05, stocking_h_ac=0.40, us_prevalence=40.0, notes="90% of US dairy; highest milk volume"),
            Breed(name="Jersey",    breed_type="Dairy",        avg_weight_kg=454, daily_feed_kg=11.4, milk_l_day=22, adg_kg_day=0.95, stocking_h_ac=0.62, us_prevalence=7.0,  notes="Efficient grazer; 5-6% butterfat"),
            Breed(name="Charolais", breed_type="Beef",         avg_weight_kg=727, daily_feed_kg=18.2, milk_l_day=9,  adg_kg_day=1.60, stocking_h_ac=0.45, us_prevalence=5.0,  notes="Heaviest common breed; watch calving"),
        ]
        for b in breeds:
            sess.add(b)
        sess.commit()

        # ── Animals ───────────────────────────────────────────────
        pk_map = {p.name: p.id for p in paddocks}
        animals_data = [
            ("Bessie",   "TAG-001", "Female", "Angus",     "Black",       1250, 52, 4, 3.0, "North Meadow",   "2020-05-12", "Rosie (TAG-018)",     "Black Diamond (REG-042)", "None yet",              "Lead cow, calm temperament"),
            ("Daisy",    "TAG-002", "Female", "Hereford",  "Red & White", 1390, 54, 5, 3.5, "North Meadow",   "2021-06-15", "Clover (TAG-022)",    "Redstone (REG-017)",      "Calf #2025-01",         "Good milk production"),
            ("Molly",    "TAG-003", "Female", "Jersey",    "Fawn",         900, 48, 2, 3.0, "Creek Bottom",   "2019-03-20", "Buttercup (TAG-011)", "Duke (REG-033)",           "Calf #2023-04, #2024-02","High butterfat milk"),
            ("Rosie",    "TAG-004", "Female", "Angus",     "Black",       1180, 51, 4, 3.0, "East Slope",     "2022-01-08", "Bessie (TAG-001)",    "Black Diamond (REG-042)", "None yet",              "Bessie's daughter"),
            ("Clara",    "TAG-005", "Female", "Simmental", "Gold & White",1380, 55, 5, 3.5, "North Field 1",  "2022-09-30", "Heidi (TAG-019)",     "Swiss Baron (REG-055)",   "None yet",              "Fast growth rate"),
            ("Hazel",    "TAG-006", "Female", "Hereford",  "Red & White", 1420, 54, 5, 3.5, "North Field 1",  "2020-02-14", "Fern (TAG-024)",      "Redstone (REG-017)",      "Calf #2022-07, #2024-05","Calm, good mother"),
            ("Pearl",    "TAG-007", "Female", "Charolais", "Cream/White", 1560, 57, 6, 3.5, "North Field 2",  "2021-04-22", "Duchess (TAG-030)",   "Ivory King (REG-061)",    "Calf #2023-09",         "Heavy frame, easy calving"),
            ("Juniper",  "TAG-008", "Female", "Angus",     "Black",       1210, 51, 4, 2.5, "North Field 2",  "2022-06-03", "Bessie (TAG-001)",    "Ironside (REG-044)",      "None yet",              "BCS slightly low - monitor"),
            ("Clover",   "TAG-009", "Female", "Jersey",    "Light Brown",  870, 47, 2, 3.0, "East Slope",     "2023-03-18", "Molly (TAG-003)",     "Duke (REG-033)",           "None yet",              "Young heifer, first season"),
            ("Wren",     "TAG-010", "Female", "Simmental", "Gold & White",1330, 54, 5, 3.0, "Creek Bottom",   "2021-11-25", "Heidi (TAG-019)",     "Swiss Baron (REG-055)",   "Calf #2024-11",         "Dual-purpose, good milk"),
            ("Bruno",    "TAG-011", "Male",   "Angus",     "Black",       1820, 60, 7, 3.5, "North Field 1",  "2021-01-30", "Rosie (TAG-018)",     "Ironside (REG-044)",      "TAG-008, TAG-004",      "Run bull March-May"),
            ("Maverick", "TAG-012", "Male",   "Hereford",  "Red & White", 1950, 62, 7, 3.5, "North Field 2",  "2019-07-11", "Fern (TAG-024)",      "Redstone (REG-017)",      "TAG-006, TAG-002",      "Senior herd sire"),
        ]
        for (name, tag, sex, breed, color, wt, ht, fs, bcs, pk_name, bd, dam, sire, off, notes) in animals_data:
            sess.add(Animal(
                name=name, tag_number=tag, sex=sex, breed=breed, coloring=color,
                weight_lb=wt, height_in=ht, framescore=fs, bcs=bcs,
                paddock_id=pk_map.get(pk_name),
                birth_date=datetime.strptime(bd, "%Y-%m-%d"),
                dam=dam, sire=sire, offspring=off, notes=notes,
                status="Active", animal_type="Cattle",
            ))
        sess.commit()

    print("✓ Database seeded successfully")
    print(f"  {len(paddocks)} paddocks | {len(breeds)} breeds | {len(animals_data)} animals")
    print("  Start the server: uvicorn main:app --reload")

if __name__ == "__main__":
    seed()
