# Attack Plan: Wyoming April 2021 — Constant Offset Per Vehicle

**Created:** 2026-02-12
**Author:** Sophie (AI Research Assistant)
**Supervisor:** Aaron Collins
**Status:** Active

---

## Overview

This attack plan implements a **constant positional offset attack** where:
- 30% of vehicles are randomly selected as malicious
- Each malicious vehicle has a **unique, consistent attack vector**
- All BSMs from a malicious vehicle are offset by that vector
- The attack simulates a compromised OBU sending falsified position data

---

## Data Source

| Property | Value |
|----------|-------|
| Source File | `Full_Wyoming_Data.csv` (40GB) |
| Location Center | Longitude: -106.0831353, Latitude: 41.5430216 |
| Radius | 2000 meters |
| Date Range | 2021-04-01 → 2021-04-30 |
| Estimated Rows | TBD after filtering |

---

## Phase 1: Clean Feature Extraction

### Input Columns

| Letter | Col # | Column Name | Purpose |
|--------|-------|-------------|---------|
| O | 15 | `metadata_receivedAt` | Timestamp / Message ID |
| W | 23 | `coreData_msgCnt` | Message counter |
| X | 24 | `coreData_id` | Vehicle ID (attack grouping key) |
| Z | 26 | `coreData_position_lat` | Latitude → convert to Y |
| AA | 27 | `coreData_position_long` | Longitude → convert to X |
| AB | 28 | `coreData_elevation` | Z position (meters) |
| AC | 29 | `coreData_accelset_accelYaw` | Yaw acceleration |
| AD | 30 | `coreData_accuracy_semiMajor` | GPS accuracy |
| AG | 33 | `coreData_speed` | Vehicle speed |
| AH | 34 | `coreData_heading` | Vehicle heading |

### Coordinate Conversion

Latitude/longitude are converted to planar X/Y coordinates in meters using a local projection centered on the filter location. This enables:
- Euclidean distance calculations
- Simple offset application (dx, dy in meters)
- Consistent ML feature scaling

**Method:** Haversine-based local projection or UTM zone conversion.

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `metadata_receivedAt` | datetime | Message timestamp |
| `coreData_msgCnt` | int | Message counter |
| `coreData_id` | string | Vehicle identifier |
| `x_pos` | float | X position (meters from center) |
| `y_pos` | float | Y position (meters from center) |
| `coreData_elevation` | float | Z position (meters) |
| `coreData_accelset_accelYaw` | float | Yaw acceleration |
| `coreData_accuracy_semiMajor` | float | GPS accuracy (meters) |
| `coreData_speed` | float | Speed (m/s) |
| `coreData_heading` | float | Heading (degrees) |

### Cache Location

```
cache/clean/wyoming-apr2021-2000m-core-v1.parquet
```

---

## Phase 2: Attack Injection

### Attack Parameters

| Parameter | Value |
|-----------|-------|
| Attack Type | Constant offset per vehicle ID |
| Malicious Ratio | 30% of unique vehicle IDs |
| Offset Distance | Random 100-200m (per vehicle) |
| Offset Direction | Random 0-360° (per vehicle) |
| Consistency | Same vector for ALL BSMs from same vehicle |
| Random Seed | 42 |

### Attack Algorithm

```python
# Step 1: Identify unique vehicle IDs
unique_ids = dataset["coreData_id"].unique()

# Step 2: Randomly select 30% as malicious (seeded for reproducibility)
rng = np.random.RandomState(42)
n_malicious = int(len(unique_ids) * 0.30)
malicious_ids = set(rng.choice(unique_ids, n_malicious, replace=False))

# Step 3: Generate attack vector per malicious vehicle
attack_vectors = {}
for vid in malicious_ids:
    direction = rng.uniform(0, 360)  # degrees
    distance = rng.uniform(100, 200)  # meters
    dx = distance * cos(radians(direction))
    dy = distance * sin(radians(direction))
    attack_vectors[vid] = (dx, dy)

# Step 4: Apply attacks (COPY data first, never modify source)
attacked_data = clean_data.copy()
for idx, row in attacked_data.iterrows():
    if row["coreData_id"] in malicious_ids:
        dx, dy = attack_vectors[row["coreData_id"]]
        attacked_data.loc[idx, "x_pos"] += dx
        attacked_data.loc[idx, "y_pos"] += dy
        attacked_data.loc[idx, "isAttacker"] = 1
    else:
        attacked_data.loc[idx, "isAttacker"] = 0
```

### Output Schema

Same as clean dataset + `isAttacker` (int, 0 = benign, 1 = attacked)

### Cache Location

```
cache/attacks/wyoming-apr2021-constoffset-100to200m-30pct-seed42.parquet
```

---

## Phase 3: ML Training/Testing

### Data Split

| Split | Ratio | Method |
|-------|-------|--------|
| Train | 80% | Random shuffle |
| Test | 20% | Random shuffle |

**Note:** Split is by BSM (row), not by vehicle ID.

### Features (X)

| Feature | Description |
|---------|-------------|
| `x_pos` | X position (meters) |
| `y_pos` | Y position (meters) |
| `coreData_elevation` | Z position |
| `coreData_speed` | Speed |
| `coreData_accelset_accelYaw` | Yaw acceleration |
| `coreData_heading` | Heading |
| `coreData_accuracy_semiMajor` | GPS accuracy |

### Label (y)

`isAttacker` (binary classification: 0 or 1)

### Classifiers

1. **RandomForest** — Ensemble tree-based
2. **DecisionTree** — Single tree baseline
3. **KNeighbors** — Distance-based

### Output

```
results/wyoming-apr2021-constoffset-RF-DT-KNN-{timestamp}.csv
```

---

## Directory Structure

```
ConnectedDrivingPipelineV4/
├── attack_plans/
│   └── WYOMING-APR2021-CONSTOFFSET-V1.md  (this file)
├── configs/
│   └── wyoming_apr2021_constoffset_v1.json
├── cache/
│   ├── clean/
│   │   └── wyoming-apr2021-2000m-core-v1.parquet
│   └── attacks/
│       └── wyoming-apr2021-constoffset-100to200m-30pct-seed42.parquet
└── results/
    └── wyoming-apr2021-constoffset-RF-DT-KNN-{timestamp}.csv
```

---

## Execution

### Prerequisites

- Python 3.10+
- Dask, pandas, numpy, scikit-learn
- `Full_Wyoming_Data.csv` in repo root

### Run Command

```bash
cd ~/repos/ConnectedDrivingPipelineV4
python run_wyoming_apr2021_constoffset_v1.py
```

---

## Research Questions

1. Can ML classifiers detect vehicles with consistent positional offset?
2. Which classifier performs best for this attack type?
3. How does GPS accuracy correlate with attack detection?
4. Does the random split (vs vehicle-disjoint split) affect results?

---

## Future Variations

- [ ] Disjoint split by vehicle ID
- [ ] Variable offset magnitude experiments
- [ ] Multiple attack types in same dataset
- [ ] Time-based features (trajectory analysis)

---

## Change Log

| Date | Change |
|------|--------|
| 2026-02-12 | Initial plan created |
