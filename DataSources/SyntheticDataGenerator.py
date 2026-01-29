"""
Synthetic BSM Data Generator for Testing

Generates realistic WYDOT-compatible BSM (Basic Safety Message) records
for testing the DataSources pipeline without S3 access.

Features:
- Realistic Wyoming coordinates (I-80 corridor)
- Valid schema structure matching version 6
- Configurable record counts
- Temporal consistency (sequential timestamps)
- Vehicle trajectory simulation
"""

import json
import random
import uuid
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Any, Iterator, Optional
import logging

logger = logging.getLogger(__name__)


# Wyoming I-80 corridor coordinates (realistic for CV Pilot)
WYOMING_COORDS = [
    # (lat, lon, elevation_m) - I-80 corridor points
    (41.1136, -104.8557, 1856),  # Cheyenne area
    (41.1423, -104.9876, 1900),
    (41.1678, -105.1234, 1950),
    (41.1901, -105.2567, 2000),
    (41.2123, -105.3890, 2050),
    (41.2456, -105.5123, 2100),
    (41.2789, -105.6456, 2150),
    (41.2827, -105.5912, 2179),  # From sample data
    (41.3012, -105.7789, 2200),
    (41.3345, -105.9012, 2250),
    (41.3678, -106.0345, 2300),
    (41.4011, -106.1678, 2350),  # Laramie area
    (41.4344, -106.3011, 2300),
    (41.4677, -106.4344, 2250),
    (41.5010, -106.5677, 2200),
    (41.5343, -106.7010, 2150),
    (41.5676, -106.8343, 2100),  # Rawlins area
]


class SyntheticBSMGenerator:
    """
    Generates synthetic BSM data matching WYDOT schema version 6.
    """
    
    def __init__(
        self,
        seed: Optional[int] = None,
        num_vehicles: int = 50,
        schema_version: int = 6,
    ):
        """
        Initialize generator.
        
        Args:
            seed: Random seed for reproducibility
            num_vehicles: Number of unique vehicle IDs to simulate
            schema_version: BSM schema version (3, 5, or 6)
        """
        self.rng = random.Random(seed)
        self.schema_version = schema_version
        
        # Generate vehicle IDs
        self.vehicle_ids = [
            f"{self.rng.randint(0, 0xFFFF):04X}{self.rng.randint(0, 0xFFFF):04X}"
            for _ in range(num_vehicles)
        ]
        
        # Track vehicle states for trajectory continuity
        self._vehicle_states: Dict[str, Dict[str, Any]] = {}
    
    def _init_vehicle_state(self, vehicle_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Initialize state for a new vehicle."""
        # Pick random starting point on I-80
        start_idx = self.rng.randint(0, len(WYOMING_COORDS) - 2)
        lat, lon, elev = WYOMING_COORDS[start_idx]
        
        # Add some randomness
        lat += self.rng.uniform(-0.01, 0.01)
        lon += self.rng.uniform(-0.01, 0.01)
        
        return {
            "latitude": lat,
            "longitude": lon,
            "elevation": elev,
            "speed": self.rng.uniform(20, 35),  # m/s (~45-80 mph)
            "heading": self.rng.uniform(80, 100) if self.rng.random() > 0.5 else self.rng.uniform(260, 280),
            "msg_cnt": 0,
            "last_update": timestamp,
            "direction": 1 if self.rng.random() > 0.5 else -1,  # East or West
        }
    
    def _update_vehicle_state(self, vehicle_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Update vehicle state with realistic motion."""
        if vehicle_id not in self._vehicle_states:
            self._vehicle_states[vehicle_id] = self._init_vehicle_state(vehicle_id, timestamp)
        
        state = self._vehicle_states[vehicle_id]
        
        # Time since last update
        dt = (timestamp - state["last_update"]).total_seconds()
        if dt <= 0:
            dt = 0.1  # Minimum time step
        
        # Update position based on speed and heading
        # Simplified: move along longitude (I-80 is roughly E-W)
        speed_mps = state["speed"]
        heading_rad = state["heading"] * 3.14159 / 180
        
        # Approximate degrees per meter at this latitude
        lat_deg_per_m = 1 / 111000
        lon_deg_per_m = 1 / (111000 * 0.75)  # Cos(41°) ≈ 0.75
        
        # Update position
        state["latitude"] += speed_mps * dt * lat_deg_per_m * 0.1 * self.rng.uniform(0.8, 1.2)
        state["longitude"] += speed_mps * dt * lon_deg_per_m * state["direction"] * self.rng.uniform(0.9, 1.1)
        
        # Clamp to valid Wyoming I-80 corridor bounds
        # This prevents vehicles from drifting outside valid coordinate ranges
        state["latitude"] = max(40.5, min(42.5, state["latitude"]))
        state["longitude"] = max(-108.0, min(-104.0, state["longitude"]))
        
        # If vehicle hits boundary, reverse direction (bounce back)
        if state["longitude"] <= -107.9 or state["longitude"] >= -104.1:
            state["direction"] *= -1
        
        # Vary speed slightly
        state["speed"] += self.rng.uniform(-2, 2)
        state["speed"] = max(5, min(40, state["speed"]))  # Clamp to 5-40 m/s
        
        # Vary heading slightly
        state["heading"] += self.rng.uniform(-2, 2)
        
        # Update elevation based on terrain (simplified)
        state["elevation"] += self.rng.uniform(-5, 5)
        state["elevation"] = max(1800, min(2400, state["elevation"]))
        
        # Increment message counter
        state["msg_cnt"] = (state["msg_cnt"] + 1) % 128
        state["last_update"] = timestamp
        
        return state
    
    def generate_bsm(self, timestamp: datetime, vehicle_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a single BSM record.
        
        Args:
            timestamp: Record timestamp
            vehicle_id: Optional specific vehicle ID (random if None)
        
        Returns:
            BSM record matching WYDOT schema
        """
        if vehicle_id is None:
            vehicle_id = self.rng.choice(self.vehicle_ids)
        
        state = self._update_vehicle_state(vehicle_id, timestamp)
        
        # Generate record matching schema version 6
        bsm = {
            "metadata": {
                "bsmSource": self.rng.choice(["RV", "EV"]),
                "logFileName": f"rxMsg_{int(timestamp.timestamp())}_{uuid.uuid4().hex[:8]}.csv",
                "recordType": "rxMsg",
                "securityResultCode": "success",
                "receivedMessageDetails": {
                    "locationData": {
                        "latitude": str(round(state["latitude"], 7)),
                        "longitude": str(round(state["longitude"], 7)),
                        "elevation": str(round(state["elevation"], 1)),
                        "speed": str(round(state["speed"], 2)),
                        "heading": str(round(state["heading"], 4))
                    },
                    "rxSource": "RV"
                },
                "payloadType": "us.dot.its.jpo.ode.model.OdeBsmPayload",
                "serialId": {
                    "streamId": str(uuid.uuid4()),
                    "bundleSize": 1,
                    "bundleId": self.rng.randint(1, 1000),
                    "recordId": self.rng.randint(1, 100),
                    "serialNumber": 0
                },
                "odeReceivedAt": (timestamp + timedelta(seconds=self.rng.uniform(0, 5))).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "schemaVersion": self.schema_version,
                "recordGeneratedAt": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "recordGeneratedBy": "OBU",
                "sanitized": self.rng.random() > 0.1  # 90% sanitized
            },
            "payload": {
                "dataType": "us.dot.its.jpo.ode.plugin.j2735.J2735Bsm",
                "data": {
                    "coreData": {
                        "msgCnt": state["msg_cnt"],
                        "id": vehicle_id,
                        "secMark": int((timestamp.second * 1000 + timestamp.microsecond / 1000) % 60000),
                        "position": {
                            "latitude": round(state["latitude"], 7),
                            "longitude": round(state["longitude"], 7),
                            "elevation": round(state["elevation"], 1)
                        },
                        "accelSet": {
                            "accelLat": round(self.rng.uniform(-2, 2), 2),
                            "accelLong": round(self.rng.uniform(-3, 3), 2),
                            "accelVert": round(self.rng.uniform(-1, 1), 2),
                            "accelYaw": round(self.rng.uniform(-5, 5), 2)
                        },
                        "accuracy": {
                            "semiMajor": round(self.rng.uniform(1, 5), 2),
                            "semiMinor": round(self.rng.uniform(1, 5), 2)
                        },
                        "transmission": self.rng.choice(["NEUTRAL", "PARK", "FORWARDGEARS", "REVERSEGEARS"]),
                        "speed": round(state["speed"], 2),
                        "heading": round(state["heading"] % 360, 4),
                        "brakes": {
                            "wheelBrakes": {
                                "leftFront": self.rng.random() > 0.9,
                                "rightFront": self.rng.random() > 0.9,
                                "unavailable": self.rng.random() > 0.5,
                                "leftRear": self.rng.random() > 0.9,
                                "rightRear": self.rng.random() > 0.9
                            },
                            "traction": "unavailable",
                            "abs": "unavailable",
                            "scs": "unavailable",
                            "brakeBoost": "unavailable",
                            "auxBrakes": "unavailable"
                        },
                        "size": {
                            "width": self.rng.choice([180, 190, 200, 220, 250]),
                            "length": self.rng.choice([400, 450, 500, 550, 600])
                        }
                    },
                    "partII": [
                        {
                            "id": "VehicleSafetyExtensions",
                            "value": {
                                "pathHistory": {
                                    "crumbData": self._generate_path_history(state, timestamp)
                                },
                                "pathPrediction": {
                                    "confidence": round(self.rng.uniform(10, 50), 1),
                                    "radiusOfCurve": round(self.rng.uniform(-100, 100), 1)
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        return bsm
    
    def _generate_path_history(self, state: Dict[str, Any], timestamp: datetime) -> List[Dict[str, Any]]:
        """Generate path history crumbs."""
        crumbs = []
        for i in range(1, self.rng.randint(3, 6)):
            crumbs.append({
                "elevationOffset": round(self.rng.uniform(-5, 5), 1),
                "latOffset": round(self.rng.uniform(-0.001, 0.001), 7),
                "lonOffset": round(self.rng.uniform(-0.002, 0.002), 7),
                "timeOffset": round(i * self.rng.uniform(1.5, 2.5), 2)
            })
        return crumbs
    
    def generate_batch(
        self,
        start_time: datetime,
        num_records: int,
        interval_ms: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Generate a batch of BSM records.
        
        Args:
            start_time: Starting timestamp
            num_records: Number of records to generate
            interval_ms: Approximate interval between records in milliseconds
        
        Returns:
            List of BSM records
        """
        records = []
        current_time = start_time
        
        for i in range(num_records):
            # Vary the interval slightly
            actual_interval = interval_ms * self.rng.uniform(0.8, 1.2)
            
            # Pick a random vehicle for this record
            vehicle_id = self.rng.choice(self.vehicle_ids)
            
            record = self.generate_bsm(current_time, vehicle_id)
            records.append(record)
            
            current_time += timedelta(milliseconds=actual_interval)
        
        return records
    
    def generate_ndjson_file(
        self,
        output_path: Path,
        start_time: datetime,
        num_records: int,
        interval_ms: int = 100
    ) -> int:
        """
        Generate NDJSON file matching S3 bucket format.
        
        Args:
            output_path: Output file path
            start_time: Starting timestamp
            num_records: Number of records
            interval_ms: Interval between records
        
        Returns:
            Number of records written
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            for record in self.generate_batch(start_time, num_records, interval_ms):
                f.write(json.dumps(record) + '\n')
        
        logger.info(f"Generated {num_records} records to {output_path}")
        return num_records
    
    def generate_day_structure(
        self,
        base_dir: Path,
        target_date: date,
        source: str = "wydot",
        message_type: str = "BSM",
        records_per_hour: int = 1000
    ) -> Dict[str, int]:
        """
        Generate a full day's data in S3-like folder structure.
        
        Structure: {base_dir}/{source}/{message_type}/{year}/{month}/{day}/{hour}/
        
        Args:
            base_dir: Base directory for data
            target_date: Date to generate data for
            source: Data source name
            message_type: Message type
            records_per_hour: Number of records per hour
        
        Returns:
            Dict mapping hour to record count
        """
        stats = {}
        
        for hour in range(24):
            # Create directory
            hour_dir = base_dir / source / message_type / str(target_date.year) / f"{target_date.month:02d}" / f"{target_date.day:02d}" / f"{hour:02d}"
            hour_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate file
            start_time = datetime.combine(target_date, datetime.min.time().replace(hour=hour))
            filename = f"{source}-filtered-bsm-{int(start_time.timestamp())}.json"
            filepath = hour_dir / filename
            
            count = self.generate_ndjson_file(filepath, start_time, records_per_hour)
            stats[hour] = count
        
        total = sum(stats.values())
        logger.info(f"Generated {total} records for {target_date} ({source}/{message_type})")
        return stats


def create_test_dataset(
    output_dir: Path,
    num_days: int = 1,
    records_per_hour: int = 100,
    start_date: Optional[date] = None,
    seed: int = 42
) -> Dict[str, Any]:
    """
    Create a complete test dataset.
    
    Args:
        output_dir: Output directory
        num_days: Number of days to generate
        records_per_hour: Records per hour per day
        start_date: Starting date (default: 2021-04-01)
        seed: Random seed
    
    Returns:
        Statistics about generated data
    """
    if start_date is None:
        start_date = date(2021, 4, 1)
    
    generator = SyntheticBSMGenerator(seed=seed)
    
    stats = {
        "output_dir": str(output_dir),
        "start_date": str(start_date),
        "num_days": num_days,
        "records_per_hour": records_per_hour,
        "days": {}
    }
    
    total_records = 0
    current_date = start_date
    
    for day_num in range(num_days):
        day_stats = generator.generate_day_structure(
            output_dir,
            current_date,
            records_per_hour=records_per_hour
        )
        
        day_total = sum(day_stats.values())
        stats["days"][str(current_date)] = {
            "total_records": day_total,
            "hours": day_stats
        }
        total_records += day_total
        
        current_date += timedelta(days=1)
    
    stats["total_records"] = total_records
    
    # Write stats file
    stats_file = output_dir / "generation_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    return stats


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("test_data/synthetic")
    num_days = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    records = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    
    stats = create_test_dataset(output, num_days=num_days, records_per_hour=records)
    print(f"Generated {stats['total_records']} total records")
