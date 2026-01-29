"""
ProcessedDataManager - Manages processed/attacked data separately from raw cache.

This ensures:
1. Original clean data is never overwritten by attack-injected data
2. Different attack configurations get separate caches
3. ML experiments don't interfere with each other

Cache Structure:
    cache/
    ├── raw/                    # Clean data from CacheManager
    │   └── {source}/{type}/{date}.parquet
    └── processed/              # Attack-injected data
        └── {source}/{type}/{date}/
            └── {config_hash}.parquet
"""

import hashlib
import json
import shutil
from pathlib import Path
from datetime import date
from typing import Dict, Any, Optional, List
import pandas as pd


def compute_config_hash(config: Dict[str, Any]) -> str:
    """
    Compute deterministic hash of a configuration.
    
    This ensures different attack configs get different cache entries.
    
    Args:
        config: Dictionary with attack/ML configuration
        
    Returns:
        8-character hash string
    """
    # Sort keys for deterministic ordering
    config_str = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(config_str.encode()).hexdigest()[:8]


class ProcessedDataManager:
    """
    Manages processed/attacked data with config-aware caching.
    
    Separates:
    - Clean raw data (managed by CacheManager)
    - Processed/attacked data (managed here, per-config)
    """
    
    def __init__(self, cache_dir: Path, config: Dict[str, Any]):
        """
        Initialize ProcessedDataManager.
        
        Args:
            cache_dir: Root cache directory
            config: ML/attack configuration dictionary
        """
        self.cache_dir = Path(cache_dir)
        self.config = config
        self.config_hash = compute_config_hash(config)
        
        # Processed data goes in a separate directory
        self.processed_dir = self.cache_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Store manifest for this config
        self.manifest_path = self.processed_dir / f"manifest_{self.config_hash}.json"
    
    def get_cache_path(self, source: str, msg_type: str, date_val: date) -> Path:
        """
        Get the cache path for processed data.
        
        Path includes config hash to ensure isolation.
        """
        return (
            self.processed_dir / 
            source / 
            msg_type / 
            f"{date_val.year}" /
            f"{date_val.month:02d}" /
            f"{date_val.day:02d}_{self.config_hash}.parquet"
        )
    
    def save_processed(
        self, 
        df: pd.DataFrame, 
        source: str, 
        msg_type: str, 
        date_val: date,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Path:
        """
        Save processed data with config-aware path.
        
        Args:
            df: Processed DataFrame (with attacks injected, etc.)
            source: Data source
            msg_type: Message type
            date_val: Data date
            metadata: Optional additional metadata
            
        Returns:
            Path to saved file
        """
        cache_path = self.get_cache_path(source, msg_type, date_val)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save with metadata
        df.to_parquet(cache_path, index=False)
        
        # Update manifest
        self._update_manifest(source, msg_type, date_val, {
            'path': str(cache_path),
            'rows': len(df),
            'config_hash': self.config_hash,
            'config': self.config,
            'metadata': metadata or {}
        })
        
        return cache_path
    
    def load_processed(
        self, 
        source: str, 
        msg_type: str, 
        date_val: date
    ) -> Optional[pd.DataFrame]:
        """
        Load processed data if it exists for this config.
        
        Returns None if not cached.
        """
        cache_path = self.get_cache_path(source, msg_type, date_val)
        
        if cache_path.exists():
            return pd.read_parquet(cache_path)
        return None
    
    def exists(self, source: str, msg_type: str, date_val: date) -> bool:
        """Check if processed data exists for this config."""
        return self.get_cache_path(source, msg_type, date_val).exists()
    
    def _update_manifest(
        self, 
        source: str, 
        msg_type: str, 
        date_val: date, 
        entry: Dict[str, Any]
    ) -> None:
        """Update the manifest file."""
        manifest = self._load_manifest()
        
        key = f"{source}/{msg_type}/{date_val.isoformat()}"
        manifest['entries'][key] = entry
        manifest['config'] = self.config
        manifest['config_hash'] = self.config_hash
        
        with open(self.manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
    
    def _load_manifest(self) -> Dict:
        """Load manifest from disk."""
        if self.manifest_path.exists():
            with open(self.manifest_path) as f:
                return json.load(f)
        return {
            'config_hash': self.config_hash,
            'config': self.config,
            'entries': {}
        }
    
    def clear_processed(self, source: Optional[str] = None) -> int:
        """
        Clear processed data cache.
        
        Args:
            source: If provided, only clear this source. Otherwise clear all.
            
        Returns:
            Number of files deleted
        """
        count = 0
        
        if source:
            target = self.processed_dir / source
        else:
            target = self.processed_dir
        
        if target.exists():
            for f in target.rglob("*.parquet"):
                f.unlink()
                count += 1
        
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about processed cache."""
        total_files = 0
        total_size = 0
        
        for f in self.processed_dir.rglob("*.parquet"):
            total_files += 1
            total_size += f.stat().st_size
        
        return {
            'config_hash': self.config_hash,
            'total_files': total_files,
            'total_size_mb': total_size / (1024 * 1024),
            'processed_dir': str(self.processed_dir)
        }


class AttackConfig:
    """
    Configuration for attack injection.
    
    This is used to generate unique config hashes for caching.
    """
    
    def __init__(
        self,
        attack_ratio: float = 0.15,
        attack_types: List[str] = None,
        seed: int = 42,
        **kwargs
    ):
        self.attack_ratio = attack_ratio
        self.attack_types = attack_types or ['speed', 'position', 'heading']
        self.seed = seed
        self.extra = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for hashing."""
        return {
            'attack_ratio': self.attack_ratio,
            'attack_types': sorted(self.attack_types),
            'seed': self.seed,
            **self.extra
        }
    
    def get_hash(self) -> str:
        """Get config hash."""
        return compute_config_hash(self.to_dict())
    
    def __repr__(self):
        return f"AttackConfig(ratio={self.attack_ratio}, types={self.attack_types}, hash={self.get_hash()})"


# Example usage
if __name__ == "__main__":
    import tempfile
    
    # Create configs with different attack ratios
    config1 = AttackConfig(attack_ratio=0.15)
    config2 = AttackConfig(attack_ratio=0.20)
    config3 = AttackConfig(attack_ratio=0.15, attack_types=['speed'])
    
    print(f"Config 1: {config1}")
    print(f"Config 2: {config2}")
    print(f"Config 3: {config3}")
    
    # All should have different hashes
    assert config1.get_hash() != config2.get_hash(), "Different ratios should have different hashes"
    assert config1.get_hash() != config3.get_hash(), "Different attack types should have different hashes"
    
    # Test ProcessedDataManager
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr1 = ProcessedDataManager(Path(tmpdir), config1.to_dict())
        mgr2 = ProcessedDataManager(Path(tmpdir), config2.to_dict())
        
        # Paths should be different
        path1 = mgr1.get_cache_path("wydot", "BSM", date(2021, 4, 1))
        path2 = mgr2.get_cache_path("wydot", "BSM", date(2021, 4, 1))
        
        print(f"\nPath 1: {path1}")
        print(f"Path 2: {path2}")
        
        assert path1 != path2, "Different configs should have different paths"
    
    print("\n✓ All cache isolation tests passed!")
