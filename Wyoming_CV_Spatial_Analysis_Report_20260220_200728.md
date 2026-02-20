# Wyoming CV BSM Data Spatial Coverage Analysis Report

**Date:** 2026-02-20  
**Task:** cdp-1-1 - Verify source data contains 200km radius records  
**Project:** Connected Driving Simulation Matrix  
**Analyst:** Worker Agent (Subagent)

## Executive Summary

✅ **VERIFICATION COMPLETE:** Wyoming CV data contains sufficient records across all requested radii for meaningful ML training.

## Dataset Analyzed

- **Source:** April_2021_Wyoming_Data_Fixed.csv
- **Total Records:** 13,318,201 raw records  
- **Valid Coordinate Records:** 13,318,200 (99.99% valid)
- **File Size:** 13 GB
- **Geographic Center Point:** (41.538689, -109.319556)

## Spatial Distribution Results

| Radius | Record Count | Percentage | ML Viability |
|--------|--------------|------------|--------------|
| **2km** | 238,744 | 1.79% | ✅ Sufficient (>10K threshold) |
| **100km** | 3,434,980 | 25.79% | ✅ Sufficient (>500K threshold) |
| **200km** | 6,276,427 | 47.13% | ✅ Sufficient (>1M threshold) |

## Key Findings

### ✅ All Acceptance Criteria Met

- [x] **Source data location identified:** April_2021_Wyoming_Data_Fixed.csv (13 GB)
- [x] **Geographic center point established:** (41.538689, -109.319556) - Central Wyoming
- [x] **Distance calculations performed:** Haversine formula applied to all 13.3M records
- [x] **Record counts documented:** All three radii analyzed with precise counts
- [x] **ML viability confirmed:** All radii exceed minimum thresholds for meaningful training
- [x] **Analysis report created:** This document
- [x] **Recommendations provided:** See below

### Data Quality Assessment

- **Coordinate Quality:** 99.99% of records contain valid lat/long coordinates
- **Geographic Coverage:** 47% of all Wyoming CV data falls within 200km of center point
- **Data Density:** Strong representation at all target radii levels

### Additional Dataset Context

Beyond the analyzed April 2021 subset, additional data is available:
- **Full_Wyoming_Data.csv:** 45.7M records (40 GB) - complete Wyoming CV dataset
- **Full_Wyoming_Data_Complete.csv:** 3.7 GB optimized version

## Recommendations for Pipeline Configuration

### 1. **Proceed with 18-Config Matrix:** ✅ GO
The data supports all planned spatial filters:
- **200km radius:** 6.3M records available
- **100km radius:** 3.4M records available  
- **2km radius:** 239K records available

### 2. **Dataset Selection Strategy**
- **For 200km/100km experiments:** Use April_2021_Wyoming_Data_Fixed.csv (sufficient volume)
- **For maximum coverage:** Consider Full_Wyoming_Data.csv (45.7M records)
- **For rapid prototyping:** Full_Wyoming_Data_Complete.csv (3.7 GB, faster processing)

### 3. **ML Training Viability**
All radii have sufficient data for robust ML training:
- **2km:** 239K records (far exceeds 10K minimum)
- **100km:** 3.4M records (far exceeds 500K minimum)  
- **200km:** 6.3M records (far exceeds 1M minimum)

### 4. **Technical Implementation**
- **Memory Management:** Use chunked processing (100K records/chunk)
- **Center Point:** (41.538689, -109.319556) validated from data distribution
- **Coordinate System:** Standard WGS84 lat/long confirmed working

## Risk Assessment

**LOW RISK** for pipeline implementation:
- ✅ Abundant data at all spatial scales
- ✅ High coordinate data quality (99.99% valid)
- ✅ Proven analysis tools and infrastructure
- ✅ Multiple dataset options available

## Next Steps

1. ✅ **Data verification complete** - proceed to pipeline config generation
2. Recommend using April_2021_Wyoming_Data_Fixed.csv as primary dataset
3. Consider Full_Wyoming_Data.csv for maximum experimental coverage
4. Pipeline configs can confidently target all three spatial filters

## Technical Notes

- **Analysis Method:** Haversine distance calculation from geographic center
- **Processing:** Memory-efficient chunked analysis (100K records/chunk)
- **Performance:** ~13M records processed in under 3 minutes
- **Environment:** Jaekel server, Python venv with pandas/numpy

---
**Report Generated:** $(date)  
**Analysis Duration:** ~3 minutes  
**Status:** COMPLETE ✅
