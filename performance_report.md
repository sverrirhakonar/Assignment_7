# Performance Benchmark Report

## 1. Overview
This benchmark compares **Pandas** and **Polars** performance across four main tasks in the parallel finance project:

1. **Data Ingestion** – loading raw CSV data.  
2. **Rolling Metrics** – computing 20-period moving averages and volatilities.  
3. **Parallel Execution** – comparing threaded vs. multiprocessing in Pandas.  
4. **Portfolio Aggregation** – computing portfolio-level metrics sequentially and in parallel.

All tests were performed on the same dataset (`market_data-1.csv`) and portfolio configuration (`portfolio_structure-1.json`).

---

## 2. System Environment
- **Python:** 3.11 (Anaconda)  
- **Packages:** pandas, polars, psutil, concurrent.futures  
- **Machine:** local CPU with multiple cores (no GPU)  
- **Operating System:** Windows 11  
- **Environment:** `finmpy` conda environment  

---

## 3. Benchmark Results

### 🧠 Data Ingestion
| Library | Time (s) | Memory Δ (MB) | CPU (%) |
|----------|-----------|---------------|----------|
| **Pandas** | 0.491 | 10.38 | 18.75 |
| **Polars** | 0.129 | 39.83 | 32.6 |

**Observation:**  
Polars is ~3.8× faster than Pandas for ingestion, benefiting from native Rust multithreading.  
However, it uses ~4× more memory, typical due to internal parallel columnar buffers.

---

### 📊 Rolling Metrics
| Library | Time (s) | Memory Δ (MB) | CPU (%) |
|----------|-----------|---------------|----------|
| **Pandas** | 0.179 | 13.57 | 0.0 |
| **Polars** | 0.036 | 47.73 | 0.0 |

**Observation:**  
Polars again outperforms Pandas by ~5× in rolling metric computations.  
The higher memory footprint reflects Polars’ temporary column caching and parallel windows.

---

### ⚙️ Parallel Performance (Pandas Only)
| Method | Time (s) | Memory Δ (MB) | CPU (%) |
|---------|-----------|---------------|----------|
| **Threaded** | 0.095 | 15.65 | 0.0 |
| **Multiprocess** | 1.225 | 16.69 | 0.0 |

**Observation:**  
Threading is significantly faster here because the tasks are mostly I/O bound or release the GIL effectively.  
Multiprocessing adds inter-process communication overhead, making it slower.

---

### 💼 Portfolio Aggregation
| Method | Time (s) | Memory Δ (MB) | CPU (%) |
|---------|-----------|---------------|----------|
| **Sequential** | 2.275 | 21.95 | 19.25 |
| **Multiprocess** | 2.386 | 1.55 | 0.0 |

**Observation:**  
Multiprocessing yields no real gain because the workload per portfolio is small and the data copying overhead dominates.  
Sequential aggregation remains more efficient for smaller portfolio structures.

---

## 4. Summary

| Category | Best Performer | Speed-up vs Pandas | Notes |
|-----------|----------------|--------------------|-------|
| Data Ingestion | **Polars** | ~3.8× faster | Higher memory usage |
| Rolling Metrics | **Polars** | ~5× faster | Efficient columnar parallelism |
| Parallelization | **Threaded Pandas** | ~13× faster than multiprocessing | Lightweight overhead |
| Portfolio Aggregation | **Sequential Pandas** | Slightly faster | Multiprocessing not beneficial for small portfolios |

---

## 5. Key Takeaways
- **Polars** dominates in pure computation (ingestion & rolling metrics).  
- **Pandas threading** is surprisingly efficient when the workload fits the GIL model.  
- **Multiprocessing** adds overhead unless each task is heavy.  
- **Memory usage** scales differently: Polars trades higher memory for speed.  
- Overall, **Polars + lightweight threading** provides the best balance of performance and simplicity.

---

## 6. Benchmark JSON
The raw performance output (saved as `benchmark_results.json`):

```json
{
    "ingestion": {
        "pandas": {"time_sec": 0.491, "mem_diff_mb": 10.375, "cpu_percent": 18.75},
        "polars": {"time_sec": 0.1291, "mem_diff_mb": 39.8281, "cpu_percent": 32.6}
    },
    "rolling": {
        "pandas": {"time_sec": 0.1788, "mem_diff_mb": 13.5742, "cpu_percent": 0.0},
        "polars": {"time_sec": 0.0357, "mem_diff_mb": 47.7344, "cpu_percent": 0.0}
    },
    "parallel": {
        "pandas_thread": {"time_sec": 0.0953, "mem_diff_mb": 15.6523, "cpu_percent": 0.0},
        "pandas_multiprocess": {"time_sec": 1.2254, "mem_diff_mb": 16.6914, "cpu_percent": 0.0}
    },
    "portfolio": {
        "sequential": {"time_sec": 2.2753, "mem_diff_mb": 21.9453, "cpu_percent": 19.25},
        "multiprocess": {"time_sec": 2.3863, "mem_diff_mb": 1.5469, "cpu_percent": 0.0}
    }
}
