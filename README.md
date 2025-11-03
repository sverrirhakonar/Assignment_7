# 🧮 Parallel Financial Computation (Assignment 7)

This project implements **parallelized financial computations** using **Pandas**, **Polars**, and Python’s **threading** and **multiprocessing** libraries.  
It benchmarks performance across libraries and execution models while profiling CPU usage, memory consumption, and runtime.  
Finally, it performs **portfolio aggregation** recursively across sub-portfolios and positions.

---

## ⚙️ Environment Setup

### Create and activate the Conda environment
```bash
conda env create -f environment.yml
conda activate finmpy
(Alternative) Install via pip
bash
Copy code
pip install -r requirements.txt
Project Structure
bash
Copy code
Assignment_7/
│
├── data/
│   ├── market_data-1.csv
│   └── portfolio_structure-1.json
│
├── parallel_fin/
│   ├── __init__.py
│   ├── data_loader.py           # Loads market data (Pandas & Polars)
│   ├── metrics.py               # Rolling metrics & profiling
│   ├── parallel.py              # Threading & multiprocessing logic
│   ├── portfolio.py             # Portfolio aggregation (sequential & parallel)
│
├── tests/
│   └── test_portfolio.py        # Unit tests for portfolio aggregation
│
├── benchmark_all.py             # Runs all benchmarks + generates JSON summary
├── benchmark_results.json       # Output file with profiling results
├── main.py                      # Demonstration script (end-to-end run)
├── performance.md               # Performance analysis & comparison report
├── requirements.txt             # pip dependencies
└── environment.yml              # Conda environment definition
How to Run
1. Run Full Benchmarks
Measures runtime, memory, and CPU usage across:

Pandas vs. Polars

Threaded vs. Multiprocess execution

Sequential vs. Parallel portfolio aggregation

bash
Copy code
python benchmark_all.py
Results are printed to the console and saved in:

pgsql
Copy code
benchmark_results.json
2. Run Unit Tests
Verifies the correctness of the portfolio aggregation logic:

bash
Copy code
pytest -v
3. Run the Main Demonstration
Executes a complete pipeline run on the provided dataset:

bash
Copy code
python main.py
Outputs
benchmark_results.json – Full profiling data

performance.md – Summary and interpretation of results

Console output – Detailed benchmark logs for ingestion, rolling metrics, and portfolio aggregation

Key Features
Parallel computation: ThreadPoolExecutor and ProcessPoolExecutor implementations

High-performance data handling: Comparison of Pandas and Polars

Resource profiling: Tracks time, memory, and CPU usage

Recursive aggregation: Combines portfolio and sub-portfolio metrics

Testing coverage: Validates computation accuracy

Authors
Sverrir Hakonarson
Robert Vilhjalmur Asgeirsson