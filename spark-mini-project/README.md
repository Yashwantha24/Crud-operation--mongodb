## Spark Mini Project: Customer Orders Analytics

A small, self-contained PySpark project that reads sample customer and order data, performs common transformations (schema cleanup, filters), analytics (aggregations, joins), and writes results to CSV and Parquet. Designed for quick demos, labs, or a college mini-project on distributed data processing.

### Folder Structure
- `src/job.py` — main PySpark job with detailed comments.
- `data/customers.json` — sample customers dataset.
- `data/orders.csv` — sample orders dataset.
- `output/` — where the job writes CSV/Parquet results (created automatically).

### Prerequisites
- **Java**: JDK 8 or later.
- **Python**: 3.8+.
- **Apache Spark**: 3.x binary for Hadoop (pre-built).
- **Pip packages**: `pyspark` (optional if using Spark distribution) and `python-dotenv` (only if you add `.env` support).

### Step-by-Step Setup (Windows-friendly)
1) **Install Java**
   - Download an OpenJDK (e.g., Adoptium Temurin 11).
   - Install to `C:\Java\jdk-11`.
   - Set env var (PowerShell):
     - `setx JAVA_HOME "C:\Java\jdk-11"`
     - `setx PATH "%PATH%;%JAVA_HOME%\bin"`

2) **Install Apache Spark**
   - Download Spark 3.x pre-built for Hadoop from https://spark.apache.org/downloads.html.
   - Extract to `C:\spark`.
   - Set env vars:
     - `setx SPARK_HOME "C:\spark"`
     - `setx PATH "%PATH%;%SPARK_HOME%\bin"`

3) **Python environment**
   - Create/activate a virtual env (optional but recommended):
     - `python -m venv .venv`
     - `.venv\Scripts\activate`
   - Install packages:
     - `pip install pyspark`
     - (Optional) `pip install python-dotenv`

4) **Verify installs**
   - `java -version`
   - `spark-shell --version` or `spark-submit --version`
   - `python -c "import pyspark; print(pyspark.__version__)"`.

### How to Run the Spark Job
From the project root (`spark-mini-project/`):
- Using local Spark distribution:
  - `spark-submit src/job.py`
- If relying on the PyPI `pyspark` wheel only:
  - `python src/job.py`

Optional args:
- `--master local[*]` to set cores explicitly (defaults to local in the script).
- `--output-base output` to change the output folder.

### What the Job Does (high-level)
- Loads customers (JSON) and orders (CSV) into DataFrames.
- Cleans and casts schemas.
- Filters orders to a recent window and positive amounts.
- Aggregates order totals by customer and by country.
- Joins customer metadata with order totals.
- Writes:
  - `output/customers_enriched.parquet` (Parquet)
  - `output/orders_by_country.csv` (CSV with header)
  - `output/sample_action_counts.txt` (action results like counts)
- Shows sample rows in the console for quick inspection.

### Expected Outputs (described)
- **Console**: printed schemas, first 5 rows of customers, enriched customer totals, top countries by revenue, and row/action counts.
- **Files**:
  - `output/customers_enriched.parquet/` — Parquet files with columns `customer_id, name, country, signup_date, total_amount, order_count`.
  - `output/orders_by_country.csv/` — CSV part files with `country, total_amount, order_count`.
  - `output/sample_action_counts.txt` — small text file summarizing counts.

### Screenshot Ideas
- Terminal running `spark-submit src/job.py` with the `show()` outputs.
- File explorer showing `output/` with Parquet/CSV results.

### Next Steps / Extensions
- Replace sample data with your own and adjust schemas.
- Add window functions (e.g., top-N per country).
- Schedule via cron/Airflow, or containerize with Docker if desired.

