# 📊 Data Processing Task – Tyroo

## ✅ Goal

Efficiently download, clean, transform, and store a large CSV dataset into a SQL database using Python and Pandas.

---

## 🚀 Tech Stack

- Python 3.8+
- Pandas
- SQLite (or any SQL database)
- Logging

---

## 📂 File Structure

- `process_csv.py`: Main data pipeline script
- `schema.sql`: SQL schema for database
- `data.db`: SQLite database (generated after running the script)
- `data_processing.log`: Log file (auto-created)
- `README.md`: Setup guide

---

## ⚙️ Setup Instructions

1. **Clone Repo / Download Files**

2. **Install Requirements**
   ```bash
   pip install pandas requests
   ```

3. **Run Script**
   ```bash
   python process_csv.py
   ```

4. **Output**
   - Data stored in `data.db`
   - Logs saved in `data_processing.log`

---

## 🧹 Transformations Applied

- Column names normalized (lowercase, underscores)
- Dropped rows with missing emails
- Converted age to integer (invalid to NaN)
- Cleaned whitespace and lowercase emails

---

## 🐛 Error Handling

- Logs all failures to `data_processing.log`
- Graceful failure on download/processing issues

---

## 📈 Performance Optimizations

- Chunked reading with `chunksize=10000` to avoid memory overload

---

## 📫 Contact

For queries: [iitg.prabhat@gmail.com](mailto:iitg.prabhat@gmail.com)
# Tyroo_Data_Processing
