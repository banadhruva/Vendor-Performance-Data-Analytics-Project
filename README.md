# 📊 Vendor Performance Data Analytics Project

This repository showcases an **end-to-end data analytics pipeline** designed to optimize vendor performance. It illustrates the complete data lifecycle—from raw ingestion and cleaning to advanced statistical modeling and interactive business intelligence.

---

## 🚀 Project Overview

The primary goal of this project is to analyze vendor metrics to identify trends that drive **profitability**, **inventory efficiency**, and **strategic sourcing**.

### Why this project stands out:
* **Real-World Scenario:** Solves a complex business problem (vendor management) rather than using generic datasets.
* **Full-Stack Integration:** Demonstrates a seamless workflow between **Python**, **SQL**, and **Power BI**.
* **Actionable Intelligence:** Focuses on delivering "So What?" insights that lead to better business decisions.



---

## ✨ Features

* **Automated Data Ingestion:** Python scripts to transform flat CSV files into a structured SQLite relational database.
* **Exploratory Data Analysis (EDA):** In-depth cleaning and feature engineering (e.g., GrossProfit, StockTurnover).
* **Statistical Analysis:** Advanced hypothesis testing (T-tests) to validate vendor performance gaps.
* **Interactive Dashboard:** A high-fidelity Power BI report for dynamic data exploration.
* **Automated Logging:** Tracks the ingestion process for debugging and pipeline health.

---

## 🛠️ Technologies Used

### **Data Processing & Analytics**
* **Python:** The core engine for ETL and analysis.
    * `pandas` & `numpy`: Data manipulation.
    * `sqlite3` & `sqlalchemy`: Database management.
    * `scipy.stats`: Statistical testing.
    * `matplotlib` & `seaborn`: Exploratory visualization.
* **SQLite:** A lightweight, file-based SQL database for structured storage.

### **Visualization & Reporting**
* **Power BI Desktop:** For building interactive, executive-level dashboards.
* **Jupyter Notebooks:** For documented, reproducible research.

---

## 📂 Project Structure & Usage

To replicate this project, follow these steps in order:

### 1. Data Ingestion (`ingestion.ipynb`)
* **Action:** Execute all cells.
* **Result:** Reads raw CSVs from `/dataset`, creates `inventory.db`, and generates a `logs/ingestion_db.log`.

### 2. Exploratory Data Analysis (`Exploratory_Data_Analysis.ipynb`)
* **Action:** Execute all cells.
* **Result:** Performs data cleaning and creates the `vendor_sales_summary` table in SQL, featuring engineered metrics like **ProfitMargin** and **SalesToPurchaseRatio**.

### 3. Statistical Analysis (`Vendor_Performance_Analysis.ipynb`)
* **Action:** Execute all cells.
* **Result:** Validates findings through statistical significance testing to ensure insights aren't just due to random noise.

### 4. Interactive Dashboard (`Vendor_Summary_Dashboard.pbix`)
* **Action:** Open in Power BI Desktop.
* **Note:** You may need to update the data source: 
    * *File > Options and settings > Data source settings > Change Source* * Point it to your local `inventory.db` file.

---

## 💡 Key Insights & Recommendations

This project answers critical business questions:
* **Vendor Tiering:** Who are the top 10% and bottom 10% performers?
* **Margin Optimization:** Which product categories offer the highest ROI?
* **Inventory Health:** Identifying vendors with low stock turnover that may be tying up capital.



---

## 🔗 Resources
* **Dataset:** [Vendor Performance Analysis on Kaggle](https://www.kaggle.com/datasets/vivekkumarkamat/vendor-performance-analysis)
* **Tool:** [Download Power BI Desktop](https://www.microsoft.com/en-us/download/details.aspx?id=58494)
