# 📊 Regulatory Reporting Simulation

• Simulated US regulatory reports (Y-9C, Y-14, 2052a) using SQL in Azure Data Studio to map financial data to compliance reporting
formats.

• Stored and transformed synthetic banking data in Hadoop HDFS to enable scalable data preparation and validation workflows.

---

## 🚀 Overview

This project replicates the process of transforming financial datasets into standardized regulatory reports using:

- SQL scripting in **Azure Data Studio**
- Scalable processing with **Apache Spark (PySpark)**
- A local simulation of **Hadoop HDFS**

---

## 🛠️ Tools & Technologies

- **Azure Data Studio** – SQL-based data generation
- **Apache Spark (PySpark)** – Distributed data processing
- **Python 3.10+**
- **Simulated HDFS** – Folder-based structure for Hadoop-like workflow

---
## ▶️ How to Run the Project

1. **Prepare Data**  
   Export SQL output to CSVs and place them inside:
simulated_hdfs/user/bank/raw_data/


### 2. **Activate Environment**
```bash
conda activate spark-env




✅ Sample Reports Generated
Y-9C Summary – Total balances by account type

Y-14 Summary – Risk-weighted balances by loan type

FR 2052a Summary – Net liquidity flow by source type


