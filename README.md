\# End-to-End Supply Chain Data Warehouse 🚀

\*\*Architected by: Azeez (Bioinformatics @ Vignan University)\*\*

\## 📌 Project Overview

An automated ETL pipeline built in PostgreSQL using the \*\*Medallion Architecture\*\*. This project transforms raw logistics data into a 3NF Star Schema to provide actionable business insights.

\## 🏗️ Architecture (Medallion Model)

\- \*\*Bronze (Staging):\*\* Raw ingestion with metadata (Audit columns) and fault-tolerant TEXT schema.

\- \*\*Silver (Warehouse):\*\* Normalized Dimensions and Fact tables. Enforced Referential Integrity and Type Casting.

\- \*\*Gold (Analytics):\*\* Business-ready Views for executive reporting.

\- \*\*Diamond (Optimization):\*\* B-Tree indexing and performance tuning.

\## 🛠️ Key Technical Features

\- \*\*Automation:\*\* PL/pgSQL Stored Procedure for one-click ETL refreshes.

\- \*\*Normalization:\*\* 3rd Normal Form (3NF) to eliminate data redundancy.

\- \*\*Data Integrity:\*\* Primary/Foreign Key constraints and regex-based data profiling.

\- \*\*Performance:\*\* Optimized query execution via \`EXPLAIN ANALYZE\`.

\## 🚀 How to Run

1\. Execute scripts in \`/scripts\` folder in order (01 to 04).

2\. Use \`CALL silver.sp\_refresh\_warehouse();\` to trigger the pipeline.

3\. Query \`gold.vw\_product\_profitability\` for insights.