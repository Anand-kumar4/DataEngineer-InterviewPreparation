# Day 5 – PySpark + SQL Interview Practice (Advanced)

This folder contains advanced and medium-complexity interview-style coding questions solved using Apache Spark (PySpark), SQL (MySQL-compatible), and DataFrame operations.

## ✅ Problems Covered

1. **Find Second Purchase Date for Each Customer**  
   📄 `second_purchase.csv`  
   🧠 Use: ROW_NUMBER + filtering

2. **Compute Running Average Revenue per Product**  
   📄 `sales_avg.csv`  
   🧠 Use: AVG() over window

3. **Identify Customers with Consecutive Purchase Days**  
   📄 `consecutive_purchases.csv`  
   🧠 Use: DATEDIFF + ROW_NUMBER + grouping

4. **Top 2 Products by Total Revenue per Region**  
   📄 `top_products.csv`  
   🧠 Use: SUM + DENSE_RANK

5. **Flatten Multi-Item Transactions into Rows**  
   📄 `multi_item_txns.csv`  
   🧠 Use: explode() (PySpark), UNNEST/FLATTEN (SQL-like)

6. **Fill in Missing Dates with Zero Revenue**  
   📄 `revenue_with_gaps.csv`  
   🧠 Use: sequence() + explode() + join()

7. **Detect Inactive (Churned) Customers Based on 30-Day Inactivity**  
   📄 `churn_check.csv`  
   🧠 Use: MAX(purchase_date) + date_diff

## 📁 Folder Structure

```
Day5/
├── data/
│   ├── second_purchase.csv
│   ├── sales_avg.csv
│   ├── consecutive_purchases.csv
│   ├── top_products.csv
│   ├── multi_item_txns.csv
│   ├── revenue_with_gaps.csv
│   └── churn_check.csv
├── pyspark/
│   ├── second_purchase_date.py
│   ├── running_avg_sales.py
│   ├── consecutive_purchase_streaks.py
│   ├── top_n_products_by_revenue.py
│   ├── explode_multi_item_txns.py
│   ├── fill_missing_dates.py
│   └── churned_customers.py
├── sql/
│   ├── second_purchase_date.sql
│   ├── running_avg_sales.sql
│   ├── consecutive_purchase_streaks.sql
│   ├── top_n_products_by_revenue.sql
│   ├── explode_multi_item_txns.sql
│   ├── fill_missing_dates.sql
│   └── churned_customers.sql
```

## 🧠 Focus Areas

This set focuses on:
- Advanced windowing and ranking
- Time-based pattern detection
- Array flattening and structural transformation
- Filling gaps and identifying inactivity (churn)
- Data cleanup and transformation (e.g., handling string arrays)
- Recursive SQL simulation alternatives (e.g., in explode)

Perfect for interview prep and strengthening PySpark + SQL transformation logic!