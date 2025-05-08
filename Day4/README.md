


# Day 4 – PySpark + SQL Interview Practice

This folder contains daily coding problems focused on **PySpark** and **SQL**, covering transformations, aggregations, and conditional logic often asked in Data Engineering interviews.

## ✅ Problems Covered

1. **Group Books per User**  
   📄 `books_purchased.csv`  
   🧠 Use: `collect_list`, `concat_ws`

2. **Users with All NULL Purchases**  
   📄 `books_purchased_problem2.csv`  
   🧠 Use: `count`, `isNull`, `having count(*) = 0`

3. **Top Region by Average Revenue per Product**  
   📄 `sales_region.csv`  
   🧠 Use: `groupBy`, `avg`, `dense_rank`

4. **Flag High vs Low Value Transactions**  
   📄 `transactions_flag.csv`  
   🧠 Use: `when`, `otherwise`, `CASE WHEN`

5. **Customers with Exactly 2 Distinct Purchase Amounts**  
   📄 `distinct_amounts.csv`  
   🧠 Use: `countDistinct`, `having = 2`

6. **First Purchase per Customer**  
   📄 `first_purchases.csv`  
   🧠 Use: `row_number`, `orderBy`, `partitionBy`

7. **Customers with Repeat Purchases (Same Product)**  
   📄 `repeat_purchases.csv`  
   🧠 Use: `groupBy`, `count > 1`

## 📁 Folder Structure

```
Day4/
├── data/
│   ├── books_purchased.csv
│   ├── books_purchased_problem2.csv
│   ├── sales_region.csv
│   ├── transactions_flag.csv
│   ├── distinct_amounts.csv
│   ├── first_purchases.csv
│   └── repeat_purchases.csv
├── pyspark/
│   ├── group_books_per_user.py
│   ├── users_with_all_null_purchases.py
│   ├── top_region_by_avg_revenue.py
│   ├── flag_high_low_transactions.py
│   ├── customers_with_two_distinct_amounts.py
│   ├── first_purchase_per_customer.py
│   └── customers_with_repeat_purchases.py
├── sql/
│   ├── group_books_per_user.sql
│   ├── users_with_all_null_purchases.sql
│   ├── top_region_by_avg_revenue.sql
│   ├── flag_high_low_transactions.sql
│   ├── customers_with_two_distinct_amounts.sql
│   ├── first_purchase_per_customer.sql
│   └── customers_with_repeat_purchases.sql
```

## 🧠 Goal

Practice hands-on PySpark and SQL transformations, filtering, grouping, window functions, and more — exactly the kinds of logic expected in real-world data engineering interviews.