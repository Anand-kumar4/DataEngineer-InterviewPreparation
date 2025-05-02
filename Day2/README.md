# 📅 Day 2 – Advanced PySpark & SQL Interview Practice

This folder contains 5 curated data engineering problems solved using both **PySpark** and **SQL**. These problems are designed to practice ranking, aggregation, window functions, and anti-joins commonly seen in interviews.

---

## 🧩 Problems Covered

| # | Problem Description                        | Focus Area                    |
| - | ------------------------------------------ | ----------------------------- |
| 1 | Second highest transaction per user        | DENSE\_RANK + Partition       |
| 2 | Users with exactly 2 purchase days         | COUNT DISTINCT + Group Filter |
| 3 | First and last login timestamp per user    | MIN / MAX Aggregates          |
| 4 | Products that were never sold              | LEFT ANTI JOIN / NOT IN       |
| 5 | Revenue % contribution per product per day | SUM over Window + Ratio       |

---

## 📂 Folder Structure

```
Day2/
├── data/              # CSV input files
│   ├── transactions.csv
│   ├── user_txns.csv
│   ├── logins.csv
│   ├── products.csv
│   ├── sales.csv
│   └── daily_sales.csv
├── pyspark/           # PySpark solutions
│   ├── second_highest_transaction.py
│   ├── users_with_two_txn_days.py
│   ├── first_last_login.py
│   ├── products_never_sold.py
│   └── revenue_contribution.py
└── sql/               # SQL solutions
    ├── second_highest_transaction.sql
    ├── users_with_two_txn_days.sql
    ├── first_last_login.sql
    ├── products_never_sold.sql
    └── revenue_contribution.sql
```

---

## 🛠 Tools Used

* PySpark DataFrame API
* SQL (MySQL-style)
* Window functions: DENSE\_RANK, SUM OVER
* Joins, Aggregations, Date Logic

---

## 💡 Usage

Run these solutions either in your PySpark environment or MySQL workbench.

Feel free to explore, fork, and adapt for your own interview prep or blog content.

---

**Author:** Anand Kumar Singh
**LinkedIn:** [https://www.linkedin.com/in/anand-kumar-singh/](https://www.linkedin.com/in/anand-kumar-singh/)
