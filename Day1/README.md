# 📅 Day 1 – Core PySpark & SQL Interview Practice

This folder contains 5 foundational data engineering problems solved using both **PySpark** and **SQL**. These problems focus on essential transformations like ranking, aggregation, joins, and date logic commonly asked in interviews.

---

## 🧩 Problems Covered

| # | Problem Description               | Focus Area                     |
| - | --------------------------------- | ------------------------------ |
| 1 | First purchase per customer       | ROW\_NUMBER + Partition        |
| 2 | Highest salary per department     | DENSE\_RANK + Tie Handling     |
| 3 | Consecutive active days detection | DATEDIFF + Row Number Grouping |
| 4 | Running total of sales            | SUM OVER Window                |
| 5 | Customers with no orders          | LEFT ANTI JOIN / NOT IN        |

---

## 📂 Folder Structure

```
Day1/
├── data/              # CSV input files
│   ├── transactions.csv
│   ├── employees.csv
│   ├── user_activity.csv
│   ├── sales.csv
│   ├── customers.csv
│   └── orders.csv
├── pyspark/           # PySpark solutions
│   ├── firstpurchasepercustomer.py
│   ├── highestsalaryperdept.py
│   ├── consecutiveactivedays.py
│   ├── runningtotalofsales.py
│   └── customerswithnoorders.py
└── sql/               # SQL solutions
    ├── firstpurchasepercustomer.sql
    ├── highestsalaryperdept.sql
    ├── consecutiveactivedays.sql
    ├── runningtotalofsales.sql
    └── customerswithnoorders.sql
```

---

## 🛠 Tools Used

* PySpark DataFrame API
* SQL (MySQL-style)
* Window functions: ROW\_NUMBER, DENSE\_RANK, SUM
* Joins, Aggregations, and Date Logic

---

## 💡 Usage

These problems can be executed in any Spark environment or SQL engine. Designed for interview preparation, concept reinforcement, or blog content.

Feel free to fork or adapt the solutions for your own use case.

---

**Author:** Anand Kumar Singh
**LinkedIn:** [www.linkedin.com/in/anand-kumar-singh-830839ab](https://www.linkedin.com/in/anand-kumar-singh-830839ab)
