# 📁 DataEngineer-InterviewPreparation

This repository is a structured collection of real-world PySpark and SQL coding problems aimed at preparing for data engineering interviews. Each day includes 5 problems with matching PySpark and SQL implementations, clean datasets, and GitHub-ready structure.

---

## ✅ Folder Structure

```
DataEngineer-InterviewPreparation/
├── Day1/
│   ├── data/             # CSV files for input datasets
│   ├── pyspark/          # PySpark scripts for Day 1 problems
│   └── sql/              # SQL solutions for Day 1 problems
├── Day2/
│   ├── data/
│   ├── pyspark/
│   └── sql/
└── README.md             # Repository overview
```

---

## 📆 Day-wise Breakdown

### 📅 Day 1: Core Data Transformations

| # | Problem                       | Focus Area              |
| - | ----------------------------- | ----------------------- |
| 1 | First Purchase Per Customer   | ROW\_NUMBER + Partition |
| 2 | Highest Salary Per Department | DENSE\_RANK / Ties      |
| 3 | Consecutive Active Days       | DATEDIFF + Row Logic    |
| 4 | Running Total of Sales        | SUM over Window         |
| 5 | Customers with No Orders      | LEFT ANTI JOIN / NOT IN |

### 📅 Day 2: Advanced Joins, Ranking & Aggregates

| # | Problem                                    | Focus Area                      |
| - | ------------------------------------------ | ------------------------------- |
| 1 | Second Highest Transaction per User        | DENSE\_RANK                     |
| 2 | Users with Exactly 2 Purchase Days         | COUNT DISTINCT + Group Filter   |
| 3 | First and Last Login Timestamp             | MIN / MAX Aggregates            |
| 4 | Products That Were Never Sold              | LEFT JOIN IS NULL / ANTI JOIN   |
| 5 | Revenue Contribution % Per Product per Day | SUM + Ratio + ROUND over Window |

---

## 🛠 Technologies Used

* **PySpark**: DataFrame API, window functions, aggregation, joins
* **SQL**: ANSI SQL (MySQL-style)
* **Git**: Structured commits by day and topic

---

## 💡 How to Use

1. Clone the repo
2. Navigate to any `DayX/pyspark/` or `sql/` folder
3. Run in PySpark or SQL of your choice
4. Reuse data files under `/data` for practice

---

## 📢 Contributions & Feedback

Suggestions and improvements are welcome! This project is built for interview prep and learning — feel free to fork and extend it.

---

## 🔗 Author & Community

Maintained by **Anand Kumar Singh** for personal and public learning. Connect on [LinkedIn](www.linkedin.com/in/anand-kumar-singh-830839ab) or star the repo to support more content!
