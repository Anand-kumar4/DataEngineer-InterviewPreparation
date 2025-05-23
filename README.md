# 📁 DataEngineer-InterviewPreparation

This repository is a structured collection of real-world PySpark and SQL coding problems aimed at preparing for data engineering interviews. Each day includes 5 problems with matching PySpark and SQL implementations, clean datasets, and GitHub-ready structure.

It includes use of Apache Spark (PySpark), SQL (MySQL-compatible), and common DataFrame operations such as joins, aggregations, window functions, and more.

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
├── Day3/
│   ├── data/
│   ├── pyspark/
│   └── sql/
├── Day4/
│   ├── data/
│   ├── pyspark/
│   └── sql/
├── Day5/
│   ├── data/
│   ├── pyspark/
│   └── sql/
├── Day6/
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

### 📅 Day 3: Multi-Row Analysis & Comparison

| # | Problem                                             | Focus Area                          |
| - | --------------------------------------------------- | ----------------------------------- |
| 1 | Latest 2 Orders per Customer                        | ROW\_NUMBER + ORDER BY DESC         |
| 2 | Transaction Comparison with Strictly Increasing     | LAG + SUM + Boolean Aggregation     |
| 3 | Products Never Purchased                            | LEFT ANTI JOIN                      |
| 4 | Top-Selling Product per Region                      | DENSE\_RANK over SUM Partition      |

### 📅 Day 4: Grouping, Flags, First Events, and Deduplication

| # | Problem                                         | Focus Area                          |
| - | ----------------------------------------------- | ----------------------------------- |
| 1 | Group Books per User                            | GROUP BY + COLLECT_LIST             |
| 2 | Users with All NULL Purchases                   | COUNT NULL Logic                    |
| 3 | Top Region by Avg Revenue per Product           | DENSE_RANK + AVG over group         |
| 4 | Flag High vs Low Value Transactions             | CASE WHEN / WHEN + OTHERWISE        |
| 5 | Customers with Exactly 2 Distinct Amounts       | COUNT DISTINCT with filter          |
| 6 | First Purchase per Customer                     | ROW_NUMBER over partition           |
| 7 | Customers Who Bought Same Product More Than Once| GROUP BY + COUNT > 1                |

### 📅 Day 5: Advanced Date & String Use Cases

| # | Problem                                                | Focus Area                                |
| - | ------------------------------------------------------ | ----------------------------------------- |
| 1 | Find Second Purchase Date per Customer                 | ROW_NUMBER + partition                    |
| 2 | Compute Running Average Revenue per Product            | AVG + Window + cumulative range           |
| 3 | Consecutive Purchase Streaks per Customer              | Row logic + DATE_DIFF + group detection   |
| 4 | Top 2 Products by Total Revenue per Region             | DENSE_RANK + SUM over group               |
| 5 | Flatten Multi-Item Transactions into Rows              | SPLIT + EXPLODE + TRIM                    |
| 6 | Fill in Missing Dates with Zero Revenue                | DATE SEQUENCE + LEFT JOIN + COALESCE      |
| 7 | Detect Churned Customers Based on 30-Day Inactivity    | MAX(date) + DATE_DIFF + filter condition  |

### 📅 Day 6: Real-World Scenarios & Sessionization

| # | Problem                                             | Focus Area                                |
| - | --------------------------------------------------- | ----------------------------------------- |
| 1 | Monthly Top Spenders                                | GROUP BY + SUM + RANK                     |
| 2 | Order-to-Delivery Duration                          | DATEDIFF between two columns              |
| 3 | Join Orders with First Product Purchased            | ROW_NUMBER join on customer_id            |
| 4 | Category-wise Revenue % Contribution                | SUM + JOIN + ratio + ROUND                |
| 5 | Repeat Buyers Within a Week                         | LAG + DATEDIFF + DISTINCT FILTER          |
| 6 | Sessionize Web Events                               | LAG + TIMESTAMPDIFF + SUM() OVER          |
| 7 | Product Category Popularity Trends                  | LAG + Revenue Trend Logic                 |



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

Maintained by **Anand Kumar Singh** for personal and public learning. Connect on [LinkedIn](https://www.linkedin.com/in/anand-kumar-singh-830839ab) or star the repo to support more content!
