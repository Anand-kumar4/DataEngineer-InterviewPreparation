# 📘 Day 6 – PySpark + SQL Interview Practice

As part of my ongoing data engineering interview preparation, here are 7 advanced and medium-complexity problems solved using both **PySpark** and **SQL**.

---

## 🧠 Problems Covered

| No. | Problem Title                                      | Description |
|-----|----------------------------------------------------|-------------|
| 1️⃣  | Monthly Top Spenders                               | Identify top monthly spender per customer |
| 2️⃣  | Order-to-Delivery Duration                          | Calculate duration between order and delivery |
| 3️⃣  | Join Orders with First Product Purchased           | For each order, tag the first product purchased by that customer |
| 4️⃣  | Category-wise Revenue % Contribution               | Calculate product's contribution to its category’s total revenue |
| 5️⃣  | Repeat Buyers Within a Week                        | Detect customers who made 2 purchases within 7 days |
| 6️⃣  | Sessionize Web Events                              | Assign session IDs based on 30-minute activity gaps |
| 7️⃣  | Product Category Popularity Trends                 | Track monthly sales trends (UP/DOWN/SAME) per category |

---

## 📁 Folder Structure

```
Day6/
├── data/
│   ├── category_monthly_sales.csv
│   ├── category_sales.csv
│   ├── consecutive_purchases.csv
│   ├── monthly_transactions.csv
│   ├── orders_delivery.csv
│   ├── orders_with_products.csv
│   ├── repeat_buyers.csv
│   └── web_events.csv
├── pyspark/
│   ├── category_popularity_trend.py
│   ├── category_revenue_contribution.py
│   ├── consecutive_purchase_streaks.py
│   ├── monthly_top_spenders.py
│   ├── order_delivery_duration.py
│   ├── repeat_buyers_within_week.py
│   └── sessionize_web_events.py
└── sql/
    ├── category_popularity_trend.sql
    ├── category_revenue_contribution.sql
    ├── consecutive_purchase_streaks.sql
    ├── monthly_top_spenders.sql
    ├── order_delivery_duration.sql
    ├── repeat_buyers_within_week.sql
    └── sessionize_web_events.sql
```

---

## ✅ Goal

Practice key transformations and logic patterns used in real-world data engineering interviews — using both PySpark (for scalable distributed processing) and SQL (for analytics and warehousing logic).

---

## 🔗 Connect

Follow my journey or connect on [LinkedIn](https://www.linkedin.com/in/anand-kumar-singh-830839ab)

#DataEngineering #PySpark #SQL #InterviewPreparation
