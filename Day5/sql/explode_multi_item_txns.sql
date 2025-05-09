-- # ✅ Problem 5: Explode Multi-Item Transactions

-- # 📄 Dataset: multi_item_txns.csv

-- # 🎯 Goal:

-- # Flatten the items array so that each item appears in its own row, while retaining the associated txn_id.

SELECT * FROM multi_item_txns;

WITH RECURSIVE split_items AS (
  SELECT
    txn_id,
    SUBSTRING_INDEX(items, ',', 1) AS item,
    SUBSTRING(items, LENGTH(SUBSTRING_INDEX(items, ',', 1)) + 2) AS remaining
  FROM multi_item_txns

  UNION ALL

  SELECT
    txn_id,
    SUBSTRING_INDEX(remaining, ',', 1),
    SUBSTRING(remaining, LENGTH(SUBSTRING_INDEX(remaining, ',', 1)) + 2)
  FROM split_items
  WHERE remaining != ''
)

