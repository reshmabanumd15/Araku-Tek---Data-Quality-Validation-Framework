Total Revenue = SUM(transactions[amount])
Orders = COUNT(transactions[order_id])
Customers = DISTINCTCOUNT(customer[customer_id])
Violation Count (from JSON) -> create a table using Power Query that flattens `reports/<date>/*.json` if needed.
