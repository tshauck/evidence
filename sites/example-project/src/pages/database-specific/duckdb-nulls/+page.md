---
title: DuckDB Nulls
---

```sql lag
select 
    order_datetime,
    sales,
    lag(sales,6) over (order by order_datetime) as lag_sales
from orders
where order_datetime >= '2019-01-01'
limit 10
```