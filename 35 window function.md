好的，我帮你把这些 **银行交易特征**整理成一个 **PySpark / Databricks 可直接用的窗口特征清单**，方便 MLE 或 Data Engineer 构建特征。这里我会用 **window、agg、lag、条件计算** 来说明。

---

## 1️⃣ 准备工作

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, count, avg, max, min, when

# 1. 定义基本窗口
w_account_date = Window.partitionBy("account_id").orderBy(col("txn_ts").cast("long"))

# 用于近 N 笔交易的 rows window
w_last30 = w_account_date.rowsBetween(-29, 0)

# 用于近 7 天 / 30 天的 range window
w_7d = w_account_date.rangeBetween(-7*86400, 0)       # 最近 7 天
w_30d = w_account_date.rangeBetween(-30*86400, 0)     # 最近 30 天
```

---

## 2️⃣ 特征列表及 PySpark 实现

| 特征             | 计算逻辑                      | PySpark 写法示例                                                                                                                   |
| -------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **近 7 天交易次数**  | 最近 7 天内交易数量               | `count("*").over(w_7d).alias("txn_count_7d")`                                                                                  |
| **30 天交易总额**   | 最近 30 天内交易金额总和            | `sum("amount").over(w_30d).alias("txn_sum_30d")`                                                                               |
| **近 30 笔平均金额** | 最近 30 笔交易的平均金额            | `avg("amount").over(w_last30).alias("txn_avg_last30")`                                                                         |
| **夜间交易次数**     | 交易时间在 00:00~06:00 的交易数量   | `sum(when((hour(col("txn_ts"))>=0)&(hour(col("txn_ts"))<6),1).otherwise(0)).over(w_account_date).alias("night_txn_count")`     |
| **交易失败比例**     | 最近 30 天失败交易占比             | `sum(when(col("status")=="FAILED",1).otherwise(0)).over(w_30d)/count("*").over(w_30d).alias("fail_ratio_30d")`                 |
| **短时间跨国交易**    | 同一账户连续交易跨不同国家，时间间隔 < 1 小时 | 可以用 `lag`：`lag_country = lag("country",1).over(w_account_date)` 然后判断 `(country != lag_country) & (txn_ts - lag_txn_ts < 3600)` |
| **30 天最大单笔交易** | 最近 30 天最大交易金额             | `max("amount").over(w_30d).alias("max_txn_30d")`                                                                               |
| **30 天最小单笔交易** | 最近 30 天最小交易金额             | `min("amount").over(w_30d).alias("min_txn_30d")`                                                                               |

---

## 3️⃣ PySpark 总示例

```python
from pyspark.sql.functions import hour, lag

df_features = df.withColumn("txn_count_7d", count("*").over(w_7d)) \
    .withColumn("txn_sum_30d", sum("amount").over(w_30d)) \
    .withColumn("txn_avg_last30", avg("amount").over(w_last30)) \
    .withColumn("night_txn_count", 
                sum(when((hour(col("txn_ts"))>=0)&(hour(col("txn_ts"))<6),1).otherwise(0))
                .over(w_account_date)) \
    .withColumn("fail_ratio_30d",
                sum(when(col("status")=="FAILED",1).otherwise(0)).over(w_30d)/
                count("*").over(w_30d)) \
    .withColumn("lag_country", lag("country",1).over(w_account_date)) \
    .withColumn("lag_ts", lag("txn_ts",1).over(w_account_date)) \
    .withColumn("short_time_cross_country", 
                when((col("country") != col("lag_country")) & ((col("txn_ts") - col("lag_ts"))<3600),1).otherwise(0)) \
    .withColumn("max_txn_30d", max("amount").over(w_30d)) \
    .withColumn("min_txn_30d", min("amount").over(w_30d))
```

---

## 4️⃣ 小结

* **window 设计**：

  * `rowsBetween` → 最近 N 笔交易
  * `rangeBetween` → 最近 N 天
* **排名 / 偏移函数**：

  * `lag` → 用于跨行对比（短时间跨国交易）
* **条件计算**：

  * `when` → 夜间交易、失败交易比例
* **聚合函数**：

  * `sum`, `count`, `avg`, `max`, `min`

---

✅ 这套特征几乎覆盖银行 **风控 / MLE 特征工程**的常见需求。

---

如果你愿意，我可以帮你画一个 **时间轴 + 滑动窗口示意图**，让这几个特征的计算逻辑一目了然。

你想让我画吗？
