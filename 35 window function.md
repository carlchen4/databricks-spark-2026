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


这个问题问得**非常关键**，你已经踩到 **Spark Window 的核心规则**了 👍
我用 **“不 cast 会怎样 → cast 在解决什么 → 什么时候必须 cast”** 来解释。

---

## 一句话先给结论

```python
Window.partitionBy("account_id") \
      .orderBy(col("txn_ts").cast("long"))
```

> **cast 的目的：把时间戳变成“可做 rangeBetween 的数值轴”**

---

## 1️⃣ 不 cast 会发生什么？

### 情况 A：只用 `rowsBetween`

```python
.orderBy("txn_ts")
.rowsBetween(-2, 0)
```

✅ **完全没问题**

原因：

* `rowsBetween` 只关心 **行的顺序**
* 不关心时间差是多少

---

### 情况 B：用 `rangeBetween`（你正在用的）

```python
.orderBy("txn_ts")
.rangeBetween(-7*86400, 0)
```

❌ **会报错**，常见报错类似：

> `rangeBetween requires numeric order by expression`

---

## 2️⃣ 为什么 `rangeBetween` 必须是数值？

### `rangeBetween` 的本质是：

> **当前行的 orderBy 值 ± 一个“数值范围”**

```text
当前 order value = X
窗口范围 = [X - 7*86400 , X]
```

所以：

* Spark 必须能算：`X - 604800`
* `timestamp - int` ❌
* `long - long` ✅

---

## 3️⃣ `cast("long")` 在做什么？

```python
col("txn_ts").cast("long")
```

👉 把时间戳变成 **Unix 时间（秒）**

例子：

```text
2025-01-01 12:00:00 → 1735732800
```

现在 Spark 可以：

```text
1735732800 - 604800  ✔
```

---

## 4️⃣ 为什么是 long，不是 date？

| 类型        | 能否 rangeBetween | 原因                          |
| --------- | --------------- | --------------------------- |
| string    | ❌               | 无法做数值范围                     |
| date      | ❌               | Spark 不支持 date rangeBetween |
| timestamp | ❌               | 不能直接减 int                   |
| long      | ✅               | 标准数值轴                       |

---

## 5️⃣ 秒 vs 毫秒（常见坑）

### 秒级时间戳（Spark cast 默认）

```python
-7 * 86400
```

### 如果你的时间是毫秒：

```python
-7 * 86400 * 1000
```

否则窗口会错 ❌

---

## 6️⃣ 什么时候“必须 cast”？

| 场景            | 是否需要 cast |
| ------------- | --------- |
| rowsBetween   | ❌ 不需要     |
| lag / lead    | ❌ 不需要     |
| rangeBetween  | ✅ 必须      |
| 排名 row_number | ❌         |

---

## 7️⃣ 面试一句话（直接背）

> **`rangeBetween` 是数值驱动窗口，timestamp 必须 cast 成 long 才能做时间范围计算**

---

## 8️⃣ 直觉理解（最重要）

> `rowsBetween`：
> 👉 “我往前看 2 行”

> `rangeBetween`：
> 👉 “我往前看 7 天”

而 **“7 天”必须落在一条数值时间轴上**，这就是 `cast("long")` 的原因。


明白，你想在之前整理的 **滑动窗口特征列表**之外，了解银行常用的其他特征。针对 **风控 / 交易 MLE / 客户行为建模**，我帮你整理一份更全面的特征清单（都是 PySpark / Databricks 上可以直接实现的）。

---

## 1️⃣ 时间序列类（滑动窗口）

| 特征           | 描述   | 示例窗口                                                      |
| ------------ | ---- | --------------------------------------------------------- |
| 近7天/30天交易次数  | 活跃度  | `count("*").over(w_7d)`                                   |
| 最近N笔交易金额平均   | 行为模式 | `avg("amount").over(w_last30)`                            |
| 最近N笔交易金额标准差  | 波动性  | `stddev("amount").over(w_last30)`                         |
| 最近30天最大/最小交易 | 极值行为 | `max("amount").over(w_30d)` / `min("amount").over(w_30d)` |
| 最近N天失败交易比例   | 风控指标 | `sum(when(status=="FAILED",1).otherwise(0))/count("*")`   |

---

## 2️⃣ 偏移 / 排名类

| 特征        | 描述      | PySpark函数                          |
| --------- | ------- | ---------------------------------- |
| 上一笔交易金额   | 变化幅度    | `lag("amount",1).over(w)`          |
| 上一笔交易时间间隔 | 时间敏感行为  | `txn_ts - lag("txn_ts",1).over(w)` |
| 行内排名      | 本月/本周排名 | `row_number().over(w)`             |
| 同金额排名     | 相等值排名   | `rank()` / `dense_rank()`          |

---

## 3️⃣ 时间段类

| 特征        | 描述      |
| --------- | ------- |
| 夜间交易次数    | 0~6点    |
| 周末交易次数    | 星期六、日交易 |
| 节假日交易次数   | 法定节假日交易 |
| 日内交易频率    | 每小时交易数  |
| 近N天平均交易间隔 | 间隔波动    |

---

## 4️⃣ 金额/类别统计

| 特征            | 描述                |
| ------------- | ----------------- |
| 消费类型占比        | POS/ATM/Online占比  |
| 大额交易次数        | 超过阈值（如$1000）的交易数量 |
| 小额交易比例        | 小额/总交易比例          |
| 不同货币交易占比      | CAD/USD/其他        |
| 最近N天累计支出/收入比例 | 收入 vs 支出模式        |

---

## 5️⃣ 地理/跨境类

| 特征          | 描述                       |
| ----------- | ------------------------ |
| 短时间跨国交易     | 上一笔与本笔国家不同，时间差<1h        |
| 异地登录/交易     | IP/geolocation变动（非交易表可接） |
| 常用城市/国家交易占比 | 常规 vs 异常地区交易             |

---

## 6️⃣ 交易结果类

| 特征        | 描述        |
| --------- | --------- |
| 交易失败次数/比例 | 风控基础指标    |
| 退款/撤销次数   | 退货/撤销交易模式 |
| 被拒交易次数    | 信用卡风控指标   |

---

## 7️⃣ 高级组合特征（MLE常用）

* **金额 × 时间间隔** → 波动特征
* **交易金额 z-score** → 异常检测
* **累计消费占比变化** → 最近交易趋势
* **跨账户交易模式** → 异常转账检测

---

### 🔹 总结

> 除了你之前列的近7天/30天交易次数、失败比例、夜间交易等，银行还会用：
>
> * 时间序列特征（滑动平均/标准差/极值）
> * 排名/偏移特征（上一笔交易、间隔、排名）
> * 时间段特征（日内/周末/节假日）
> * 金额/类别统计（大额、小额、货币、收入支出比）
> * 地理/跨境特征（异地、跨国）
> * 交易结果特征（失败、退款、拒绝）
> * 高级组合特征（金额×时间间隔、z-score、趋势）


好，我们用一个 **具体银行账户交易例子** 来直观对比 **有排序 vs 无排序** 的区别，尤其是 `rowsBetween` 或 `rangeBetween` 下的行为。

---

## 1️⃣ 数据准备

假设账户 A 最近 5 笔交易：

| txn_id | txn_ts           | amount |
| ------ | ---------------- | ------ |
| T1     | 2025-01-01 10:00 | 100    |
| T2     | 2025-01-03 12:00 | 50     |
| T3     | 2025-01-02 09:00 | 200    |
| T4     | 2025-01-05 09:00 | 30     |
| T5     | 2025-01-04 11:00 | 120    |

注意 **txn_ts 是乱序的**。

---

## 2️⃣ window 不排序（只 partitionBy）

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

w = Window.partitionBy("account_id").rowsBetween(-2, 0)

df.withColumn(
    "last_3_txn_amount",
    sum(col("amount")).over(w)
).show()
```

### 计算过程

* Spark 会按 **DataFrame 原始顺序**计算：

  * 第 1 行 T1 → sum(100) = 100
  * 第 2 行 T2 → sum(100+50) = 150
  * 第 3 行 T3 → sum(100+50+200) = 350
  * 第 4 行 T4 → sum(50+200+30) = 280
  * 第 5 行 T5 → sum(200+30+120) = 350

> ❌ 明显错误，因为“最近 3 笔交易”不是按时间，而是按 DataFrame 原始顺序

---

## 3️⃣ window 排序（按 txn_ts）

```python
w = Window.partitionBy("account_id").orderBy("txn_ts").rowsBetween(-2, 0)

df.withColumn(
    "last_3_txn_amount",
    sum(col("amount")).over(w)
).show()
```

### 计算过程（按时间顺序）

* 按 txn_ts 排序：

| txn_id | txn_ts           | amount |
| ------ | ---------------- | ------ |
| T1     | 2025-01-01 10:00 | 100    |
| T3     | 2025-01-02 09:00 | 200    |
| T2     | 2025-01-03 12:00 | 50     |
| T5     | 2025-01-04 11:00 | 120    |
| T4     | 2025-01-05 09:00 | 30     |

* 第 3 行 T2 → 最近 3 笔 = 100 + 200 + 50 = 350 ✅
* 第 5 行 T4 → 最近 3 笔 = 50 + 120 + 30 = 200 ✅

> ✔️ 结果符合“最近 3 笔交易”的业务逻辑




