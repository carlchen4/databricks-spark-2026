好，这些**正是 Databricks / Spark 真·进阶必学点**。我给你一套**“MLE + 银行场景友好”**的讲解，**不啰嗦、直给、带例子、帮你建立感觉**。

---

# 一、Window：`rowsBetween` vs `rangeBetween`

👉 **做时间序列、交易滚动统计必会**

## 1️⃣ `rowsBetween`（按“行数”）

**不看值，只看前后多少行**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

w = Window.partitionBy("account_id") \
          .orderBy("transaction_date") \
          .rowsBetween(-2, 0)

df.withColumn("sum_last_3_txn", sum("amount").over(w))
```

**含义**

* 当前行 + 前 2 行
* 常用于：**最近 N 笔交易**

📌 银行例子

> 最近 **3 笔交易总金额**

---

## 2️⃣ `rangeBetween`（按“值范围”）

**看 orderBy 的值范围（通常是时间）**

```python
from pyspark.sql.functions import col

w = Window.partitionBy("account_id") \
          .orderBy(col("txn_ts").cast("long")) \
          .rangeBetween(-7*86400, 0)
```

**含义**

* 最近 **7 天内** 的交易
* 时间不连续也没问题

📌 银行例子

> 最近 **7 天消费总额（AML / 风控）**

---

## 对比总结

| 对比     | rowsBetween | rangeBetween |
| ------ | ----------- | ------------ |
| 依据     | 行数          | 值（时间 / 数值）   |
| 是否连续   | 必须          | 不要求          |
| 常用场景   | 最近 N 笔      | 最近 N 天       |
| 风控/AML | ❌           | ✅            |

---

# 二、Catalyst Optimizer 是什么

👉 **Spark SQL 的“大脑”**

**一句话**

> Catalyst 会把你写的 SQL / DataFrame **偷偷改写成更快的执行计划**

## Catalyst 会干啥？

* Predicate Pushdown
* Column Pruning
* Reorder Filter / Join
* Simplify Expression

你写：

```python
df.filter(col("amount") > 100).select("account_id", "amount")
```

Catalyst 可能变成：

```text
→ 先 filter
→ 再只读 account_id, amount 两列
```

📌 **你不用手写优化，但要“配合它”**

---

# 三、Predicate Pushdown（超级重要）

👉 **银行数据必考点**

**意思**

> 把 filter **推到数据源层（Parquet / Delta）**

```python
df.filter("txn_date >= '2024-01-01'")
```

如果：

* Parquet / Delta
* 非 UDF
* 简单比较

👉 Spark **只读满足条件的 row group**

❌ 不能下推：

```python
df.filter(my_udf(col("amount")) > 0)
```

---

## 为什么重要？

| 场景   | 后果     |
| ---- | ------ |
| 可下推  | 少 IO，快 |
| 不可下推 | 全表扫描   |

📌 银行：**10 年交易表 → 只扫 1 个月**

---

# 四、Z-Order（Databricks 特有）

👉 **Delta Lake 的物理排序**

```sql
OPTIMIZE transactions
ZORDER BY (account_id, txn_date)
```

**效果**

* 相似值物理靠近
* 减少文件扫描

📌 银行推荐 Z-Order

* `account_id`
* `customer_id`
* `txn_date`

❗ Z-Order ≠ partition
👉 **是补充关系**

---

# 五、AS OF / Time Travel

👉 **审计 & 回溯神技**

```sql
SELECT * FROM transactions VERSION AS OF 123
```

```sql
SELECT * FROM transactions TIMESTAMP AS OF '2024-10-01'
```

📌 银行用法

* 模型训练复现
* 审计 / 合规
* 回滚错误数据

---

# 六、Photon Engine（Databricks 面试常问）

👉 **C++ 向量化执行引擎**

**一句话**

> Photon = Spark SQL 的“外挂加速器”

* 用 C++ 重写执行算子
* 向量化 + SIMD
* 对 SQL / DF 自动生效

📌 加速明显场景

* Aggregation
* Join
* Window
* Scan Parquet / Delta

---

# 七、Shuffle 是什么（必会）

👉 **Spark 最贵的操作**

**定义**

> 数据从一个 executor **跨网络** 发送到另一个

发生在：

* `groupBy`
* `join`
* `orderBy`
* `repartition`

📌 特点

* 慢
* 占网络
* 占磁盘

---

# 八、`repartition` vs `coalesce`

👉 **控制 Shuffle 的核心手段**

## 1️⃣ repartition（有 Shuffle）

```python
df.repartition(200, "account_id")
```

* **打乱数据**
* 可增 / 减 partition
* 成本高

📌 用于：

* Join 前
* 数据均匀化

---

## 2️⃣ coalesce（无 Shuffle）

```python
df.coalesce(20)
```

* **只合并**
* 不重新分布
* 很快

📌 用于：

* 写数据前减少小文件

---

## 对比

|                | repartition | coalesce |
| -------------- | ----------- | -------- |
| 是否 Shuffle     | ✅           | ❌        |
| 是否均匀           | ✅           | ❌        |
| 是否可增 partition | ✅           | ❌        |
| 写数据前           | ❌           | ✅        |

---

# 九、Data Skew（银行数据常见坑）

👉 **一部分 task 特别慢**

## 典型场景

* `account_id = 000001` 有 1 亿交易
* 其他账号只有几百条

结果：

* 一个 task 跑 30 分钟
* 其他早就结束

---

## 解决方案

### 1️⃣ Salting（最经典）

```python
df.withColumn("salt", rand())
```

### 2️⃣ AQE（Databricks 推荐）

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 3️⃣ 拆大 key

* 热账户单独处理

---

# 十、给你一张 **MLE 必会清单（银行）**

| 主题                     | 是否必会 |
| ---------------------- | ---- |
| Window（rangeBetween）   | ✅    |
| Catalyst 原理            | ✅    |
| Predicate Pushdown     | ✅    |
| Z-Order                | ✅    |
| Photon                 | ✅    |
| Shuffle                | ✅    |
| repartition / coalesce | ✅    |
| Data Skew              | ✅    |

---

如果你愿意，我可以帮你下一步直接做👇

* ✅ **一套 Databricks 面试题（含陷阱）**
* ✅ **AML / 风控 Spark 实战 Notebook**
* ✅ **Spark 执行计划 explain() 拆解**

你想先哪一个？
