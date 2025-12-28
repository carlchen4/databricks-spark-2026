这行代码 **不对**，而且这是一个**非常常见的新手坑** 👍
我直接给你**结论 → 原因 → 正确用法**。

---

## ❌ 这行为什么不行

```python
df.partionBy("account_id").display()
```

### 原因 1：拼写错误

```text
partionBy ❌
partitionBy ✅
```

但就算你拼对了，**还是不行**👇

---

## 原因 2（关键）：`partitionBy` 不是 DataFrame 的方法

👉 **`partitionBy` 只属于 `Window` 或 `write`，不属于 `DataFrame`**

---

## Spark 里有 3 个“partition”，但意思完全不同

### ① Window 的 partition（你前面学的这个）

```python
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_ts")
```

👉 **这是逻辑分组，用来算特征**

* running sum
* rolling avg
* 近 7 天 / 30 天

❌ 不能 `display()`

---

### ② DataFrame 的物理分区（repartition / coalesce）

```python
df.repartition("account_id").display()
```

👉 **这是 Spark 执行层的 partition**

* 决定 shuffle
* 决定并行度
* 和 window 特征**不是一回事**

✅ 可以 `display()`

---

### ③ 写数据时的 partition（Hive / Delta）

```python
df.write.partitionBy("account_id").save(...)
```

👉 **这是存储分区**

* 文件按 account_id 分目录
* 用于查询加速

---

## 所以你真正想干的是什么？我帮你对号入座

### 如果你是想「看 account_id 分组后的结果」

```python
df.orderBy("account_id").display()
```

或

```python
df.groupBy("account_id").count().display()
```

---

### 如果你是想「按 account 算 window 特征」

```python
from pyspark.sql.functions import sum
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_ts")

df.withColumn("running_sum", sum("amount").over(w)).display()
```

---

### 如果你是想「让 Spark 并行更合理」

```python
df = df.repartition("account_id")
df.display()
```

---

## 一张速记表（银行 MLE 必背）

| 你想干什么  | 用哪个                   |
| ------ | --------------------- |
| 算滚动特征  | `Window.partitionBy`  |
| 改执行并行度 | `df.repartition()`    |
| 存表分区   | `write.partitionBy()` |
| 看数据    | `display()`           |

---

## 一句总结

> **`partitionBy` 从来不是用来 display 的，
> 它要么定义 window 计算边界，要么定义数据怎么被切分存储。**

> **A 的最后一笔交易，为什么会包含 B 的 1000？**

---

## 核心原因（一句话）

👉 **因为你没有 `partitionBy`，Spark 把 A 和 B 当成“同一个人”的连续交易来算**

---

## Spark 在“不 partition”时真实做的事

当你写：

```python
w = Window.orderBy(col("txn_ts").cast("long"))
```

Spark 的理解是：

> “我只关心时间顺序，
> 不关心这笔交易属于谁。”

于是它会：

1. **把所有账户的数据放进同一个 window**
2. 按 `txn_ts` 排序
3. 从第一行开始 **一行一行往下累计**

---

## 用你那句例子一步一步走

原始数据（时间顺序）：

| 行号 | account_id | txn_ts | amount |
| -- | ---------- | ------ | ------ |
| 1  | A          | t1     | 100    |
| 2  | A          | t2     | 50     |
| 3  | B          | t3     | 1000   |
| 4  | A          | t4     | 30     |

### 计算 `running_sum = sum(amount).over(w)`

Spark 的计算逻辑是：

* 第 1 行：100
* 第 2 行：100 + 50 = 150
* 第 3 行：150 + 1000 = 1150
* **第 4 行：1150 + 30 = 1180**

👉 **Spark 不会“重置”窗口**，因为你没告诉它在哪里重置。

---

## 所以为什么 A 的最后一笔包含了 B 的 1000？

因为：

* 对 Spark 来说：

  * **第 4 行只是“下一行”**
  * 不是“A 的新一轮计算”
* 它不知道 **account_id 是边界**

---

## `partitionBy` 到底干了什么？

```python
Window.partitionBy("account_id").orderBy(...)
```

你是在明确告诉 Spark：

> “当 account_id 变了，
> 之前的累积结果 **作废，重新算**。”

于是：

* A 的 window：只看 A
* B 的 window：只看 B

---

## 银行 / 风控一句话版

> 不 partition，相当于把全银行当成一个超级账户；
> partitionBy(account_id) 才是在按账户算风险。

---

## 直觉类比（很重要）

### 不 partition

📒 一本账本，**所有人的账都写在一起**

### 有 partition

📒📒📒 每个人一本账本

---

## 为什么这是 Spark 的“默认行为”？

因为 Spark 是：

* 通用分布式计算引擎
* 不懂“账户”“客户”“风险主体”

👉 **边界必须你来定义**

---

### 最后一行总结（你可以直接背）

> 因为没有 partitionBy，Spark 的 window 不会在 account_id 变化时重置，
> 所以 A 后面的计算自然会包含 B 之前的交易金额。

如果你愿意，下一步我可以从 **执行计划（explain）** 层面再给你拆一次。
