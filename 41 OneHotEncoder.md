## 一、Pipeline 是什么？

在 Spark ML 中，**Pipeline 就像流水线**，把多个 **数据处理步骤 + 特征工程 + 模型** 串起来，按顺序执行。

https://www.youtube.com/watch?v=rsyrZnZ8J2o

https://www.youtube.com/watch?v=G2iVj7WKDFk

### 作用：

1. **把整个流程自动化**

   * 不用手动一个个 `transform()`
   * 训练 / 测试 / 生产用同一个流程
2. **防止数据泄漏**

   * 比如 StringIndexer 只用训练集 fit，不会看到测试集新类别
3. **易维护、易复用**

   * 银行业务复杂，Pipeline 可加多个步骤：编码、填充缺失值、归一化、滚动特征

---

## 二、StringIndexer 是什么？为什么要用？

### 意思：

把 **字符串类别**（比如 txn_type） → **数字索引**（0,1,2…）

* DEBIT → 0
* CREDIT → 1
* TRANSFER → 2

### 银行作用：

1. **模型不能直接理解文字**

   * Logistic / Linear / Tree 都只能用数字
2. **可做 One-Hot / 数值特征**

   * 紧接着 OneHotEncoder
3. **handleInvalid="keep"**

   * 防生产数据中出现新的 txn_type crash
   * 会给未知类别一个 index，让模型也能学“未知类别”信号（有时很强，尤其是欺诈检测）

---

## 三、OneHotEncoder 是什么？为什么要用？

### 意思：

把数字索引 → **稀疏向量**（0/1），每个类别一列

示例：

```
DEBIT    → [1,0,0]
CREDIT   → [0,1,0]
TRANSFER → [0,0,1]
```

### 银行作用：

1. **消除顺序误导**

   * StringIndexer 编号只是顺序，不能当大小用
   * OHE 把每个类别独立出来，模型不会误解
2. **适合线性模型**

   * Logistic / Linear 回归要求特征独立
3. **稀疏存储**

   * 高维类别不会占用太多内存

⚠️ 注意：

* 树模型不需要 OHE，直接用索引就可以
* dropLast=True 避免多列完全共线，方便线性模型

---

## 四、Pipeline + StringIndexer + OneHotEncoder 的具体作用

结合起来看 **完整流程**：

1. **Pipeline**：把步骤串起来，保证训练 / 测试 / 生产一致
2. **StringIndexer**：把类别转数字，数字是索引，不是数值大小
3. **OneHotEncoder**：把索引转向量，每个类别独立列，消除顺序误导
4. **结果**：

   * 训练集、测试集、线上数据统一处理
   * 模型可以直接使用
   * 可以安全应对新类别，不挂掉

---

## 五、举个银行场景例子（MLE 实战）

| transaction_id | txn_type | txn_type_idx | txn_type_ohe |
| -------------- | -------- | ------------ | ------------ |
| TXN001         | DEBIT    | 0            | [1,0,0]      |
| TXN002         | CREDIT   | 1            | [0,1,0]      |
| TXN003         | REVERSAL | 2            | [0,0,1]      |

* **模型用 txn_type_ohe** 来判断不同交易类型对 **欺诈概率 / 违约概率** 的影响
* **如果线上出现新 txn_type（FEE）**

  * StringIndexer 会分配新的 index
  * OneHotEncoder 会生成“未知类别”向量
  * 模型能学到“未知交易类型也可能有风险”

---

✅ 总结一句话：

> Pipeline + StringIndexer + OneHotEncoder 是 **银行机器学习特征工程的标准做法**，可以把类别特征安全、高效、自动化地转成模型可用的数值特征，同时防止生产数据异常或新类别导致报错。

---
明白，我帮你把前面解释的 **Pipeline + StringIndexer + OneHotEncoder 的原理和作用**，直接整合到 **带 Spark 代码的知识总结**，让你一边看原理一边能直接跑。

---

# 银行 MLE 特征工程：Pipeline + StringIndexer + OneHotEncoder

## 一、背景

* `txn_type` 是银行交易表中的类别特征（DEBIT / CREDIT / TRANSFER / REVERSAL…）
* **目标**：把它转换为 **模型可用特征**，适合线性或树模型
* **原则**：

  1. 防生产 crash → `handleInvalid="keep"`
  2. 线性模型 → 用 OHE 消除顺序误导
  3. 树模型 → 直接用索引也可

---

## 二、Spark 代码示例

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

# 1️⃣ 创建 Spark Session
spark = SparkSession.builder.appName("BankMLE").getOrCreate()

# 2️⃣ 假设银行 transactions 表
data = [
    ("TXN001","DEBIT"),
    ("TXN002","CREDIT"),
    ("TXN003","TRANSFER"),
    ("TXN004","DEBIT"),
    ("TXN005","REVERSAL")
]
df = spark.createDataFrame(data, ["transaction_id", "txn_type"])
df.show()
```

输出：

```
+-------------+---------+
|transaction_id|txn_type|
+-------------+---------+
|TXN001       |DEBIT    |
|TXN002       |CREDIT   |
|TXN003       |TRANSFER |
|TXN004       |DEBIT    |
|TXN005       |REVERSAL |
+-------------+---------+
```

---

### 3️⃣ 定义 Pipeline

```python
# Step 1: StringIndexer
indexer = StringIndexer(
    inputCol="txn_type",
    outputCol="txn_type_idx",
    handleInvalid="keep"   # 防生产 crash
)

# Step 2: OneHotEncoder
encoder = OneHotEncoder(
    inputCols=["txn_type_idx"],
    outputCols=["txn_type_ohe"],
    dropLast=True           # 避免 multicollinearity
)

# Step 3: Pipeline
pipeline = Pipeline(stages=[indexer, encoder])
```

---

### 4️⃣ fit & transform

```python
# 训练 pipeline (只 fit 在训练集)
pipeline_model = pipeline.fit(df)
df_transformed = pipeline_model.transform(df)

df_transformed.select("transaction_id","txn_type","txn_type_idx","txn_type_ohe").show(truncate=False)
```

输出示例：

```
+-------------+---------+------------+----------------+
|transaction_id|txn_type|txn_type_idx|txn_type_ohe    |
+-------------+---------+------------+----------------+
|TXN001       |DEBIT    |0.0         |(4,[0],[1.0])  |
|TXN002       |CREDIT   |1.0         |(4,[1],[1.0])  |
|TXN003       |TRANSFER |2.0         |(4,[2],[1.0])  |
|TXN004       |DEBIT    |0.0         |(4,[0],[1.0])  |
|TXN005       |REVERSAL |3.0         |(4,[3],[1.0])  |
+-------------+---------+------------+----------------+
```

---

### 5️⃣ 展开 Sparse Vector（可选，便于查看）

```python
# UDF: SparseVector -> Array
vector_to_array_udf = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
df_final = df_transformed.withColumn("txn_type_ohe_array", vector_to_array_udf("txn_type_ohe"))

df_final.select("transaction_id","txn_type","txn_type_ohe_array").show(truncate=False)
```

输出：

```
+-------------+---------+----------------+
|transaction_id|txn_type|txn_type_ohe_array|
+-------------+---------+----------------+
|TXN001       |DEBIT    |[1.0,0.0,0.0,0.0]|
|TXN002       |CREDIT   |[0.0,1.0,0.0,0.0]|
|TXN003       |TRANSFER |[0.0,0.0,1.0,0.0]|
|TXN004       |DEBIT    |[1.0,0.0,0.0,0.0]|
|TXN005       |REVERSAL |[0.0,0.0,0.0,1.0]|
+-------------+---------+----------------+
```

✅ 可以直接喂给 Logistic Regression 或存到 Feature Store

---

## 三、知识总结（结合代码）

| 步骤            | 作用                           | Spark 代码                                                                                |
| ------------- | ---------------------------- | --------------------------------------------------------------------------------------- |
| StringIndexer | 字符串类别 → 数字索引（0,1,2…）         | `StringIndexer(inputCol="txn_type", outputCol="txn_type_idx", handleInvalid="keep")`    |
| OneHotEncoder | 数字索引 → Sparse Vector（每类独立一列） | `OneHotEncoder(inputCols=["txn_type_idx"], outputCols=["txn_type_ohe"], dropLast=True)` |
| Pipeline      | 串联步骤，训练 / 测试 / 线上一致          | `Pipeline(stages=[indexer, encoder])`                                                   |
| 展示 / debug    | 查看 OHE 数值                    | `udf(lambda v: v.toArray().tolist())`                                                   |

---

### 四、银行实战要点

1. **handleInvalid="keep"** → 防止生产 crash
2. **dropLast=True** → 避免线性模型多重共线
3. **训练 Pipeline 只 fit 在训练集** → 防止数据泄漏
4. **树模型可只用 StringIndexer** → 避免维度爆炸
5. **Feature Store** → 可以存 OHE 列直接复用


好，这个是 **Spark / 银行 MLE 面试必问题**。
我直接给你 **结论 → 原理 → 银行场景 → 面试答案**，一遍就懂。

---

## 一句话结论（先背）

> **StringIndexer = 编码（label → number）**
> **OneHotEncoder = 展开（number → vector）**
> 👉 **不是二选一，是前后两步**

---

## 一、它们各自是干嘛的？

### 1️⃣ StringIndexer（必须先用）

**作用：**

> 把字符串类别 → 数字 ID

```text
DEBIT    → 0
CREDIT   → 1
TRANSFER → 2
```

```python
StringIndexer(
  inputCol="txn_type",
  outputCol="txn_type_idx"
)
```

📌 特点：

* 不是 OHE
* 数字 **没有大小意义**
* 按 **频率排序**（默认）

---

### 2️⃣ OneHotEncoder

**作用：**

> 把数字 ID → One-Hot 向量

```text
0 → [1,0,0]
1 → [0,1,0]
2 → [0,0,1]
```

```python
OneHotEncoder(
  inputCols=["txn_type_idx"],
  outputCols=["txn_type_ohe"]
)
```

📌 特点：

* 输出是 **Sparse Vector**
* 适合线性模型
* 防止“虚假顺序关系”

---

## 二、为什么不能只用 StringIndexer？（很关键）

### ❌ 错误理解

```text
CREDIT (2) > DEBIT (0)
```

模型会以为：

> CREDIT 比 DEBIT “更大”

👉 对 **Logistic / Linear Model** 是灾难

---

## 三、银行里什么时候用哪个？

### 场景 1：Logistic Regression / Linear Model

✅ **必须：StringIndexer + OneHotEncoder**

```text
txn_type_ohe = [0,1,0]
```

原因：

* 避免顺序误导
* 权重可解释

---

### 场景 2：Tree / XGBoost / LightGBM

✅ **只用 StringIndexer**

```text
txn_type_idx = 2
```

原因：

* 树不关心大小，只关心 split
* OHE 会 **维度爆炸**
* 训练更慢

---

### 场景 3：Rule / SQL / Feature Engineering

❌ 都不用
直接：

```sql
count(case when txn_type='DEBIT' then 1 end)
```

---

## 四、对比表（面试最爱）

| 对比项     | StringIndexer | OneHotEncoder |
| ------- | ------------- | ------------- |
| 输入      | String        | Index         |
| 输出      | 数字            | 向量            |
| 是否单独用   | ✅（树模型）        | ❌             |
| 是否有顺序误导 | ⚠️ 有          | ❌             |
| 维度      | 1             | N             |
| 银行使用频率  | ⭐️⭐️⭐️        | ⭐️⭐️          |

---

## 五、真实银行 Pipeline（标准写法）

```python
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[
    StringIndexer(
        inputCol="txn_type",
        outputCol="txn_type_idx",
        handleInvalid="keep"
    ),
    OneHotEncoder(
        inputCols=["txn_type_idx"],
        outputCols=["txn_type_ohe"],
        dropLast=True
    )
])
```

---

## 六、面试级总结回答（你可以直接背）

> StringIndexer converts categorical strings into numerical indices, which is required before modeling.
> OneHotEncoder then converts those indices into sparse vectors to remove any artificial ordinal relationship.
> In banking, we typically use both for linear models, but only StringIndexer for tree-based models to avoid dimensional explosion.

---

## 七、加分点（你像 CIBC / HSBC MLE）

* `handleInvalid="keep"` 防 production crash
* `dropLast=True` 防 multicollinearity
* Indexer **只 fit 在 training data**（防 leakage）
* Feature Store 里 **存 OHE 结果，不存 raw**


