明白，你想做 **Target Encoding（目标编码）**，把 `merchant` 类别特征用 `is_fraud` 标签做编码。我帮你用 **银行交易 / MLE 场景**写一个 **Spark / Databricks 可运行示例**，并解释意义。

---

## 一、什么是 Target Encoding

**定义**：

* 将类别特征映射成与目标变量（label）相关的数值。
* 典型做法：计算类别在训练集上的 **目标均值**，然后替换类别值。

示例：`merchant` → `is_fraud`

| merchant    | is_fraud=1 次数 | 总交易数 | Target Encoding |
| ----------- | ------------- | ---- | --------------- |
| AMAZON      | 10            | 100  | 0.10            |
| WALMART     | 2             | 50   | 0.04            |
| LOCAL_STORE | 5             | 10   | 0.50            |

---

## 二、银行 MLE 意义

1. **抓类别对风险的影响**

   * 不同商户的欺诈率不同 → 直接用类别难建模型
2. **高维类别特征处理**

   * merchant 类别可能几百、几千 → One-Hot 太稀疏
3. **对线性模型特别有用**

   * 给模型提供连续数值，更容易学习权重
4. **需小心数据泄漏**

   * Target Encoding **必须只用训练集计算均值**，测试集不能看到 label

---

## 三、Spark 代码示例

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

spark = SparkSession.builder.getOrCreate()

# 假设 transactions 表
data = [
    ("TXN001","AMAZON",1),
    ("TXN002","AMAZON",0),
    ("TXN003","WALMART",0),
    ("TXN004","AMAZON",0),
    ("TXN005","LOCAL_STORE",1),
    ("TXN006","LOCAL_STORE",0)
]

df = spark.createDataFrame(data, ["transaction_id","merchant","is_fraud"])
df.show()
```

输出：

```
+-------------+------------+--------+
|transaction_id|merchant    |is_fraud|
+-------------+------------+--------+
|TXN001       |AMAZON      |1       |
|TXN002       |AMAZON      |0       |
|TXN003       |WALMART     |0       |
|TXN004       |AMAZON      |0       |
|TXN005       |LOCAL_STORE |1       |
|TXN006       |LOCAL_STORE |0       |
+-------------+------------+--------+
```

---

### 1️⃣ 计算 Target Encoding

```python
# merchant 对 is_fraud 求均值
merchant_te = df.groupBy("merchant").agg(mean("is_fraud").alias("merchant_te"))
merchant_te.show()
```

输出：

```
+------------+-----------+
|merchant    |merchant_te|
+------------+-----------+
|AMAZON      |0.333333   |
|WALMART     |0.0        |
|LOCAL_STORE |0.5        |
+------------+-----------+
```

---

### 2️⃣ 合并回原表

```python
df_te = df.join(merchant_te, on="merchant", how="left")
df_te.show()
```

输出：

```
+------------+-------------+--------+-----------+
|merchant    |transaction_id|is_fraud|merchant_te|
+------------+-------------+--------+-----------+
|AMAZON      |TXN001       |1       |0.333333   |
|AMAZON      |TXN002       |0       |0.333333   |
|AMAZON      |TXN004       |0       |0.333333   |
|WALMART     |TXN003       |0       |0.0        |
|LOCAL_STORE |TXN005       |1       |0.5        |
|LOCAL_STORE |TXN006       |0       |0.5        |
+------------+-------------+--------+-----------+
```

---

## 四、注意事项（银行生产）

1. **防止数据泄漏**

   * TE **只用训练集计算均值**
   * 测试集 / 线上用训练集均值
2. **稀疏类别处理**

   * 低频类别可用整体均值平滑
3. **适合高维类别特征**

   * merchant, channel, product_type 等
4. **对模型意义**

   * 给模型提供 **连续风险特征**
   * 能提升 **欺诈检测 / 风险评分** 精度

---

## 五、可选优化：平滑 Target Encoding

```python
# 平滑公式: TE_smooth = (sum(y) + k*global_mean) / (count + k)
global_mean = df.agg(mean("is_fraud")).first()[0]
k = 5  # 平滑系数
from pyspark.sql.functions import lit

df_smooth = df.groupBy("merchant").agg(
    ((mean("is_fraud") * col("count")).alias("sum_y") + k * global_mean) / (col("count")+k)
)
```

> 可以防止低频类别 TE 值极端（比如只有 1 笔交易就 TE=1 或 0）

---

如果你愿意，我可以帮你写一个 **完整银行特征 Pipeline**：

* txn_type → OHE / index
* merchant → Target Encoding（含平滑）
* 近30天滚动交易特征
* 输出直接可训练模型

你希望我写吗？
