from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession


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

pipeline_model = pipeline.fit(txn)  
df_transformed = pipeline_model.transform(txn)




## 一、Pipeline 是什么？

在 Spark ML 中，**Pipeline 就像流水线**，把多个 **数据处理步骤 + 特征工程 + 模型** 串起来，按顺序执行。

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

如果你想，我可以帮你画一个 **银行 transactions → Pipeline → 模型训练的完整流程图**，让你一眼看懂每一步的作用。

你希望我画吗？
