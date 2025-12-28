在 **Databricks notebook** 里：

```python
display(_)
```

### `_` 是什么意思？

`_` 代表 **上一个 cell / 上一条表达式的返回结果**。

也就是说：

```python
df2 = df.select(col("amount") * cur)
df2
display(_)
```

等价于：

```python
display(df2)
```

---

### 为什么在 Databricks 里常用？

* Databricks notebook 会把 **最后一个表达式的结果** 自动存到 `_`
* 对 DataFrame 来说，`display(_)` 很方便，不用再起变量名

---

### 对比几个例子

#### ✅ 常见用法

```python
df.select(col("amount") * cur)
display(_)
```

#### ❌ 在普通 PySpark / 本地 Python 里

```python
display(_)
```

* **不一定可用**
* `_` 只是 Python REPL 的约定，不是 Spark 特性

---

### 小总结（一句话）

> **`display(_)` = 显示“刚刚算出来的那个 DataFrame”**

如果你愿意，我可以顺便讲下：

* `display()` vs `.show()`
* `_` 在 Python / Databricks / SQL cell 里的区别
  这几个在面试里也经常被问。
