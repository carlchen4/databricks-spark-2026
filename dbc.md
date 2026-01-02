在 **Databricks** 语境里，**`dbc`** 通常指的是：

## **DBC = Databricks Archive**

---

## 1️⃣ 什么是 `.dbc`

* **`.dbc` 是 Databricks 的导出文件格式**
* 本质：**一个打包的 Notebook / Folder 的压缩档**
* 用来：

  * 备份 Notebook
  * 在不同 workspace 之间迁移代码
  * 交付 / 分享 Databricks 项目

> 可以把 `.dbc` 理解成
> **“Databricks 专用的 Notebook 压缩包”**

---

## 2️⃣ `.dbc` 里面有什么

一个 `.dbc` 文件通常包含：

* 一个或多个 **Notebooks**
* Notebook 的 **目录结构**
* Notebook 的 **语言信息**（Python / SQL / Scala / R）

⚠️ 不包含：

* Cluster 配置
* Secrets
* Data（Delta / 表）

---

## 3️⃣ 常见使用场景（银行 / 企业）

### ✅ 场景 1：代码交付

* MLE / Data Engineer 把：

  * 特征工程
  * 模型训练
  * 推理 Notebook
    打包成 `.dbc` 给另一个团队

### ✅ 场景 2：环境迁移

* Dev → UAT → Prod
* 不同 Databricks workspace 之间迁移 Notebook

### ✅ 场景 3：备份

* 重要 Notebook 的离线备份（合规要求）

---

## 4️⃣ 如何导入 / 导出 `.dbc`

### 导出

* Workspace → 选中 Folder / Notebook
* **Export → DBC Archive**

### 导入

* Workspace → Import
* 选择 `.dbc` 文件

---

## 5️⃣ `.dbc` vs 其他格式（面试可能问）

| 格式       | 用途                  |
| -------- | ------------------- |
| `.dbc`   | Databricks 原生打包（推荐） |
| `.py`    | 单个 Python 脚本        |
| `.ipynb` | Jupyter Notebook    |
| Git Repo | 代码版本管理（生产首选）        |

👉 **生产环境更推荐 Git 集成**
`.dbc` 更多用于 **快速迁移 / 分享**

---

## 6️⃣ 面试一句话总结

> “`.dbc` 是 Databricks 的 Notebook 打包格式，用于在不同 workspace 之间迁移或备份 Databricks Notebooks，本身不包含数据或集群配置，主要用于代码层面的交付。”

---

如果你愿意，我可以帮你：

1️⃣ 对比 **DBC vs Git vs Repo** 的最佳实践
2️⃣ 给你一个 **银行项目标准 Databricks 代码交付流程**
3️⃣ 解释 `.dbc` 在 **CI/CD** 里一般怎么用（或为什么不用）

你选一个就行 👍
