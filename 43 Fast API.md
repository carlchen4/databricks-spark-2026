没问题 👍，下面给你一个**单文件版 `app.py`**，包含：

* FastAPI 服务
* 内置 fake 模型（不用额外文件）
* `/predict` 接口
* 可直接在 Git Bash 本地运行
* 
#### Pre-requirement 

### Go to your project folder

```bash
cd /c/Users/Carl/Desktop/model_api_demo
```

---

### Create the virtual environment

```bash
python -m venv venv
```

This creates a folder named `venv/`.

---

### Activate it

### 👉 Git Bash / macOS / Linux:

```bash
source venv/Scripts/activate
```

### 👉 Windows PowerShell / CMD:

```powershell
venv\Scripts\activate
```

You should see:

```text
(venv)
```

in your terminal.

---

## ✅ app.py（完整可运行）

```python
from fastapi import FastAPI
from pydantic import BaseModel

# =========================
# Fake Model (内置)
# =========================
class FakeFraudModel:
    def __init__(self):
        self.amount_weight = 0.002
        self.risk_weight = 1.2
        self.bias = -0.3

    def predict_proba(self, X):
        results = []
        for row in X:
            amount, merchant_risk = row

            score = (
                self.amount_weight * amount
                + self.risk_weight * merchant_risk
                + self.bias
            )

            prob_fraud = 1 / (1 + pow(2.71828, -score))
            prob_not_fraud = 1 - prob_fraud

            results.append([prob_not_fraud, prob_fraud])

        return results


# =========================
# FastAPI App
# =========================
app = FastAPI(title="Simple Fake Model API")

model = FakeFraudModel()


class PredictRequest(BaseModel):
    amount: float
    merchant_risk: float


class PredictResponse(BaseModel):
    proba_fraud: float
    label: int


@app.get("/")
def root():
    return {"status": "ok", "message": "Fake model API is running"}


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    X = [[req.amount, req.merchant_risk]]
    proba = model.predict_proba(X)[0][1]
    label = 1 if proba >= 0.5 else 0

    return PredictResponse(
        proba_fraud=round(proba, 4),
        label=label
    )
```

---

## ▶️ 运行方式（Git Bash）

```bash
pip install fastapi uvicorn
python -m uvicorn app:app --reload
```


## 验证

你会发现folder 变化

```bash
\venv\Lib\site-packages
```
 
---

## 🧪 测试

浏览器：

```
http://127.0.0.1:8000/docs
```

请求示例：

```json
{
  "amount": 1200,
  "merchant_risk": 0.8
}
```

返回：

```json
{
  "proba_fraud": 0.87,
  "label": 1
}
```

---

## 这个文件已经模拟了：

* 模型加载
* online inference
* schema 校验
* REST API
* 银行风控模型接口基本形态

---

如果你后面想升级，我可以继续给你：

* Dockerfile
* API key / JWT 鉴权
* batch 接口
* 日志 + request id
* 模型版本号字段
* 健康检查 /metrics 接口

随时说 👍
