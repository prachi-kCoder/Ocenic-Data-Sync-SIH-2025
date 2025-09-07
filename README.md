# 🌊 SIH-2025 Oceno Data Pipelines

This project regulates siloed oceanic data—weather, microorganisms, fisheries—and automates the entire data workflow using FastAPI.

## 🚀 FastAPI Server Setup

### 1. Clone the Repository
```bash
git clone https://github.com/prachi-kCoder/SIH-2025-Oceno-Data-Pipelines.git
cd SIH-2025-Oceno-Data-Pipelines/server
```

### 2. Create Virtual Environment
```bash
python -m venv venv
On Windows: venv\Scripts\activate

```

3.  Install Dependencies
```bash
pip install -r requirements.txt
```

4. Run fastapi server
```bash
uvicorn main:app --reload
```


5. Access the API :
```bash
Swagger UI: http://localhost:8000/docs
ReDoc: http://localhost:8000/redoc
```

