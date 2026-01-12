# GML Processing Pipeline

A modular, scalable Python pipeline for processing, validating, and transforming GML datasets.  
Designed for geospatial ETL workflows, Spark-based processing, and Kadaster/PDOK-style data ingestion.

This project follows a clean `src/` package layout and includes tools for:
- Parsing and flattening GML files
- Inspecting and validating geospatial attributes
- Running Spark-based transformations
- Experimenting with new GML processing techniques
- Documenting and testing the full pipeline

---

## 📁 Project Structure

```
python_workspace/
│
├── src/
│   └── gml_processing/
│       ├── __init__.py
│       ├── core/              # Core GML processing logic
│       ├── inspection/        # Validation, inspection, and debugging tools
│       └── experiments/       # Experimental scripts and prototypes
│
├── notebooks/                 # Jupyter notebooks for exploration
├── docs/                      # Documentation, diagrams, presentations
├── scripts/                   # Utility scripts and automation
├── config/                    # Configuration files
├── spatial/                   # Spatial reference data
├── tests/                     # Unit tests
├── requirements.txt           # Runtime dependencies
└── requirements_dev.txt       # Development dependencies
```

---

## 🚀 Getting Started

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Use the `src/` layout

Run Python from the project root:

```
cd python_workspace
python -m gml_processing.core.<your_module>
```

Or import modules in Python:

```python
from gml_processing.core import inlezen_gml_spark
from gml_processing.inspection import inspecteren_rowtags
```

---

## 🧱 Architecture Overview

The pipeline is built around three main components:

### **Core**
Implements the main GML ETL logic:
- Reading GML files
- Flattening nested structures
- Spark transformations
- Output generation

### **Inspection**
Tools for:
- Debugging GML attributes
- Validating row tags
- Checking schema consistency

### **Experiments**
A sandbox for:
- Prototyping new transformations
- Testing alternative parsing strategies
- Benchmarking approaches

---

## 🧪 Running Tests

```
pytest tests/
```

---

## 📄 Documentation

See the `docs/` folder for:
- Architecture diagrams  
- Presentations  
- Design notes  
- Data flow explanations  

---

## 🤝 Contributing

Pull requests are welcome.  
Please ensure:
- Code is modular and documented  
- Tests are added for new features  
- Folder structure remains clean  

---

## 📜 License

MIT License (or your preferred license)
