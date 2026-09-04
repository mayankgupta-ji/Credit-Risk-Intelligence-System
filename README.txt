Credit Risk Intelligence System
Big Data Driven Credit Score Classification using Hadoop Ecosystem and Python Analytics
An end-to-end Big Data and Data Science project that analyzes customer demographic, financial, and credit-behavior data to identify credit-risk patterns and classify customers into Good, Standard, and Poor credit-risk categories.
🎯 Project Objective
The primary objective of this project is to build a Credit Risk Intelligence System that combines the Hadoop ecosystem with Python-based Data Science techniques to transform raw banking data into actionable credit-risk insights.
The project aims to:
    1. Ingest and process a large-scale banking dataset using a Hadoop-based Big Data pipeline.
    2. Clean and prepare the dataset for accurate and consistent analysis.
    3. Define and calculate business-relevant Key Performance Indicators (KPIs).
    4. Perform exploratory data analysis across demographic, financial, credit, and behavioral dimensions.
    5. Visualize important patterns and relationships using statistical charts.
    6. Classify customers into Good / Standard / Poor credit-risk categories using machine-learning techniques.
    7. Translate analytical findings into actionable business recommendations for loan approval, risk management, and customer segmentation.
🔄 Project Workflow
Raw Banking Dataset
        ↓
HDFS
        ↓
Hive
        ↓
Python / PyHive
        ↓
Data Cleaning & Feature Engineering
        ↓
KPI Analysis
        ↓
Exploratory Data Analysis
        ↓
Visualization
        ↓
Machine Learning
        ↓
Credit Risk Classification
        ↓
Business Recommendations
🛠️ Technologies Used
    • Hadoop / HDFS — Big Data storage and processing
    • Hive — Data warehousing and SQL-based analysis
    • Python — Data analysis and machine learning
    • Pandas / NumPy — Data manipulation and numerical analysis
    • Matplotlib / Seaborn — Data visualization
    • Scikit-learn — Machine learning and model evaluation
    • PyHive — Python-to-Hive connectivity
    • Jupyter Notebook — Interactive analysis
🤖 Machine Learning
The project evaluates credit-risk classification using:
    • Random Forest Classifier
    • Logistic Regression baseline
    • Stratified train/test split
    • Class balancing
    • Classification report
    • Confusion matrix
    • Feature importance analysis
The selected Random Forest model achieved 73.81% accuracy on the held-out test set, compared with 66.42% for the Logistic Regression baseline.
📊 Key Findings
The analysis identified debt-to-income ratio, delayed payments, number of loans, and minimum-payment-only behavior as important indicators of credit risk.
The dataset contains three credit-risk categories:
    • Good
    • Standard
    • Poor
The analysis also found that some commonly assumed factors, including occupation and credit utilization ratio, showed limited discriminative power in this dataset.
📁 Repository Structure
Credit-Risk-Intelligence-System/
│
├── README.md
├── requirements.txt
│
├── data/
│   └── credit_data.csv
│
├── notebooks/
│   └── Credit_Risk_Analysis.ipynb
│
├── outputs/
│   └── figures/
│
├── report/
│   └── Project_Report.md
│
└── hadoop-hive/
    └── README.md
🚀 How to Use
Clone the repository:
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd Credit-Risk-Intelligence-System
Install the Python dependencies:
pip install -r requirements.txt
Open the notebook:
jupyter notebook notebooks/Credit_Risk_Analysis.ipynb
The notebook contains the analysis workflow and saved outputs for easy review directly on GitHub.
📚 Project Documentation
    • Project Report: report/Project_Report.md
    • Analysis Notebook: notebooks/Credit_Risk_Analysis.ipynb
    • Python Dependencies: requirements.txt
    • Dataset: data/credit_data.csv
⚠️ Note
The Hadoop/Hive components documented in the project require a configured Hadoop/Hive environment. The Python analysis and machine-learning stages can be reviewed through the Jupyter notebook.

Project: Credit Risk Intelligence System
Specialization: Big Data Analytics Using Hadoop & Data Science Using Python













# 📊 Credit Risk Assessment & Classification Model

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange?logo=jupyter&logoColor=white)](https://jupyter.org/)
[![Pandas](https://img.shields.io/badge/Pandas-Data%20Manipulation-150458?logo=pandas)](https://pandas.pydata.org/)
[![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-Machine%20Learning-F7931E?logo=scikit-learn)](https://scikit-learn.org/)
[![Hadoop/Hive](https://img.shields.io/badge/Hadoop-Hive%20Integration-yellow?logo=apachehadoop)](https://hive.apache.org/)

## 📑 Table of Contents
1. [Project Overview](#-project-overview)
2. [Key Highlights](#-key-highlights)
3. [Dataset](#-dataset)
4. [Tech Stack](#-tech-stack)
5. [Project Pipeline](#-project-pipeline)
6. [Key Insights from EDA](#-key-insights-from-eda)
7. [Model Performance](#-model-performance)
8. [How to Run](#-how-to-run)
9. [Future Scope](#-future-scope)

---

## 📖 Project Overview
This project implements an **end-to-end Machine Learning pipeline** to classify customers into credit risk categories (**Good, Standard, Poor**). By leveraging a dataset of 100,000 customer records ingested from a **HIVE database**, the project aims to assist financial institutions in making data-driven loan approval and risk management decisions. 

The project covers the entire data science lifecycle: from big data ingestion (Hadoop/Hive), rigorous data cleaning, advanced feature engineering, comprehensive Exploratory Data Analysis (EDA), to supervised model building and evaluation.

## 🌟 Key Highlights
* **Big Data Integration:** Successfully connected to a HIVE database using `PyHive` and JDBC to ingest 100,000 records.
* **Data Leakage Prevention:** Identified and corrected data leakage during the preprocessing phase to ensure model integrity.
* **Domain-Driven Feature Engineering:** Created highly predictive financial KPIs such as `debt_to_income_ratio`, `emi_burden_ratio`, and `savings_ratio`.
* **Comprehensive EDA:** Generated deep-dive visualizations across demographics, income, loan performance, and behavioral patterns.
* **Class Imbalance Handling:** Addressed the imbalanced nature of the target variable (Standard: 53%, Poor: 29%, Good: 18%) using class weighting and stratified evaluation metrics.

## 🗄️ Dataset
* **Source:** HIVE Data Warehouse (`credit_project.credit_data`)
* **Size:** 100,000 records × 28 features
* **Target Variable:** `credit_score` (Categorical: Good, Standard, Poor)
* **Key Features:** Demographics (age, occupation), Financials (income, salary, debt), Credit Metrics (utilization, history age, inquiries), and Behavioral data (payment behavior, loan types).

## 🛠️ Tech Stack
| Category | Technologies |
| :--- | :--- |
| **Programming** | Python 3.12 |
| **Data Manipulation** | Pandas, NumPy |
| **Visualization** | Matplotlib, Seaborn |
| **Machine Learning** | Scikit-Learn (Random Forest, Logistic Regression) |
| **Big Data / DB** | Hadoop, Hive, PyHive |
| **Environment** | Jupyter Notebook |

## 🔄 Project Pipeline
1. **Data Ingestion:** Extracted data from HIVE using `pd.read_sql` and saved a local checkpoint.
2. **Data Cleaning:** Handled missing values, parsed multi-valued `type_of_loan` strings, removed PII (`name`, `ssn`), and corrected data types.
3. **Feature Engineering:** 
   * Converted currency (USD to INR) for business presentation.
   * Engineered composite ratios: **DTI**, **EMI Burden**, and **Savings Ratio**.
4. **Exploratory Data Analysis (EDA):** Visualized distributions, correlations, and category-wise breakdowns to understand risk drivers.
5. **Model Building:** Trained **Random Forest** and **Logistic Regression** classifiers.
6. **Evaluation:** Assessed models using Accuracy, Precision, Recall, F1-Score, Confusion Matrix, and Feature Importance plots.

## 💡 Key Insights from EDA
* **Engineered Features Win:** Composite ratio features like `debt_to_income_ratio` and `emi_burden_ratio` proved to be significantly stronger predictors of credit risk than raw metrics like `credit_utilization_ratio`.
* **Occupation is Weak:** Occupation alone is not a strong standalone predictor of credit score, as income spreads across occupations were relatively narrow.
* **Behavioral Patterns:** The most common payment behavior was `"Low_spent_Small_value_payments"` (28.6%), indicating a large portion of the customer base uses credit conservatively.
* **Loan Types:** The specific *type* of loan held did not meaningfully differentiate credit risk, but the *number* of loans held showed a stronger positive correlation with risk.

## 📈 Model Performance
The **Random Forest Classifier** outperformed the Logistic Regression baseline, effectively handling the non-linear relationships in the financial data.

| Metric | Random Forest | Logistic Regression |
| :--- | :---: | :---: |
| **Overall Accuracy** | **68.08%** | 65.86% |
| **Good (F1-Score)** | 0.63 | 0.50 |
| **Poor (F1-Score)** | **0.70** | 0.66 |
| **Standard (F1-Score)** | **0.69** | 0.69 |

*Note: Feature Importance analysis highlighted `outstanding_debt`, `credit_mix`, and `interest_rate` as the top predictors.*

![Confusion Matrix](charts/chart_confusion_matrix_fixed.png)
![Feature Importance](charts/chart_feature_importance.png)
*(Note: Ensure the image paths match the actual saved PNGs in your repository)*

## 🚀 How to Run
1. **Clone the repository:**
   ```bash
   git clone https://github.com/YourUsername/Credit-Risk-Assessment.git
   cd Credit-Risk-Assessment

