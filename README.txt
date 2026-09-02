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

