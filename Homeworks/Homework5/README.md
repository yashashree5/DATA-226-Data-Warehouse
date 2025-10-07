
## 📘 Homework #5 – Porting Homework #4 to Airflow (13 pts)

### 🎯 Objective
Port the previous homework (#4) into **Apache Airflow**, implementing the ETL workflow using Airflow tasks, connections, and variables.

---

### 🧩 Tasks Breakdown

#### (+2 pts) Create Tasks Using `@task` Decorator
- Define Airflow tasks using the `@task` decorator (refer to provided GitHub example).  
- You may use as many tasks as needed.  
- Ensure proper **task dependencies** are established using the `>>` or `<<` operators.

---

#### (+1 pt) Set Up Alpha Vantage API Key as an Airflow Variable
- Create a variable for your Alpha Vantage API key in **Admin → Variables**.  
- Retrieve it in your DAG using:
  ```python
  from airflow.models import Variable
  api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
  ```
- Capture a screenshot of the Variables page (Example ①).

---

#### (+2 pts) Set Up Snowflake Connection
- Create a **Snowflake Connection** under **Admin → Connections**.  
- Use the connection in your Airflow DAG (refer to the provided GitHub example).  
- Capture a screenshot of the Connection detail page (Example ②).

---

#### (+5 pts) Implement and Run the Full DAG
- Ensure the **overall DAG** is correctly implemented and runs successfully in Airflow.  
- Include your **GitHub link** with the entire code (2 pts).  
- Implement a **full refresh** process using **SQL transactions** (3 pts).

---

#### (+2 pts) Capture Required Screenshots
Include two screenshots of your Airflow Web UI:
1. Airflow **homepage** showing the DAG (Example ③).  
2. Airflow **log screen** for the DAG run (Example ④).

---

#### (+1 pt) Formatting and Documentation
- Ensure the DAG code and README are **well-formatted and documented**.  
- Use clear variable names, comments, and consistent indentation.

---

### ✅ Deliverables
- Airflow DAG code (`homework_5.py`)
- Screenshots:
  - Admin → Variables  
  - Admin → Connection  
  - DAG homepage  
  - DAG log screen  
- GitHub repository link

---

### 📂 Folder Structure Example
```
Homeworks/
└── Homework5/
    ├── homework_5.py
    ├── Homework_5.pdf
    ├── README.md   <-- this file
```

---

### 🧑‍💻 Author
*Yashashree Shinde*  
DATA-226: Data Warehouse  
*Instructor: [Keeyong Han]*  
*Semester: Fall 2025*
