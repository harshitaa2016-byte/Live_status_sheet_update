# Airflow DAG Monitor 🛰️

This Python script checks the status of specific Airflow DAGs across multiple environments and updates a Google Sheet with the results.

It's particularly useful for operations teams to track the state of deployment pipelines or data validation processes by site.

---

## 🚀 Features

- Connects to multiple Airflow environments via REST API
- Supports Google Sheets API for input/output
- Searches for DAG runs matching specific site IDs
- Tracks the status of key tasks within each DAG
- Outputs failed tasks and task states per site
- Writes a structured summary back into Google Sheets

---

## 📊 Output Format

The Google Sheet is updated with the following columns:

| Site ID | Airflow | DAG | Run ID | State | Failed Tasks | Tracked Task Status |
|---------|---------|-----|--------|-------|----------------|----------------------|

---

## 🛠️ Requirements

- Python 3.7+
- A Google service account with Sheets API access
- Access to the Airflow REST APIs (v1)

### Python Dependencies

Install them using pip:

```bash
pip install requests google-api-python-client google-auth google-auth-httplib2
⚙️ Configuration
1. Google Sheets
Update the following in the script:

python
Copy code
SPREADSHEET_ID = 'your_google_sheet_id_here'
SERVICE_ACCOUNT_FILE = 'path/to/your/service_account.json'
📌 Important: The sheet must have a header named Site ID in cell A1, and list site names below it.

2. Airflow Environments
Replace the placeholders in the airflows dictionary:

python
Copy code
airflows = {
    "Env1": {
        "base_url": "https://your-airflow-instance-1/api/v1",
        "auth": ('your_username', 'your_password')
    },
    ...
}
✅ You can monitor as many environments as needed.

3. DAG & Tasks to Monitor
Update these if you're tracking different DAGs or tasks:

python
Copy code
dags = ["WRDarkDUDeployE2E"]
tracked_tasks = [
    "DataValidation",
    "DeployDU",
    "RestoreHostPath",
    "UpgradeDU",
    "UpgradeRU",
    "PostRANCheck"
]
▶️ Usage
Activate your Python environment and run:

bash
.\airflow_env\Scripts\Activate
python check_airflow_status.py
The script will:

Read site names from your Google Sheet.

Query all matching DAG runs across all Airflow environments.

Collect task statuses.

Update the same Google Sheet with a detailed report.
