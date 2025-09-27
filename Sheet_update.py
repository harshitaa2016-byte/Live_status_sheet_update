import time
import requests
import urllib3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Config (Sanitized) ---
SPREADSHEET_ID = 'your_google_sheet_id_here'
SHEET_RANGE = 'Sheet1!A1:Z1000'

# Replace with your actual Airflow instances and credentials
airflows = {
    "Env1": {
        "base_url": "https://your-airflow-instance-1/api/v1",
        "auth": ('your_username', 'your_password')
    },
    "Env2": {
        "base_url": "https://your-airflow-instance-2/api/v1",
        "auth": ('your_username', 'your_password')
    },
    "Env3": {
        "base_url": "https://your-airflow-instance-3/api/v1",
        "auth": ('your_username', 'your_password')
    }
}

# DAG and task names to track
dags = ["WRDarkDUDeployE2E"]
tracked_tasks = [
    "DataValidation",
    "DeployDU",
    "RestoreHostPath",
    "UpgradeDU",
    "UpgradeRU",
    "PostRANCheck"
]

# Path to service account key file for Google Sheets API
SERVICE_ACCOUNT_FILE = 'path/to/your/service_account.json'


def get_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes
    )
    return build('sheets', 'v4', credentials=creds)


def read_sites():
    service = get_sheets_service()
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Sheet1!A1:Z1000').execute()
    values = result.get('values', [])
    if not values or len(values) < 2:
        print("No site data found in the sheet.")
        return []
    headers = values[0]
    if "Site ID" not in headers:
        print("Sheet must contain 'Site ID' column.")
        return []
    idx = headers.index("Site ID")
    sites = [row[idx].strip() for row in values[1:] if len(row) > idx and row[idx].strip()]
    return sites


def get_dag_runs_batch(base_url, dag_id, auth, limit=500, offset=0):
    url = f"{base_url}/dags/{dag_id}/dagRuns?order_by=-execution_date&limit={limit}&offset={offset}"
    response = requests.get(url, auth=auth, verify=False)
    response.raise_for_status()
    return response.json().get('dag_runs', [])


def get_task_instances(base_url, dag_id, dag_run_id, auth):
    url = f"{base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(url, auth=auth, verify=False)
    response.raise_for_status()
    return response.json().get('task_instances', [])


def update_sheet_with_results(results):
    service = get_sheets_service()
    sheet = service.spreadsheets()

    headers = ["Site ID", "Airflow", "DAG", "Run ID", "State", "Failed Tasks", "Tracked Task Status"]
    values = [headers]

    for res in results:
        row = [
            res.get("Site ID", ""),
            res.get("Airflow", ""),
            res.get("DAG", ""),
            res.get("Run ID", ""),
            res.get("State", ""),
            res.get("Failed Tasks", ""),
            res.get("Tracked Task Status", "")
        ]
        values.append(row)

    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range="Sheet1!A1:Z1000").execute()

    body = {'values': values}
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body=body
    ).execute()
    print(f"✅ Sheet updated with {len(values) - 1} rows.")


def main():
    print("🚀 Starting DAG status check for WRDarkDUDeployE2E...")

    sites = read_sites()
    print(f"🔍 Found {len(sites)} sites to check.")

    if not sites:
        print("⚠️ No sites to check. Exiting.")
        return

    results_to_update = []

    for site in sites:
        found_any = False
        for airflow_name, airflow_info in airflows.items():
            base_url = airflow_info["base_url"]
            auth = airflow_info["auth"]
            dag_id = "WRDarkDUDeployE2E"

            all_runs = []
            offset = 0
            batch_size = 100
            max_limit = 2000

            while offset < max_limit:
                try:
                    runs = get_dag_runs_batch(base_url, dag_id, auth, limit=batch_size, offset=offset)
                except Exception as e:
                    print(f"⚠️ Failed to fetch DAG runs from {airflow_name}: {e}")
                    break

                if not runs:
                    break
                all_runs.extend(runs)
                offset += batch_size

            matched_runs = [run for run in all_runs if site.lower() in run.get("dag_run_id", "").lower()]
            if not matched_runs:
                continue

            found_any = True

            for run in matched_runs:
                run_id = run.get("dag_run_id")
                state = run.get("state")

                try:
                    task_instances = get_task_instances(base_url, dag_id, run_id, auth)
                    task_states = {t["task_id"]: t.get("state", "unknown") for t in task_instances}
                except Exception as e:
                    print(f"⚠️ Could not fetch tasks for {run_id}: {e}")
                    task_states = {}

                tracked_status = {task: task_states.get(task, "not_found") for task in tracked_tasks}
                failed_tasks = [task for task, st in tracked_status.items() if st == "failed"]

                results_to_update.append({
                    "Site ID": site,
                    "Airflow": airflow_name,
                    "DAG": dag_id,
                    "Run ID": run_id,
                    "State": state,
                    "Failed Tasks": ", ".join(failed_tasks),
                    "Tracked Task Status": str(tracked_status)
                })

        if not found_any:
            results_to_update.append({
                "Site ID": site,
                "Airflow": "",
                "DAG": "",
                "Run ID": "",
                "State": "No DAG run found",
                "Failed Tasks": "",
                "Tracked Task Status": ""
            })

    update_sheet_with_results(results_to_update)
    print("\n✅ Script finished.")


if __name__ == "__main__":
    main()
