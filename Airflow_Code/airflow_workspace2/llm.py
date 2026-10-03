"""
LLM-based DAG and Standard JSON generation using Azure OpenAI.
Converts Excel data to Standard JSON, then Standard JSON to Airflow DAG Python files.
"""
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional

from pipeline import ensure_dir

# ============================================================================
# SYSTEM PROMPTS
# ============================================================================

System_Prompt = """
ROLE
You are an expert Apache Airflow Python developer who converts a Standard JSON job definition into a production-ready DAG. Return only a complete Python file inside a fenced code block: ```python ... ```. Do not include explanations, reasoning, or commentary outside the code block.
 
INPUT
You will receive one Standard JSON job definition containing fields such as:
- GlobalAppID
- TaskDescription
- TaskName (use this to generate dynamic task name like {TaskName}_ExecuteTask)
- StartDate (ISO-8601 format with time, e.g., "2025-09-25T15:36:11.000095") - MUST be parsed into pendulum.datetime()
- ExecutionTimeout (minutes; optional → default 0)
- StrEmailTo (list of emails; optional → default ['OutreachDevTeam2@cognizant.com'])
- Frequency: dictionary of only the environments with scheduling data (e.g., {"SIT": [...], "PROD": [...]})
- Tags: quoted string of comma-separated tags
- AppCriticality: application criticality level (e.g., "Non-Critical", "Critical")
- CriticalJob: job criticality indicator (e.g., if "Yes" then JC_YES, and if "No" then JC_NO)
- JobType: job type that determines DAG format (e.g., "Informatica Jobs", "PeopleSoft", "Windows Task", etc.)
- AppType: application type (e.g., "1C", "Non1C", "PeopleSoft", "IICS")
- Nonc should be formatted without dashes (e.g., "1C", not "Non-1C")
- ServiceNow: AssignmentGroup, Service, ServiceOffering, Impact, Urgency
- Operator: server_name, command, command_argument, command_timeout (milliseconds), poke_interval, taskflow_id, task_type
 

OUTPUT (Single Python file; no prose)
Produce a compilable, PEP 8-compliant Airflow DAG with:
CRITICAL — IMPORT STATEMENTS MUST MATCH EXACTLY: The import statements (and the sys.path.insert line, where present) at the top of the generated file MUST be copied 
EXACTLY as given in the matching EXAMPLE (1, 2, 3, or 4) for the selected JobType/format — same modules, same imported names, same order, same lines. Do NOT add extra imports, 
omit any listed import, reorder them, merge/split lines, or substitute a different but "equivalent" import (e.g., NEVER use 'from airflow import DAG' or 'from airflow.models.param import Param' —
 the correct import is 'from airflow.sdk import DAG, Param' as shown in every EXAMPLE). This rule applies independently per format — 1C, PeopleSoft, IICS, and SQL Jobs each have their own exact import block; do not mix them.
SPECIAL RULES FOR SQL JOBS:
    - If JobType is "SQL Jobs":
        * SQL Jobs is a FULLY INDEPENDENT format. It is NOT a variant of the 1C format and 1C-format rules (e.g., server_name='', GMSA-based command derivation, 1C tag conventions) MUST NEVER be applied to SQL Jobs. Use ONLY the SQL Jobs rules below and the EXAMPLE 4 template.
        * Use the SQL Jobs format as shown in EXAMPLE 4 below.
        * CRITICAL — NON-NEGOTIABLE: The ScheduledTaskOperator server_name MUST ALWAYS be the literal string 'localhost'. NEVER use '' (empty string, which is the 1C rule) or any other value. This applies to EVERY SQL Job, no exceptions.
        * CRITICAL — NON-NEGOTIABLE: The ScheduledTaskOperator task_type MUST ALWAYS be the literal string 'Sql Job'. NEVER derive it from GMSA Enabled, Task Type column, or any 1C logic. This applies to EVERY SQL Job, no exceptions.
        * WRONG (1C rule, FORBIDDEN for SQL Jobs): server_name=''. CORRECT (SQL Jobs, ALWAYS): server_name='localhost'.
        * WRONG (1C rule, FORBIDDEN for SQL Jobs): task_type derived from GMSA Enabled ("Task Scheduler"/"Application Executable"). CORRECT (SQL Jobs, ALWAYS): task_type='Sql Job'.
    * The DAG id MUST be in the format f'{GlobalAppID}-{JobName}_{server_type}' (where GlobalAppID is the application ID and JobName is the job name from JSON, without any auto-added BJ_ prefix or ExecuteTask suffix).
    * CRITICAL — NO ARTIFICIAL 'BJ' PREFIX FOR SQL JOBS: Do NOT artificially prepend 'BJ-' or 'BJ_' to the dag_id (e.g., NOT '2735-BJ-Planned-Effort-Update_{server_type}' when the real JobName is just 'Planned Effort Update').
      If JobName from JSON already starts with an added 'BJ-'/'BJ_' prefix segment (i.e., it is NOT part of the genuine job name from Excel), 
    STRIP that added prefix before building dag_id. HOWEVER, if 'BJ' is a genuine, natural part of the job name itself (e.g., the real Excel JobName legitimately contains "BJ" as a substring), preserve it as-is — do NOT strip legitimate data.
    * CRITICAL FOR dag_id: Replace ONLY SPACES with hyphens (KEEP UNDERSCORES AS-IS). Examples: "Planned Effort Update" -> "Planned-Effort-Update", "EDS_DE_Data_Porting" -> "EDS_DE_Data_Porting" (no underscore conversion)
    * Example dag_id formats (CORRECT, no artificially-added BJ prefix): f'2735-Planned-Effort-Update_{server_type}', f'2735-EDS_DE_Data_Porting_{server_type}'. Example (INCORRECT, must never be produced when JobName has no genuine 'BJ'): f'2735-BJ-Planned-Effort-Update_{server_type}'.
    * CRITICAL FOR command field (SQL Jobs): Use the EXACT command value from the JSON "command" field (which contains the raw ExecutableLocation value with NO replacements). Do NOT modify spaces, slashes, or any other characters. Use it as-is.
    * Example command values from JSON (no transformation): 'Planned Effort Update', 'EDS_DE_Data_Porting' (no extra characters, suffixes, prefixes, or file extensions).
        * You MUST use the exact command_timeout value from the JSON input for ScheduledTaskOperator:
            - command_timeout in generated DAG code MUST always be an INTEGER (without quotes).
            - If JSON provides command_timeout as a numeric string (e.g., "2700000"), convert it to integer 2700000 in the DAG code.
            - If JSON provides 0 or "0", emit integer 0.
            - Do NOT recalculate or hardcode a different value; preserve the same numeric value from JSON, but as integer.
            - This rule is STRICT: the generated DAG code must use integer command_timeout.
        * In servername_parameters, the app_id field MUST be formatted as 'app_id': '{GlobalAppID}_SQL' (e.g., 'app_id': '357_SQL'), not just the numeric app id.
        * The task_id for the ScheduledTaskOperator MUST NOT have an artificially-added 'BJ' prefix; use only the genuine job name, followed by _ExecuteTask and server_type. For example: task_id=f'ActCBTEMPActApproval_ExecuteTask_{server_type}'. (If the genuine Excel job name itself legitimately contains 'BJ', preserve it as-is.)
        * The final bare line (task declaration/variable reference) in the generated DAG script MUST be strictly {JobName}_ExecuteTask (e.g., ActCBTEMPActApproval_ExecuteTask), reflecting the genuine job name exactly — do NOT artificially add a 'BJ_' prefix that isn't part of the real job name.
        * All other structure and parameters must follow the SQL Jobs template in EXAMPLE 4 below.
- Values taken solely from JSON or explicit defaults
- FORMAT SELECTION BASED ON JobType:
    * If JobType is "Informatica Jobs": Use EXAMPLE 3 (IICS format) with IICSOperator
    * If JobType is "PeopleSoft": Use EXAMPLE 2 (PeopleSoft format) with DBStatusOperator
    * If JobType is "SQL Jobs": Use EXAMPLE 4 (SQL Jobs format) with the above SQL Job rules and dag id format as above
    * For all other JobType values: Use EXAMPLE 1 (1C format) with ScheduledTaskOperator
- For 1C format: The task_id for the ScheduledTaskOperator, use only the job name, followed by _ExecuteTask and server_type. For example: task_id=f'ActCBTEMPActApproval_ExecuteTask_{server_type}'.
- For 1C format: Use ScheduledTaskOperator with task name {TaskName}_ExecuteTask
- For 1C format: ScheduledTaskOperator server_name MUST always be empty string ''
- CRITICAL FOR 1C format dag_id: The DAG id MUST be in the format f'{TaskName}_{server_type}' (TaskName is the TaskName (Outside OperatorList) from JSON, without any auto-added BJ_ prefix or ExecuteTask suffix).
  * CRITICAL — NO ARTIFICIAL 'BJ' PREFIX FOR 1C JOBS: Do NOT artificially prepend 'BJ-' or 'BJ_' to the dag_id (e.g., NOT '357-BJ-CFR-CRS-DataDelete_{server_type}' when the real JobName is just 'CFR-CRS-DataDelete'). HOWEVER, if 'BJ' is a genuine, 
  natural part of the job name itself (e.g., the real Excel JobName legitimately contains "BJ" as a substring), preserve it as-is.
  * CRITICAL FOR dag_id: Replace ONLY SPACES in JobName with hyphens (KEEP UNDERSCORES AS-IS).
  * CRITICAL: The dag_id itself MUST NOT include an '_ExecuteTask' suffix — that suffix belongs ONLY to the ScheduledTaskOperator task_id/variable name, never to the dag_id.
  * Example dag_id (CORRECT): f'357-CFR-CRS-DataDelete_{server_type}'. Example (INCORRECT, must never be produced): f'357-BJ-CFR-CRS-DataDelete_ExecuteTask_{server_type}'.
- For 1C format command handling (CRITICAL GMSA Enabled rules):
  * Read task_type from JSON Properties
  * If task_type is "Task Scheduler" (GMSA Enabled="yes"): Command MUST be the EXACT "command" value already present in the input JSON (which is the raw ExecutableLocation value from Excel, with NO replacements/derivation from dag_id). Do NOT modify it,
    do NOT include .exe extension or paths, do NOT rebuild it from GlobalAppID/TaskName/dag_id.
  * If task_type is "Application Executable" (GMSA Enabled="no"): Command MUST be in .exe format (e.g., "383\\ImpactIndexBatchJob.exe"), extracted from ExecutablePath column
  * For 1C format jobs, command_timeout MUST always be INTEGER (e.g., 0, 360000), never string
- For PeopleSoft format: Use DBStatusOperator with dataset-based scheduling
- For IICS format: Use IICSOperator with taskflow_id and task_type parameters
- For IICS format: The DAG id MUST be in the format f'{GlobalAppID}-BJ-{JobName}_ExecuteTask' (where GlobalAppID is the application ID, JobName is the job name without prefix). Example: f'1598-BJ-CL-Track1_PROD_ExecuteTask'.
- Notifications behaving per enable_notifications
- Final bare line containing only the task reference identifier
- Frequency dictionary: Copy ALL environments from input JSON Frequency field. For environments with empty string values (manual/one-time jobs), use empty string "" instead of cron array (e.g., {"DEV": ['*/5 * * * *'], "PT": "", "PROD": ['0 3 * * *']})
 
BEHAVIORAL INVARIANTS
- Parse Frequency dictionary from JSON: use ONLY environments present in input, preserve empty strings for manual/one-time frequencies
- CHECK JobType FIELD TO DETERMINE FORMAT (evaluate in this exact order):
  * JobType == "Informatica Jobs" → Use IICS format (Example 3)
  * JobType == "PeopleSoft" → Use PeopleSoft format (Example 2)
  * JobType == "SQL Jobs" → Use SQL Jobs format (Example 4). CRITICAL: This is a SEPARATE, INDEPENDENT branch — SQL Jobs MUST NEVER fall through to or be treated as 1C format. Do NOT apply any 1C-format rule (server_name='', GMSA command derivation, 1C tags) to SQL Jobs.
  * JobType == anything else (i.e., NOT Informatica Jobs, NOT PeopleSoft, NOT SQL Jobs) → Use 1C format (Example 1)
- For ALL formats (1C, PeopleSoft, IICS, SQL): Build default_args with description, start_date (pendulum Asia/Kolkata), execution_timeout, email settings, catchup, trigger_rule, queue
- CRITICAL: For ALL formats (1C, SQL), handle retry keys conditionally:
    include BOTH keys in default_args:
        - 'retries': value from JSON RetryCount (default 0 if missing)
        - 'retry_delay': timedelta(minutes=<JSON RetryDelay>) (default timedelta(minutes=0) if missing)
  - CRITICAL: For ALL formats (1C, PeopleSoft, IICS, SQL), default_args MUST NOT include an 'sla' key. Do not generate `sla=...` or `'sla': ...` in default_args.
- For ALL formats: Handle EmailOnRetry from JSON - if "True" set email_on_retry to True in default_args, otherwise set to False
- Implement notifications (1C format only): 
  * Initialize on_success_callback = None
  * Inside if enable_notifications: set on_failure_callback in default_args, set sla_miss_callback
  * Read EmailOnSuccess from JSON: if "True", add on_success_callback = mail_sms_notifications.send_success_notification INSIDE if enable_notifications block (3rd line) AND include on_success_callback=on_success_callback in DAG constructor
  * If EmailOnSuccess is NOT "True": omit on_success_callback assignment and parameter entirely
  * CRITICAL: No if statements in generated code - YOU decide based on JSON value
- start_date for all formats: Parse StartDate from JSON ISO-8601 format into pendulum.datetime(year, month, day, hour, minute, second, microsecond, tz="Asia/Kolkata")
- DAG id for 1C format: Use f'{GlobalAppID}-{JobName}_{server_type}' directly in the DAG constructor (no separate dag_id variable). JobName has ONLY spaces replaced with hyphens (underscores preserved), no artificial 'BJ' prefix unless genuine, and NO '_ExecuteTask' suffix.
- DAG id for PeopleSoft/IICS formats: Use simple string format
- Tags for 1C format: Include app name, portfolio, criticality, JobCriticality, AppType, server_type as list directly in DAG constructor. App name tag format: '{AppName} ({GlobalAppID})' (e.g., 'MeetingConcierge (3)', 'Outreach (383)'). Criticality tag: use AppCriticality value as-is (e.g., 'Non-Critical', 'Critical').
- Tags for SQL Jobs format: Include app name, portfolio, criticality, JobCriticality, AppType, server_type as a list directly in DAG constructor (same tag STRUCTURE as 1C, but SQL Jobs remains its own independent format — do not apply other 1C rules). Differences from 1C tags:
  * App name tag format MUST be '{AppName} ({GlobalAppID})' (e.g., 'Mainspring (2735)', 'Cognizant LEARN (357)'). Do NOT use '{GlobalAppID} - {AppName}'.
  * Criticality tag MUST be prefixed with 'AC_' (e.g., 'AC_Critical', 'AC_Non-Critical'). Do NOT use bare 'Critical' or 'Non-Critical' for SQL Jobs.
- Tags for IICS format: Include ['informatica', 'etl', 'monitoring', GlobalAppID, PortfolioName, JobCriticality] as list in DAG constructor
- For 1C format: Include params dictionary with ServiceNow Assignment Group, Service, ServiceOffering, Impact, Urgency
- For PeopleSoft format: Include params dictionary with Category, SubCategory, Service, ServiceOffering, impact, urgency (lowercase)
- For IICS format: DAG id and task_id MUST be identical and include the _ExecuteTask suffix
- For IICS format: Include params dictionary with task_type, task_id, Service, ServiceOffering, CMDB_CI, impact, urgency
- For IICS format: Extract task_type and taskflow_id from JSON OperatorList Properties and use them in params as task_type and task_id
- For IICS format: Define get_iics_operator_args(params) helper function that returns operator arguments
- For IICS format: STRICTLY in get_iics_operator_args(): ONLY TWO conditions allowed:
  * if params['task_type'] == 'TF': args['taskflow_id'] = params['task_id']
  * elif params['task_type'] == 'MTT': args['task_federated_id'] = params['task_id']
  * else: raise ValueError(f"Unknown task_type: {params['task_type']}")
  * FORBIDDEN: Do NOT add extra elif clauses for other task_type values like 'Application Executable', 'DSS', 'WORKFLOW', 'PCS', etc.
- For IICS format: Use IICSOperator with **get_iics_operator_args(dag.params) to unpack arguments dynamically
- For ALL formats: Handle EmailOnRetry from JSON - if "True" set email_on_retry to True in default_args, otherwise set to False
 
CRITICAL FORMATTING RULES FOR 1C FORMAT (MUST FOLLOW EXACTLY):
- FORBIDDEN: Do NOT create intermediate variables named dag_params, dag_tags, or dag_kwargs
- FORBIDDEN: Do NOT use dictionary unpacking (**dag_kwargs, **kwargs, **params) in DAG constructor
- FORBIDDEN: Do NOT store tags in a separate variable before passing to DAG constructor
- FORBIDDEN: Do NOT store params in a separate variable before passing to DAG constructor
- FORBIDDEN: ABSOLUTELY NO hardcoded string comparisons - NEVER write: if "True" == "True", if "False" == "True", if True == True, or if "anything" == "anything"
- FORBIDDEN: Do NOT add "or True" or "or False" after any condition
- FORBIDDEN: Do NOT add any if statements in the generated code for checking EmailOnSuccess
- REQUIRED: Write tags list DIRECTLY inline in DAG constructor. App name tag format differs by JobType:
    * For 1C / non-SQL jobs: '{AppName} ({GlobalAppID})' (e.g., tags=['MeetingConcierge (3)', 'Portfolio', ...])
    * For SQL Jobs: '{AppName} ({GlobalAppID})' (e.g., tags=['Mainspring (2735)', 'Portfolio', ...])
- REQUIRED: Write params dictionary DIRECTLY inline in DAG constructor (e.g., params={"ServiceNow Assignment Group": Param(...), ...})
- REQUIRED: Write ALL DAG parameters explicitly (schedule_interval, timetable, max_active_runs, sla_miss_callback, etc.)
- FORBIDDEN: Do NOT add `'sla'` in `default_args` for any format.
- REQUIRED: Pass command and command_timeout values directly to ScheduledTaskOperator (no separate variables)
- REQUIRED: Use the exact numeric command_timeout value from JSON but emit it as INTEGER in DAG code (e.g., JSON "2700000" -> DAG command_timeout=2700000)
- REQUIRED: EmailOnSuccess handling - YOU must check the JSON value and generate different code accordingly:
  * Read the EmailOnSuccess value from the input JSON
  * If the value is "True": generate code that sets on_success_callback = mail_sms_notifications.send_success_notification AND includes on_success_callback=on_success_callback in DAG()
  * If the value is NOT "True": generate code that does NOT set on_success_callback and does NOT include on_success_callback parameter in DAG()
  * Generate the final code based on what you read - do NOT add conditional checks in the generated Python code
- For 1C format only - command: Use single quotes with double backslashes (e.g., '383\\ImpactIndexBatchJob.exe')
- For 1C format only - command_timeout: ALWAYS use integer (e.g., 0, 2700000), never string.
- CRITICAL - Final bare line task reference:
  * For 1C format: End file with bare task reference matching the ScheduledTaskOperator variable name (e.g., BJ_CL_ExecuteTask)
  * For PeopleSoft format: End file with EXACTLY the DBStatusOperator variable name (typically process_status), NOT the 1C task name
  * For IICS format: End file with EXACTLY the IICSOperator variable name (typically iics_task), NOT the 1C task name 
  * NEVER use BJ_{TaskName}_ExecuteTask for PeopleSoft or IICS - use the actual operator variable name
 
FEW-SHOT EXAMPLES:
 
EXAMPLE 1 - JobType: Windows Task, Unix Cron, or any other type (1C FORMAT)
```python
import pendulum
import sys
import os
from datetime import timedelta
from airflow.sdk import DAG, Param
from batchjob.notification.mail_sms_notification import mail_sms_notifications
from scheduled_task.multiple_trigger_timetable import MultipleTriggerTimeTable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from conf import server_type, queue, enable_notifications
from scheduled_task.operators.scheduled_task_operator import ScheduledTaskOperator
 
frequency = {"DEV": ['*/5 * * * *'], "SIT": ['*/5 * * * *'], "PROD": ['0 3 * * *']}
 
default_args = {
    'description': 'To generate the task card.',
    'start_date': pendulum.datetime(2026, 4, 24, 12, 45, 30, 835064, tz="Asia/Kolkata"),
    'execution_timeout': timedelta(minutes=0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'mail_list': ['MCSupport@cognizant.com'],
    'email_on_failure': False,
    'email': None,
    'catchup': False,
    'trigger_rule': 'all_success',
    'queue': queue,
}
 
on_success_callback = None
if enable_notifications:
    default_args['on_failure_callback'] = mail_sms_notifications.send_failure_notification
 
dag = DAG(
    f'3-BJ-CL4-MobileTask_{server_type}',
    default_args=default_args,
    schedule=MultipleTriggerTimeTable(
        schedule_time_list=frequency.get(server_type),
        timezone='Asia/Kolkata',
        Stop_Jobs=True
    ),
    max_active_runs=1,
    tags=['MeetingConcierge (3)', 'IT Operations', 'Non-Critical', 'JC_NO', '1C', server_type],
    params={
        "ServiceNow Assignment Group": Param("MeetingConcierge L2 Support", type="string"),
        "Service": Param("Network&Systems Services-Technical Support-Meeting Concierge", type="string"),
        "ServiceOffering": Param("Meeting Concierge [3] - App Support", type="string"),
        "Impact": Param("Low", type="string"),
        "Urgency": Param("Low", type="string"),
    }
)
 
servername_parameters = {"server_type": server_type, "app_id": "3"}
 
BJ_CL4_MOBILETASK_ExecuteTask = ScheduledTaskOperator(
    task_id=f'BJ_CL4_MOBILETASK_ExecuteTask_{server_type}',
    extra_options={"verify": False},
    servername_parameters=servername_parameters,
    server_name='',
    task_type='Application Executable',
    command='3-CL4-MobileTask\\CTS.MC.Mobiletask.exe',
    command_argument='',
    command_timeout=360000,
    poke_interval=60,
    dag=dag
)

BJ_CL4_MOBILETASK_ExecuteTask
```
 
EXAMPLE 2 - JobType: PeopleSoft (PeopleSoft FORMAT)
```python
import pendulum
import sys
import os
from airflow.sdk import DAG, Param
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from HCM_operator.db_status_operator import DBStatusOperator
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from conf import queue
 
CT_APP_MSG_R_dataset = Dataset("file:///batchjob/Peoplesoft_HCM/28-CT_APP_MSG_R_PROD.txt")
 
default_args = {
    'description': 'PeopleSoft HCM batch job processing.',
    'start_date': pendulum.datetime(2025, 9, 25, 15, 36, 11, 95, tz="Asia/Kolkata"),
    'execution_timeout': timedelta(minutes=90),
    'email_on_retry': False,
    'mail_list': ["JOMDevTeam@cognizant.com", "ITAppL1Monitoring@cognizant.com"],
    'email_on_failure': False,
    'email': None,
    'catchup': False,
    'trigger_rule': 'all_success',
    'queue': "PROD",
  
}
 
with DAG(
    dag_id="28-CT_APP_MSG_R_PROD",
    start_date= pendulum.datetime(2025, 9, 25, 15, 36, 11, 95, tz="Asia/Kolkata"),
    default_args=default_args,
    schedule=[CT_APP_MSG_R_dataset],
    catchup=False,
    max_active_runs=10,
    max_active_tasks=10,
    tags=["HCM", "consumer", '28'],
    params={
        "Category": Param("application", type="string"),
        "SubCategory": Param("performance", type="string"),
        "Service": Param("Events & Alert Management", type="string"),
        "ServiceOffering": Param("Infrastructure Assets & Tools - Applications", type="string"),
        "impact": Param("low", type="string"),
        "urgency": Param("low", type="string"),
    }
) as dag:
    process_status = DBStatusOperator(
        task_id="process_CT_APP_MSG_R_status",
        job_id="28-CT_APP_MSG_R_PROD"
    )
 
    process_status
```
EXAMPLE 3 - JobType: Informatica Jobs(IICS FORMAT)
```python
from airflow.sdk import DAG, Param
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from iics_plugin import IICSOperator
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'mail_list': ["Samson.D@cognizant.com"],
}
 
with DAG(
    dag_id='iics_task_1',
    default_args=default_args,
    description='Trigger IICS Taskflow or Mapping Task based on params',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['iics', 'informatica', 'etl', 'monitoring'],
    params={
        "task_type": Param("TF", type="string"),
        "task_id": Param("2mdgCmQfEiIlqjRRA65kc8", type="string"),
        "Service": Param("Network&Systems Services-Technical Support - Enterprise DataStore", type="string"),
        "ServiceOffering": Param("Enterprise Data  Store[2795] - Admin Non-prod Support", type="string"),
        "CMDB_CI": Param("Enterprise DataStore - 2795", type="string"),
        "impact": Param("low", type="string"),
        "urgency": Param("low", type="string"),
    }
) as dag:
 
    def get_iics_operator_args(params):
        args = {
            'task_type': params['task_type'],
            'poke_interval': 30,
            'timeout': 3600,
        }
        if params['task_type'] == 'TF':
            args['taskflow_id'] = params['task_id']
        elif params['task_type'] == 'MTT':
            args['task_federated_id'] = params['task_id']
        else:
            raise ValueError(f"Unknown task_type: {params['task_type']}")
        return args
 
    iics_task = IICSOperator(
        task_id='run_iics_task',
        **get_iics_operator_args(dag.params)
    )
 
    iics_task
```

EXAMPLE 4 - JobType: SQL Jobs (SQL JOBS FORMAT)
```python
import pendulum
import sys
import os
from datetime import timedelta
from airflow.sdk import DAG, Param
from batchjob.notification.mail_sms_notification import mail_sms_notifications
from scheduled_task.multiple_trigger_timetable import MultipleTriggerTimeTable
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from conf import server_type, queue, enable_notifications
from scheduled_task.operators.scheduled_task_operator import ScheduledTaskOperator
 
frequency = {"PROD": ["0 22 * * *"]}
 
default_args = {
    'description': 'to update the records [LMS].[STG_RDPCertificateUserStatus]',
    'start_date': pendulum.datetime(2025, 4, 9, 16, 59, 38, 403, tz="Asia/Kolkata"),
    'execution_timeout': timedelta(minutes=0),
    'email_on_retry': False,
    'mail_list': ["ITOpsTMACAL2@cognizant.com"],
    'email_on_failure': False,
    'email': None,
    'catchup': False,
    'trigger_rule': 'all_success',
    'queue': queue,
}
 
on_success_callback = None
if enable_notifications:
    default_args['on_failure_callback'] = mail_sms_notifications.send_failure_notification
    on_success_callback = mail_sms_notifications.send_success_notification
 
dag = DAG(
    f'357-CFR-CRS-DataDelete_{server_type}',
    default_args=default_args,
    schedule=MultipleTriggerTimeTable(
        schedule_time_list=frequency.get(server_type),
        timezone='Asia/Kolkata',
        Stop_Jobs=True
    ),
    max_active_runs=1,
    tags=['Cognizant learn (357)', 'Fulfilment IT systems', 'AC_Non-Critical', 'JC_No', 'Non1C', server_type],
    on_success_callback=on_success_callback,
    params={
        "ServiceNow Assignment Group": Param("Cognizant Learn L2 Support", type="string"),
        "Service": Param("Human Resources-Technical Support-Cognizant LEARN", type="string"),
        "ServiceOffering": Param("Cognizant LEARN [357] - App Support", type="string"),
        "Impact": Param("Low", type="string"),
        "Urgency": Param("Low", type="string"),
    }
)
 
servername_parameters = {"server_type": server_type, "app_id": "357_SQL"}
 
CFRCRSDataDelete_ExecuteTask = ScheduledTaskOperator(
    task_id=f'CFRCRSDataDelete_ExecuteTask_{server_type}',
    extra_options={"verify": False},
    servername_parameters=servername_parameters,
    server_name='localhost',
    task_type='Sql Job',
    command='CFR_CRS_DataDelete',
    command_argument='',
    command_timeout=0,
    poke_interval=60,
    dag=dag
)
 
CFRCRSDataDelete_ExecuteTask
```

""".strip()
 

System_Prompt_JSON = """
ROLE
You are an expert data engineer who converts consolidated Excel job metadata into a production-ready Standard JSON job definition for Apache Airflow. Return only a single valid JSON object inside a fenced code block: ```json ... ```. Do not include explanations, notes, or commentary outside the block.

INPUT
You will receive consolidated Excel data from Master and BatchJobInventory sheets with fields such as:
- Application identifiers (GlobalAppID, StrAppName, PortfolioName)
- TaskDescription: Job description text (REQUIRED - extract from Excel)
- Command/ExecutablePath: Full Windows path to executable (REQUIRED - shorten to format like "383\\ImpactIndexBatchJob.exe")
- Environment schedules (DEV_Schedule, SIT_Schedule, UAT_Schedule, PT_Schedule, PROD_Schedule)
- ServiceNow metadata (AssignmentGroup, Service, ServiceOffering, Impact, Urgency)
- Notification lists (NotificationDL, SuccessCommunicationRequired)
- Timeouts (ExecutionTimeout, CommandTimeout in minutes)
- Date/time fields (StartDate, CreatedDate, ModifiedDate)
- Retry configs (RetryYESNO, RetryInterval, # of Retries). EXACT MAPPING (do not confuse these two):
  - JSON field "RetryCount" MUST be taken from the Excel column "# of Retries" (may appear as "%23 of Retries" due to URL-encoding of "#"; treat "%23" as "#").
  - JSON field "RetryDelay" MUST be taken from the Excel column "RetryInterval".
  - Do NOT swap these two values. Do NOT invent or default values not present in the Excel row.
- SLA: read from column "SLA (Execution DurationInSeconds)" OR "Baseline Duration (Execution DurationInMinutes)". Despite the column name, both columns contain values in minutes or hours (with optional unit suffixes like "mins", "hrs"). 
The final Sla value in JSON MUST always be in minutes as a STRING. See Sla field extraction rules in the output section for full parsing logic.

OUTPUT (Single JSON; no prose)
Produce a Standard JSON job definition that:
- Is immediately parseable (valid JSON)
- Reflects Excel inputs faithfully
- Encodes per-environment schedules using cron lists (include ONLY environments with data)
- Frequency: JSON object with all present environments:
  * For valid cron expressions (e.g., "*/5 * * * *", "0 3 * * *"): Use cron array format (e.g., {"SIT": ["*/5 * * * *"]})
  * For human-readable schedules (e.g., "Every 15 minutes", "Daily at 3am"): Convert to cron array (e.g., {"SIT": ["*/15 * * * *"]})
    * CRITICAL: Preserve explicit minute start/range windows when converting. Do NOT simplify offset/ranged cron to */N. Example: if schedule implies starting at minute 15 every 10 minutes within the hour, generate "15-59/10 * * * *" (NOT "*/10 * * * *").
  * For manual/one-time keywords ONLY ("One Time", "@manual", "once", "manual run", "manual", "ad-hoc", "on-demand"): Use empty string "" (e.g., {"DEV": ""})
  * CRITICAL: If frequency value is ALREADY a valid cron expression, DO NOT treat it as manual - keep it as cron array
- ALL environments found in the Excel data MUST appear in the Frequency dictionary - either with cron arrays or empty strings (except DR environment)
- Contains exactly one operator entry with DYNAMIC key: {ExtractedJobName}_ExecuteTask
- TaskName outside OperatorList format:
  * For all JobTypes: {GlobalAppID}-{JobName} (e.g., "2735-RHMS_SALM_FlatTable", "357-ActCBT-EMPActApproval")
- StrAppName format: "AppName (GlobalAppID)" (e.g., "Outreach (383)" NOT just "Outreach")
- Command path extraction from ExecutablePath / ExecutableLocation:
  * CRITICAL — 'platinum\' STRIPPING RULE (applies ONLY to 1C format AND ONLY when GMSA Enabled="no"; do NOT apply when GMSA Enabled="yes", and do NOT apply to SQL Jobs, PeopleSoft, or IICS/Informatica): If the raw ExecutableLocation/ExecutablePath value from Excel contains 'platinum\' (case-insensitive) anywhere in it, 
  STRIP everything up to and including the LAST occurrence of 'platinum\' and use ONLY the remaining substring as the basis for the command value. Example: "D:\Platinum\383\ImpactIndexBatchJob.exe" -> use "383\ImpactIndexBatchJob.exe".
    If 'platinum\' is not present, use the value as-is (no change). This rule is applied BEFORE building the final .exe command for GMSA="no" jobs. For GMSA="yes" jobs, 
  the ExecutableLocation value MUST be used exactly as-is with NO stripping.
  * CRITICAL FOR SQL JOBS (JobType == "SQL Jobs"):
    - Extract the "command" field ONLY from the ExecutableLocation column in Excel (do NOT use JobName, TaskName, or any other field)
    - Do NOT add any prefix (BJ_, BJ, etc.), suffix (_ExecuteTask, etc.), or file extension
    - CRITICAL: If ExecutableLocation is empty or missing, DO NOT fallback to JobName, TaskName, or any other field. The command MUST be extracted from ExecutableLocation only.
    - The "server_name" field in Properties MUST be "localhost" by default
  * CRITICAL FOR 1C FORMAT (GMSA Enabled):
    - If GMSA Enabled is "yes": task_type MUST be "Task Scheduler" and command MUST be the EXACT value from the ExecutableLocation field in Excel (e.g., "3-BJ-CL4-MobileTask")
    - If GMSA Enabled is "no": task_type MUST be "Application Executable" and command MUST be .exe format (e.g., "383\\ImpactIndexBatchJob.exe")
  * For 1C format with GMSA Enabled="Yes" (Task Scheduler):
    1. CRITICAL: The command field MUST be the EXACT value from the ExecutableLocation column in Excel — do NOT build it from dag_id, GlobalAppID, or TaskName. Do NOT modify spaces, casing, or any characters. Use it as-is.
    2. This command represents the Windows Task Scheduler task name as entered in Excel.
    3. Example: If ExecutableLocation is "3-BJ-CL4-MobileTask", command must be exactly "3-BJ-CL4-MobileTask".
    4. If ExecutableLocation is empty or missing, DO NOT fall back to dag_id/TaskName/JobName — leave command as the empty/raw value provided.
  * For 1C format with GMSA Enabled="No" (Application Executable):
    1. Take the full path from ExecutablePath / ExecutableLocation field
    2. Apply the 'platinum\' STRIPPING RULE above: if the path contains 'platinum\' (case-insensitive), strip everything up to and including the LAST occurrence of it, and use the remaining substring as the command. Do NOT extract based on GlobalAppID.
    3. Command MUST end with .exe and use backslashes (e.g., "383\\ImpactIndexBatchJob.exe")
    4. Example: If ExecutablePath is "D:\Platinum\383\BJ-CL4-MobileTask\CTS.MC.Mobiletask.exe", extract as "383\\BJ-CL4-MobileTask\\CTS.MC.Mobiletask.exe"
  * For all other 1C/non-SQL jobs (non-Excel scenarios):
    1. Take the full path from ExecutablePath / ExecutableLocation field
    2. Apply the 'platinum\' STRIPPING RULE above if applicable; otherwise use the value as-is.
- upstream and server_name rules:
    * upstream: use empty string "" if not provided
    * For 1C format (non-SQL Scheduled Task): server_name MUST always be empty string ""
- ExecutionTimeout is default to 0.
- command_timeout extraction:
  1. If command_timeout column exists, convert to milliseconds (multiply by 60000) as STRING
  2. If missing, default to "0"
- timeout field in OperatorList Properties:
    1. CRITICAL FOR SQL JOBS: If JobType is "SQL Jobs", set "timeout" to the SAME milliseconds value as "command_timeout" (both as STRING). Example: if command_timeout is "600000", timeout MUST be "600000".
    2. For non-SQL jobs, keep existing behavior (default "0" unless explicitly provided).
- taskflow_id extraction (CRITICAL FOR IICS JOBS):
  1. If "taskflow_id" or "Taskflow ID" column exists in Excel, extract that exact value (e.g., "7uyV8gGoBZskZmR8HYtzi4")
  2. This is a REQUIRED field for IICS/Informatica jobs
  3. Place this value in OperatorList Properties as "taskflow_id" field
  4. This identifies the specific IICS task/workflow to execute
- task_type extraction (CRITICAL):
  0. CRITICAL FOR SQL JOBS: If JobType is "SQL Jobs", set task_type to exactly "Sql Job" — ignore any "Task Type" column and do NOT infer from JobName. This overrides all rules below.
  1. For all other JobTypes: First check if "Task Type" column exists in Excel - if yes, use that exact value
  2. If "Task Type" column is missing, infer from JobName prefix:
     - If JobName starts with "mt_" or "m_": use "DSS" (Mapping Task)
     - If JobName starts with "tf_": use "TF" or "WORKFLOW" (TaskFlow)
     - If JobName starts with "lr_": use "PCS" (Linear Replication)
     - Otherwise: use "Application Executable" as default
  3. Place this value in OperatorList Properties as "task_type" field
  4. This is CRITICAL for IICSOperator - determines which IICS task type to execute
  5. Examples: "DSS", "TF", "WORKFLOW", "PCS", "Application Executable", "Sql Job"
- GMSA Enabled override (FOR 1C FORMAT ONLY):
  1. This rule ONLY applies when AppType/Nonc is "1C"
  2. If "GMSA Enabled" column exists in Excel, the LLM must set task_type accordingly:
     - If value is "yes" (case-insensitive): Override task_type to "Task Scheduler". Command MUST be the EXACT value from the ExecutableLocation field in Excel (no modification, no dag_id derivation). Example: ExecutableLocation="3-BJ-CL4-MobileTask" → command="3-BJ-CL4-MobileTask"
     - If value is "no" (case-insensitive): Set task_type to "Application Executable". Command MUST be .exe format (e.g., "383\\ImpactIndexBatchJob.exe")
  3. If GMSA Enabled is missing, follow the normal task_type extraction rules above
  4. CRITICAL INSTRUCTION FOR LLM:
     - When generating JSON for 1C format: Check if GMSA Enabled="yes" in the input data
     - If GMSA="yes": Set task_type="Task Scheduler" AND set command to the EXACT ExecutableLocation value from Excel (do NOT derive from dag_id/TaskName)
     - If GMSA="no": Set task_type="Application Executable" AND ensure command is in .exe format from ExecutablePath
  5. For 1C format with GMSA Enabled="Yes": The command is the EXACT value from the ExecutableLocation field in Excel (e.g., "3-BJ-CL4-MobileTask"). This represents the Windows Task Scheduler task name as entered in Excel — do NOT build it from dag_id or GlobalAppID.
  6. For 1C format with GMSA Enabled="No": The command is the executable path in .exe format (extracted from ExecutablePath/ExecutableLocation column). Apply the 'platinum\' STRIPPING RULE (strip everything up to and including the LAST occurrence of 'platinum\', case-insensitive) if present. Do NOT extract based on GlobalAppID.
- JobType extraction (REQUIRED):
  1. Extract JobType field from Excel input as-is (e.g., "Informatica Jobs", "PeopleSoft", "Windows Task", "Unix Cron", etc.)
  2. This field is MANDATORY and must be included in the output JSON
  3. Do NOT modify or transform this value
- Nonc extraction:
  1. If AppType column exists, use that exact value (e.g., "1C", "Non1C", "PeopleSoft", "IICS")
  2. If AppType missing and TaskName starts with "Non1C", set to "Non1C"
  3. Otherwise set to "1C"
- Do NOT include separate AppType field in output (Nonc will contain the AppType value, but JobType is separate)
- Sla field extraction (CRITICAL — output must ALWAYS be in minutes as a STRING):
  Both columns may contain a plain number OR a value with a unit suffix. Despite the column name "SLA (Execution DurationInSeconds)", it does NOT contain seconds — it contains minutes or hours just like the other column. Apply the same unit-aware parsing rules to BOTH columns:
  Unit-aware parsing rules (apply to whichever column is used):
    - If the value contains "hr", "hrs", "hour", or "hours" (case-insensitive) → extract the numeric part and multiply by 60 (e.g., "2 hrs" → "120", "1 hour" → "60")
    - If the value contains "min", "mins", "minute", or "minutes" (case-insensitive) → extract the numeric part and use as-is (e.g., "30 mins" → "30", "45 minutes" → "45")
    - If the value is a plain number with no unit suffix → treat as MINUTES, use as-is (e.g., "40" → "40")
  1. Check for column "SLA (Execution DurationInSeconds)" first — if present and non-empty, apply unit-aware parsing above
  2. If that column is absent or empty, use "Baseline Duration (Execution DurationInMinutes)" — apply unit-aware parsing above
  3. Store the final result as a STRING. The Sla field in the output JSON MUST always represent minutes. Do NOT default to "4" unless the Excel value is explicitly 4.
- Criticality mapping (CRITICAL):
    1. Extract AppCriticality from Excel exactly (e.g., "Critical", "Non-Critical").
    2. Set CriticalJob based on AppCriticality:
         - If AppCriticality is "Critical" → CriticalJob MUST be "Yes"
         - If AppCriticality is "Non-Critical" → CriticalJob MUST be "No"
    3. Do NOT output conflicting values (e.g., AppCriticality="Critical" with CriticalJob="No" is invalid).
- StartDate: Use TODAY'S DATE AND TIME in ISO-8601 format
- EmailOnSuccess: "True" if SuccessCommunication? is "Yes", else "False"
- If there is one "No" under SuccessCommunication? set EmailOnSuccess to "False", and one "No" for RetryCommunication? set EmailOnRetry to "False"

BEHAVIORAL INVARIANTS
- Map Excel fields to JSON fields semantically
- Build Frequency object for ALL environments found in Excel data (do NOT exclude environments with manual/one-time frequencies)
- Frequency parsing rules (CRITICAL):
  1. If value is a valid cron expression (contains * or numbers with spaces): Keep as cron array ["cron_expression"]
  2. If value is human-readable (e.g., "Every 15 minutes", "Daily"): Convert to cron array
    - Preserve explicit minute start/range windows in output cron; do NOT collapse to */N when an offset/range is present (e.g., keep "15-59/10 * * * *" as-is).
  3. If value is EXACTLY one of these keywords: "One Time", "@manual", "once", "manual run", "manual", "ad-hoc", "on-demand": Use empty string ""
  4. NEVER use empty string "" or [""] for actual schedules - only for explicit manual/one-time keywords
- For environments with valid schedules: parse human-readable frequencies to cron expression arrays
- Normalize NotificationDL: split by commas, trim whitespace
- Use ISO-8601 for dates/timestamps
- Convert numeric fields to integers
- Convert command_timeout from minutes to milliseconds
- Shorten command paths
- OperatorList key format:
  * For all JobTypes: use {JOBNAME}_ExecuteTask. Example: "CommandLogCleanup_ExecuteTask"

  * CRITICAL — SPACES IN JOBNAME: If the Excel JobName contains spaces, replace EVERY space with a underscore "_"  when building {JOBNAME} for the OperatorList key (and for TaskName/task_id/taskid_new below, which all follow the same {JOBNAME} value). KEEP UNDERSCORES AS-IS — do NOT convert existing underscores to hyphens. 
  Do NOT remove spaces without replacing them. Example: JobName "Project Request AutoClosure" → OperatorList key "Project_Request_AutoClosure_ExecuteTask" (NOT "ProjectRequestAutoClosure_ExecuteTask" and NOT "Project-Request-AutoClosure_ExecuteTask").
  * CRITICAL - Hyphens in JobName: If the Excel JobName contains hyphens "-", replace them with underscores "_" in the OperatorList key. Example: JobName "RHMS-SALM FlatTable" → OperatorList key "RHMS_SALM_FlatTable_ExecuteTask"
  - TaskName inside OperatorList Properties must match the OperatorList key (e.g., "CommandLogCleanup_ExecuteTask")
  - task_id inside OperatorList Properties must follow the same rule as the OperatorList key above 
  - taskid_new inside OperatorList Properties must be {JOBNAME}_ExecuteTask_{server_type} eg: "taskid_new":"OneC_BatchJob_IM_Reports_ExecuteTask_{server_type}"
TaskName outside OperatorList format:
  * For all JobTypes : {GlobalAppID}-{JobName} (e.g., "2735-RHMS_SALM_FlatTable"). If JobName contains spaces, replace them with hyphens here too (SAME rule as dag_id, keep underscores as-is) (e.g., JobName "Project Request AutoClosure" with GlobalAppID 2735 → "2735-Project-Request-AutoClosure").


ONE-SHOT EXAMPLE:
```json
{
  "OperatorList": {
    "BJ_CL_ExecuteTask": {
      "OperatorType": "Scheduled Task",
      "TaskName": "BJ_CL_ExecuteTask",
      "Editable": false,
      "Properties": {
        "command": "383\\ImpactIndexBatchJob.exe",
        "command_argument": "",
        "taskid_new": "BJ_CL_ExecuteTask_{server_type}",
        "upstream": "",
                "server_name": "",
        "command_timeout": "0",
        "extra_options": "{\"verify\":False}",
        "timeout": "0",
        "task_type": "DSS",
        "taskflow_id": "7uyV8gGoBZskZmR8HYtzi4",
        "task_id": "BJ_CL_ExecuteTask",
        "poke_interval": "60"
      }
    }
  },
  "Impact": "Low",
  "Urgency": "Low",
  "ServiceAssignmentGroup": "Outreach L2 Support",
  "Service": "Legal-Technical Support-Outreach",
  "ServiceOffering": "Outreach [383] - App Support",
  "ExecutionTimeout": "0",
  "Owner": "Airflow",
  "JobType": "Windows Task",
  "Nonc": "1C",
  "Sla": "4",
  "StrPortfolioName": "Corporate services IT systems",
  "StrAppName": "Outreach (383)",
  "Stepname": null,
  "Frequency": {
    "DEV": ["*/5 * * * *"],
    "SIT": ["*/15 * * * *"],
    "UAT": ["0 2 * * *"],
    "PT": "",
    "PROD": ["0 3 * * *"]
  },
  "TimeZone": "Asia/Kolkata",
  "Command": null,
  "DAGTaskID": 0,
  "GlobalAppID": 383,
  "PortfolioID": 0,
  "PortfolioName": "Corporate services IT systems",
  "Tags": "\"383 - Outreach\",\"Corporate services IT systems\",\"Non-Critical\"",
  "TaskName": "383-BJ-CL",
  "AppCriticality": "Non-Critical",
  "TaskDescription": "Monthly report triggered to business POC.",
  "StartDate": "2025-12-09T00:00:00",
  "StartTime": null,
  "DependsOnPast": "False",
  "RetryYESNO": "False",
  "EmailOnRetry": "False",
  "CriticalJob": "No",
  "EmailOnSuccess": "True",
  "RetryCount": 0,
  "RetryDelay": 0,
  "DailyDays": 1,
  "CatchUp": "False",
  "CreatedBy": "2321703",
  "CreatedDate": "2025-12-09T10:06:37.000000",
  "ModifiedBy": "2321703",
  "ModifiedDate": "2025-12-09T10:06:37.000000",
  "StrEmailTo": ["OutreachDevTeam2@cognizant.com"]
}
```
""".strip()

# ============================================================================
# AZURE OPENAI CLIENT INITIALIZATION
# ============================================================================
 
def _load_azure_config_from_json() -> Dict[str, str]:
    """Load Azure OpenAI settings from config.json (non-sensitive) and Azure Key Vault via Variable.get() (sensitive)."""
    from airflow.models import Variable
 
    cfg_path = Path(__file__).parent / "config.json"
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            azure = cfg.get("azure_openai", {})
            return {
                "endpoint": azure.get("endpoint", ""),
                "deployment": azure.get("deployment", ""),
                "api_key": Variable.get("Azure_Openai_Api_Key", default_var=""),
                "api_version": azure.get("api_version", "2024-08-01-preview"),
            }
        except Exception as e:
            print(f"[LLM] Error loading config: {e}")
            return {}
    return {}
 
 
def _init_azure_client():
    """Initialize Azure OpenAI client using config.json + Azure Key Vault secrets."""
    try:
        from openai import AzureOpenAI
    except ImportError as e:
        print(f"[LLM] AzureOpenAI SDK import failed: {e}")
        return None
 
    azure = _load_azure_config_from_json()
    ep = azure.get("endpoint", "")
    dep = azure.get("deployment", "")
    key = azure.get("api_key", "")
    api_version = azure.get("api_version", "")
 
    if not ep or not dep or not key:
        print("[LLM] Missing Azure OpenAI config (check config.json and Airflow Variables)")
        return None
 
    try:
        client = AzureOpenAI(azure_endpoint=ep, api_key=key, api_version=api_version)
        print("[LLM] Azure OpenAI client initialized")
        return client, dep
    except Exception:
        print("[LLM] Azure client init failed")
        return None
  
# ============================================================================
# DAG GENERATION FROM STANDARD JSON
# ============================================================================

def _build_system_prompt() -> str:
    """Returns the DAG system prompt."""
    return System_Prompt


def _build_user_prompt(std_json: Dict[str, Any]) -> str:
    """Build user prompt for DAG generation with dynamic task name."""
    raw_task = std_json.get("TaskName", "CL")
    
    # Remove numeric prefix (e.g., "357-BJ-HRIS" → "BJ-HRIS")
    if raw_task and "-" in str(raw_task):
        parts = str(raw_task).split("-", 1)
        if parts[0].isdigit():
            raw_task = parts[1]
    else:
        raw_task = str(raw_task)

    # Keep genuine 'BJ' prefix as-is; don't strip it.
    jobname_for_dag = raw_task
    
    # Clean and format task name for operator (preserve CamelCase; avoid full uppercase)
    op_task = raw_task.replace("-", "_").replace(" ", "_")

    # Only known synthetic AppType prefixes are stripped — these are never
    # part of the genuine job name.
    if op_task.startswith("NON1C_"):
        op_task = op_task[6:]
    elif op_task.startswith("1C_"):
        op_task = op_task[3:]

    # Preserve original token casing from the input job name.
    # Do not force uppercase/camelcase transformation here.

    # For ALL job types (SQL Jobs and all others).
    job_type_check = std_json.get("JobType", "").strip().lower()
    task_name = f"{op_task}_ExecuteTask"

    # For IICS, build DAG id and task_id without server_type
    job_type = std_json.get("JobType", "")
    appid = str(std_json.get("GlobalAppID", ""))
    server_type = str(std_json.get("server_type", "{server_type}"))
    dag_id_iics = f"{appid}-{jobname_for_dag}_ExecuteTask"
    task_id_iics = dag_id_iics  # Task ID is same as DAG ID for IICS

    # Pre-computed dag_id for 1C/SQL Jobs to avoid LLM re-deriving (and duplicating) the app ID.
    dag_id_1c = f"{appid}-{jobname_for_dag}_{{server_type}}"

    # Parse Frequency if it's a string
    frequency_data = std_json.get("Frequency", "{}")
    if isinstance(frequency_data, str):
        try:
            frequency_dict = json.loads(frequency_data)
        except:
            frequency_dict = {}
    else:
        frequency_dict = frequency_data

    std_json_modified = std_json.copy()
    std_json_modified["Frequency"] = frequency_dict

    # CRITICAL: Check JobType to determine which DAG format to use
    job_type = std_json_modified.get("JobType", "")
    if not job_type:
        # Fallback: if JobType is missing, try AppType or Nonc
        job_type = std_json_modified.get("AppType", std_json_modified.get("Nonc", "Windows Task"))
        print(f"[LLM] WARNING - JobType missing, using fallback: {job_type}")
    job_type_normalized = str(job_type).strip()

    # Determine format based on JobType
    if job_type_normalized.lower() == "informatica jobs" or job_type_normalized.upper() == "IICS":
        format_type = "IICS"
        example_used = "EXAMPLE 3 - Informatica Jobs (IICS FORMAT with IICSOperator)"
    elif job_type_normalized.lower() == "peoplesoft":
        format_type = "PeopleSoft"
        example_used = "EXAMPLE 2 - PeopleSoft (PeopleSoft FORMAT with DBStatusOperator)"
    elif job_type_normalized.lower() == "sql jobs":
        format_type = "SQL Jobs"
        example_used = "EXAMPLE 4 - SQL Jobs (SQL JOBS FORMAT with ScheduledTaskOperator)"
    else:
        # All other JobType values (Windows Task, Unix Cron, etc.) use 1C format
        format_type = "1C"
        example_used = f"EXAMPLE 1 - {job_type_normalized} (1C FORMAT with ScheduledTaskOperator)"

    print(f"[LLM] DAG Generation - JobType detected: '{job_type_normalized}'")
    print(f"[LLM] DAG Generation - Format selected: {format_type}")
    print(f"[LLM] DAG Generation - Using: {example_used}")

    # Build base prompt based on format type
    if format_type == "IICS":
        format_type = "IICS"
        example_used = "EXAMPLE 3 - Informatica Jobs (IICS FORMAT with IICSOperator)"
        # For IICS, enforce DAG id and task_id format
        base_prompt = f"""
Standard JSON Input:
{json.dumps(std_json_modified, indent=2, ensure_ascii=False)}

IMPORTANT: For IICS jobs:
- DAG id MUST be exactly: {dag_id_iics}
- Task id MUST be exactly: {task_id_iics}
- DAG id and task_id does NOT include server_type
Do NOT add extra prefixes or duplicate the app ID or BJ in the DAG id.
Do NOT use the generic task name format (BJ_JOBNAME_ExecuteTask) for IICS - use the exact task_id specified above.

Generate complete Python Airflow DAG file following all rules.
Return ONLY code between ```python and ``` with no explanation.
"""
        note = f"\nCRITICAL: JobType is '{job_type_normalized}' - Use EXAMPLE 3 (IICS FORMAT) from the system prompt. Use IICSOperator with params dictionary and get_iics_operator_args() helper function. Do NOT use ScheduledTaskOperator or constants at the top. The DAG id MUST be '{dag_id_iics}' and IICSOperator task_id MUST be '{task_id_iics}'."
    elif job_type_normalized.lower() == "peoplesoft":
        format_type = "PeopleSoft"
        example_used = "EXAMPLE 2 - PeopleSoft (PeopleSoft FORMAT with DBStatusOperator)"
        base_prompt = f"""
Standard JSON Input:
{json.dumps(std_json_modified, indent=2, ensure_ascii=False)}

IMPORTANT: Generate task name as: {task_name}
Do NOT use BJ_ twice or include app ID in task name.
Task name MUST be exactly: {task_name}

Generate complete Python Airflow DAG file following all rules.
Return ONLY code between ```python and ``` with no explanation.
"""
        note = f"\nCRITICAL: JobType is '{job_type_normalized}' - Use EXAMPLE 2 (PeopleSoft FORMAT) from the system prompt. Use DBStatusOperator with dataset-based scheduling. Do NOT use ScheduledTaskOperator."
    else:
        format_type = "1C"
        example_used = f"EXAMPLE 1 - {job_type_normalized} (1C FORMAT with ScheduledTaskOperator)"
        base_prompt = f"""
Standard JSON Input:
{json.dumps(std_json_modified, indent=2, ensure_ascii=False)}

IMPORTANT: Generate task name as: {task_name}
Do NOT use BJ_ twice or include app ID in task name.
Task name MUST be exactly: {task_name}

IMPORTANT: For the DAG id, do NOT re-derive it from the raw TaskName field
(TaskName already includes the GlobalAppID prefix, e.g. "{appid}-{jobname_for_dag}" —
prepending GlobalAppID again would duplicate it, e.g. "{appid}-{appid}-{jobname_for_dag}").
The DAG id MUST be exactly: f'{dag_id_1c}'
Do NOT add an extra '{appid}-' prefix and do NOT duplicate the app ID.

Generate complete Python Airflow DAG file following all rules.
Return ONLY code between ```python and ``` with no explanation.
"""
        note = f"\nCRITICAL: JobType is '{job_type_normalized}' - Use EXAMPLE 1 (1C FORMAT) from the system prompt. Use ScheduledTaskOperator. This is the default format for all non-Informatica and non-PeopleSoft job types. The DAG id MUST be exactly f'{dag_id_1c}' — do NOT duplicate the app ID prefix."

    return (base_prompt + note).strip()


def _extract_code_from_fence(text: str) -> str:
    """Extract code between ```python ... ``` fences."""
    m = re.search(r"```python\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else text.strip()


def _validate_dag_text(code: str, job_type: str = "", global_app_id: str = "") -> list[str]:
    """Validate generated DAG code has required structure. job_type-aware for
    SQL Jobs BJ_ rule and per-format exact import statements."""
    problems = []
    job_type_norm = str(job_type).strip().lower()
    is_sql_job = job_type_norm == "sql jobs"

    # Determine which EXAMPLE's import block applies, per FORMAT SELECTION rules.
    if job_type_norm == "informatica jobs":
        expected_imports = [
            "from airflow.sdk import DAG, Param",
            "from airflow.utils.dates import days_ago",
            "from datetime import timedelta, datetime",
            "from iics_plugin import IICSOperator",
        ]
    elif job_type_norm == "peoplesoft":
        expected_imports = [
            "import pendulum",
            "import sys",
            "import os",
            "from airflow.sdk import DAG, Param",
            "from airflow.datasets import Dataset",
            "from datetime import datetime, timedelta",
            "from HCM_operator.db_status_operator import DBStatusOperator",
            "sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))",
            "from conf import queue",
        ]
    else:
        # 1C and SQL Jobs share identical imports (Example 1 / Example 4).
        expected_imports = [
            "import pendulum",
            "import sys",
            "import os",
            "from datetime import timedelta",
            "from airflow.sdk import DAG, Param",
            "from batchjob.notification.mail_sms_notification import mail_sms_notifications",
            "from scheduled_task.multiple_trigger_timetable import MultipleTriggerTimeTable",
            "sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))",
            "from conf import server_type, queue, enable_notifications",
            "from scheduled_task.operators.scheduled_task_operator import ScheduledTaskOperator",
        ]

    for line in expected_imports:
        if line not in code:
            problems.append(f"Missing/incorrect required import statement (must match EXAMPLE exactly): {line}")

    uses_scheduled_task_operator = job_type_norm not in {"informatica jobs", "peoplesoft"}

    if "frequency = {" not in code and "frequency={" not in code:
        problems.append("frequency dictionary not found")
    if "default_args = {" not in code and "default_args={" not in code:
        problems.append("default_args dictionary not found")
    if uses_scheduled_task_operator:
        if "dag = DAG(" not in code and "dag=DAG(" not in code:
            problems.append("DAG declaration missing")
        if "ScheduledTaskOperator(" not in code:
            problems.append("ScheduledTaskOperator task missing")
    # else:
    #     if "as dag:" not in code and "DAG(" not in code:
    #         problems.append("DAG declaration missing")

    if is_sql_job:
        # SQL Jobs: task/variable name must be *_ExecuteTask. 'BJ' is allowed if genuine.
        if not re.search(r"\w+_ExecuteTask\s*=\s*ScheduledTaskOperator", code):
            problems.append("Task name pattern *_ExecuteTask not found")
        # SQL Jobs are an INDEPENDENT format: server_name MUST be 'localhost' and task_type MUST be 'Sql Job'.
        # These must NEVER be derived from 1C rules (server_name='', GMSA-based task_type).
        if not re.search(r"server_name\s*=\s*['\"]localhost['\"]", code):
            problems.append("FORBIDDEN: SQL Job server_name must be exactly 'localhost' (1C rule of server_name='' must NOT be applied).")
        if not re.search(r"task_type\s*=\s*['\"]Sql Job['\"]", code):
            problems.append("FORBIDDEN: SQL Job task_type must be exactly 'Sql Job' (must NOT be derived from GMSA/1C logic like 'Task Scheduler' or 'Application Executable').")
        # SQL Jobs: servername_parameters app_id MUST be '{GlobalAppID}_SQL', never the bare numeric app id.
        if global_app_id:
            expected_app_id = f"{global_app_id}_SQL"
            if not re.search(r"[\"']app_id[\"']\s*:\s*[\"']" + re.escape(expected_app_id) + r"[\"']", code):
                problems.append(
                    f"FORBIDDEN: servername_parameters app_id must be '{expected_app_id}' "
                    f"(format '{{GlobalAppID}}_SQL'), not the bare numeric app id or any other value."
                )
    elif job_type_norm not in {"informatica jobs", "peoplesoft"}:
        # Task name must be *_ExecuteTask; 'BJ' presence depends on genuine JobName.
        if not re.search(r"\w+_ExecuteTask\s*=\s*ScheduledTaskOperator", code):
            problems.append("Dynamic task name pattern *_ExecuteTask not found")

    if uses_scheduled_task_operator:
        required_params = ["extra_options", "servername_parameters", "server_name", "task_type", "command", "dag=dag"]
        for param in required_params:
            if param not in code:
                problems.append(f"ScheduledTaskOperator missing parameter: {param}")

    return problems


def _ask_llm_for_dag_code(std_json: Dict[str, Any]) -> Optional[str]:
    """Call Azure OpenAI to generate DAG Python code from Standard JSON.
    Validates output against structural rules and performs one repair pass
    (temperature=0) if problems are found, to reduce hallucination risk."""
    client_info = _init_azure_client()
    if client_info is None:
        return None
    client, deployment = client_info

    system = _build_system_prompt()
    user = _build_user_prompt(std_json)
    job_type = str(std_json.get("JobType", "")).strip()
    global_app_id = str(std_json.get("GlobalAppID", "")).strip()

    try:
        print("[LLM] Requesting DAG code from Azure OpenAI...")
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
        )
        raw = resp.choices[0].message.content or ""
        code = _extract_code_from_fence(raw)

        if not code.strip():
            print("[LLM] Empty code from model")
            return None

        # Validate structure; if problems found, do ONE repair pass with explicit feedback.
        problems = _validate_dag_text(code, job_type=job_type, global_app_id=global_app_id)
        if problems:
            print(f"[LLM] DAG validation issues found:\n  - " + "\n  - ".join(problems))
            repair_request = (
                "The generated DAG code has these issues:\n" + "\n".join(f"  - {p}" for p in problems) +
                "\n\nFix these issues and return ONLY corrected Python code fenced with ```python ... ```. "
                "Do not introduce any artificial 'BJ' prefix/segment that is not part of the genuine "
                "Excel job name. Preserve all other correct parts unchanged."
            )
            repair = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": repair_request},
                ],
                temperature=0.0,
            )
            repaired_code = _extract_code_from_fence(repair.choices[0].message.content or "")
            if repaired_code.strip():
                remaining = _validate_dag_text(repaired_code, job_type=job_type, global_app_id=global_app_id)
                if remaining:
                    print(f"[LLM] DAG still has issues after repair:\n  - " + "\n  - ".join(remaining))
                else:
                    print("[LLM] DAG code repaired and validated successfully")
                code = repaired_code
        else:
            print("[LLM] DAG code generated and validated successfully")

        return code.strip()

    except Exception as e:
        print(f"[LLM] Azure call failed: {e}")
        return None


def _enforce_sql_job_operator_fields(code: str) -> str:
    """Safety-net for SQL Jobs: force server_name='localhost' and task_type='Sql Job'.
    Call only after validation confirms these are wrong, not unconditionally."""
    # Force server_name to 'localhost' wherever it appears as a kwarg with any string value.
    code = re.sub(
        r"server_name\s*=\s*['\"][^'\"]*['\"]",
        "server_name='localhost'",
        code,
    )
    # Force task_type to 'Sql Job' wherever it appears as a kwarg with any string value.
    code = re.sub(
        r"task_type\s*=\s*['\"][^'\"]*['\"]",
        "task_type='Sql Job'",
        code,
    )
    return code


def _enforce_sql_job_app_id(code: str, global_app_id: str) -> str:
    """Safety-net for SQL Jobs: force app_id in servername_parameters to
    '{GlobalAppID}_SQL'. Call only after validation confirms it's wrong."""
    if not global_app_id:
        return code
    expected_app_id = f"{global_app_id}_SQL"
    # Match "app_id": "<anything>" or 'app_id': '<anything>' and force it to expected value.
    code = re.sub(
        r"(['\"])app_id\1\s*:\s*['\"][^'\"]*['\"]",
        lambda m: f"{m.group(1)}app_id{m.group(1)}: '{expected_app_id}'",
        code,
    )
    return code


def generate_dag_with_llm_from_standard_json(
    standard_json_path: Path,
    appid: str,
    out_dir: Path,
    job_name: str = None
) -> Path:
    """Generates Airflow DAG Python file from Standard JSON using Azure OpenAI LLM."""

    if not standard_json_path.exists():
        raise FileNotFoundError(f"Standard JSON not found: {standard_json_path}")

    with open(standard_json_path, "r", encoding="utf-8") as f:
        std_json = json.load(f)

    # Request DAG code from LLM (includes internal validation + one repair pass)
    dag_text = _ask_llm_for_dag_code(std_json)

    if dag_text is None or not dag_text.strip():
        raise RuntimeError("Failed to generate DAG code. LLM unavailable or returned empty response.")

    # Deterministic safety net for SQL Jobs: server_name/task_type/app_id must
    # never inherit 1C-format values. 'BJ' presence is only warned, not auto-stripped.
    job_type = str(std_json.get("JobType", "")).strip().lower()
    if job_type == "sql jobs":
        before = dag_text

        # Only correct server_name/task_type if still wrong after repair pass.
        server_name_ok = re.search(r"server_name\s*=\s*['\"]localhost['\"]", dag_text)
        task_type_ok = re.search(r"task_type\s*=\s*['\"]Sql Job['\"]", dag_text)
        if not server_name_ok or not task_type_ok:
            print("[LLM] server_name/task_type still incorrect after repair pass — applying deterministic correction")
            dag_text = _enforce_sql_job_operator_fields(dag_text)

        # Only correct app_id if still wrong after repair pass.
        sql_global_app_id = str(std_json.get("GlobalAppID", "")).strip()
        if sql_global_app_id:
            expected_app_id = f"{sql_global_app_id}_SQL"
            app_id_ok = re.search(r"[\"']app_id[\"']\s*:\s*[\"']" + re.escape(expected_app_id) + r"[\"']", dag_text)
            if not app_id_ok:
                print(f"[LLM] servername_parameters app_id still incorrect after repair pass — forcing '{expected_app_id}'")
                dag_text = _enforce_sql_job_app_id(dag_text, sql_global_app_id)

        if before != dag_text:
            print("[LLM] Deterministic safety-net corrected server_name/task_type/app_id in SQL Job DAG code")
        # Final checks: warn only, don't stop generation. reviewer_agent is the downstream safety net.
        # NOTE: 'BJ' may legitimately be part of the genuine job name; not an error on its own.
        if not re.search(r"server_name\s*=\s*['\"]localhost['\"]", dag_text):
            print(
                "[LLM] WARNING: SQL Job DAG server_name='localhost' not found after deterministic "
                "cleanup. Continuing generation; this should be caught by reviewer_agent."
            )
        if not re.search(r"task_type\s*=\s*['\"]Sql Job['\"]", dag_text):
            print(
                "[LLM] WARNING: SQL Job DAG task_type='Sql Job' not found after deterministic "
                "cleanup. Continuing generation; this should be caught by reviewer_agent."
            )
        if sql_global_app_id:
            expected_app_id = f"{sql_global_app_id}_SQL"
            if not re.search(r"[\"']app_id[\"']\s*:\s*[\"']" + re.escape(expected_app_id) + r"[\"']", dag_text):
                print(
                    f"[LLM] WARNING: SQL Job DAG app_id '{expected_app_id}' not found after "
                    "deterministic cleanup. Continuing generation; this should be caught by reviewer_agent."
                )
    out_dir_path = Path(out_dir)
    ensure_dir(out_dir_path)

    # File naming
    global_app_id = std_json.get("GlobalAppID")
    job_name = std_json.get("TaskName", "CL").replace(" ", "-")

    # Keep genuine 'BJ-'/'BJ_' prefix as-is in filename/dag_id.

    if job_name.startswith(f"{global_app_id}-"):
        dag_filename = f"{job_name}.py"
    else:
        dag_filename = f"{global_app_id}-{job_name}.py"

    dag_file = out_dir_path / dag_filename
    with open(dag_file, "w", encoding="utf-8") as f:
        f.write(dag_text)

    print(f"✅ DAG script generated: {dag_file}")
    return dag_file

# ============================================================================
# STANDARD JSON GENERATION FROM EXCEL
# ============================================================================

def _build_system_prompt_json() -> str:
    """Returns the JSON system prompt."""
    return System_Prompt_JSON


def _build_user_prompt_json(consolidated_json: Dict[str, Any]) -> str:
    """Build user prompt for Standard JSON generation from Excel data."""
    # Extract top-level fields and GMSA Enabled from sheets structure
    app_type = consolidated_json.get("AppType", "")
    task_name = consolidated_json.get("TaskName", "")
    gmsa_enabled = ""
    
    # Extract GMSA Enabled from sheets, promote to top-level for LLM visibility
    if isinstance(consolidated_json, dict) and "sheets" in consolidated_json:
        sheets = consolidated_json["sheets"]
        if isinstance(sheets, dict):
            batch_jobs = sheets.get("BatchJobInventory", [])
            if isinstance(batch_jobs, list) and len(batch_jobs) > 0:
                gmsa_val = batch_jobs[0].get("GMSA Enabled", "")
                if gmsa_val:
                    gmsa_enabled = str(gmsa_val).strip().lower()
                    print(f"[LLM] GMSA Enabled extracted from BatchJobInventory: '{gmsa_val}' → '{gmsa_enabled}'")
    
    # Fallback to Master sheet
    if not gmsa_enabled and isinstance(consolidated_json, dict) and "sheets" in consolidated_json:
        sheets = consolidated_json["sheets"]
        if isinstance(sheets, dict):
            master = sheets.get("Master", [])
            if isinstance(master, list) and len(master) > 0:
                gmsa_val = master[0].get("GMSA Enabled", "")
                if gmsa_val:
                    gmsa_enabled = str(gmsa_val).strip().lower()
                    print(f"[LLM] GMSA Enabled extracted from Master: '{gmsa_val}' → '{gmsa_enabled}'")
    
    # Determine application type
    if app_type.lower() == "peoplesoft":
        determined_type = "PeopleSoft"
    elif app_type.lower() == "iics":
        determined_type = "IICS"
    elif app_type == "1C":
        determined_type = "1C"
    elif app_type == "Non1C":
        determined_type = "Non1C"
    elif isinstance(task_name, str) and task_name.startswith("Non1C"):
        determined_type = "Non1C"
    else:
        # Default to 1C if not explicitly specified
        determined_type = "1C"
    
    # Log which example will be used for JSON generation
    if determined_type == "PeopleSoft":
        example_info = "PeopleSoft-specific structure (DBStatusOperator, dataset scheduling)"
    elif determined_type == "IICS":
          example_info = "IICS-specific structure (IICSOperator, params with task_type/task_id, get_iics_operator_args helper)"
    elif determined_type == "1C":
        example_info = "1C example from system prompt"
    else:
        example_info = f"{determined_type} - adapted from general rules"
    
    print(f"[LLM] JSON Generation - AppType detected: {determined_type}")
    print(f"[LLM] JSON Generation - Using: {example_info}")
    if gmsa_enabled:
        print(f"[LLM] JSON Generation - GMSA Enabled='{gmsa_enabled}' (will affect command format)")
    
    # Build the data to send to LLM, with GMSA Enabled promoted to top-level for clarity
    data_for_llm = consolidated_json.copy()
    if gmsa_enabled:
        data_for_llm["GMSA_Enabled_Extracted"] = gmsa_enabled
    
    base_prompt = f"""
Consolidated Excel Data:
{json.dumps(data_for_llm, indent=2, ensure_ascii=False)}

Generate complete Standard JSON job definition following all rules.
Return ONLY JSON between ```json and ``` with no explanation.
"""
    
    # Add note about which example to use based on AppType
    if determined_type == "PeopleSoft":
        note = "\nNOTE: This is a PeopleSoft application. Adapt the JSON structure for PeopleSoft-specific requirements (DBStatusOperator, dataset scheduling)."
    elif determined_type == "IICS":
        note = "\nNOTE: This is an IICS application. Adapt the JSON structure for IICS-specific requirements (IICSOperator, params with task_type and task_id parameters)."
    elif determined_type == "1C":
        note = "\nNOTE: This is a 1C application. Use the example from the system prompt as reference."
    else:
        note = f"\nNOTE: This is a {determined_type} application. Follow the general rules but adapt for this application type."
    
    # Reminder for GMSA-based command format on 1C jobs
    if determined_type == "1C" and gmsa_enabled:
        gmsa_note = f"\nCRITICAL FOR COMMAND FORMAT: GMSA Enabled='{gmsa_enabled}' for this 1C job. "
        if gmsa_enabled == "yes":
            gmsa_note += "Set task_type='Task Scheduler' and command=the EXACT value from the ExecutableLocation field in Excel (do NOT derive from dag_id/GlobalAppID/TaskName, do NOT modify it, no .exe suffix). Example: ExecutableLocation='3-BJ-CL4-MobileTask' → command='3-BJ-CL4-MobileTask'."
        elif gmsa_enabled == "no":
            gmsa_note += "Set task_type='Application Executable' and command=.exe path taken AS-IS from ExecutableLocation/ExecutablePath, with ONLY the 'platinum\\' prefix stripped if present (strip everything up to and including the LAST occurrence of 'platinum\\', case-insensitive). "
            "Do NOT add a GlobalAppID prefix and do NOT derive the path from GlobalAppID/dag_id/TaskName. "
            "Example: ExecutableLocation='E:\\OneCognizantSource\\Platinum\\CL4-MobileTask\\CTS.MC.Mobiletask.exe' → command='CL4-MobileTask\\\\CTS.MC.Mobiletask.exe'."
            " Example (no platinum present): ExecutableLocation='C:\\App\\383\\ImpactIndexBatchJob.exe' → command='C:\\\\App\\\\383\\\\ImpactIndexBatchJob.exe' (used as-is, unless shortening is separately required by other rules)."
        note += gmsa_note
    
    return (base_prompt + note).strip()


def _extract_json_from_fence(text: str) -> str:
    """Extract JSON between ```json ... ``` fences."""
    m = re.search(r"```json\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else text.strip()


def _validate_standard_json(json_obj: Dict[str, Any]) -> list[str]:
    """Validate generated Standard JSON has required structure."""
    problems = []

    required_keys = [
        "OperatorList", "Impact", "Urgency", "ServiceAssignmentGroup", "Service",
        "ServiceOffering", "Owner", "Sla", "StrAppName", "Frequency", "GlobalAppID",
        "TaskName", "StrEmailTo", "ExecutionTimeout", "Nonc", "StrPortfolioName",
        "TimeZone", "DAGTaskID", "PortfolioID", "PortfolioName", "Tags", "AppCriticality",
        "TaskDescription", "StartDate", "StartTime", "DependsOnPast", "RetryYESNO",
        "EmailOnRetry", "CriticalJob", "EmailOnSuccess", "RetryCount", "RetryDelay",
        "DailyDays", "CatchUp", "CreatedBy", "CreatedDate", "ModifiedBy", "ModifiedDate",
        "Stepname", "Command"
    ]
    for key in required_keys:
        if key not in json_obj:
            problems.append(f"Missing required field: {key}")

    # Validate OperatorList
    if "OperatorList" in json_obj:
        op_list = json_obj["OperatorList"]
        if not isinstance(op_list, dict):
            problems.append("OperatorList must be an object")
        elif len(op_list) == 0:
            problems.append("OperatorList must contain at least one operator")
        elif len(op_list) > 1:
            problems.append(f"OperatorList must contain exactly one operator")
        else:
            operator_key = list(op_list.keys())[0]
            # Key must be {JOBNAME}_ExecuteTask; genuine 'BJ_' prefix is fine.
            if not operator_key.endswith("_ExecuteTask"):
                problems.append(f"Operator key must match pattern {{JOBNAME}}_ExecuteTask, got: {operator_key}")
            
            task = op_list.get(operator_key, {})
            required_task_keys = ["OperatorType", "TaskName", "Editable", "Properties"]
            for tk in required_task_keys:
                if tk not in task:
                    problems.append(f"{operator_key} missing: {tk}")
            
            if "Properties" in task:
                props = task["Properties"]
                required_props = ["command", "command_argument", "taskid_new", "upstream",
                                "server_name", "command_timeout", "extra_options", "timeout",
                                "task_type", "task_id", "poke_interval"]
                for prop in required_props:
                    if prop not in props:
                        problems.append(f"Properties missing: {prop}")

    # Validate Frequency
    if "Frequency" in json_obj:
        freq = json_obj["Frequency"]
        if not isinstance(freq, dict):
            problems.append("Frequency must be an object")

    # Type checks
    if "GlobalAppID" in json_obj:
        try:
            int(json_obj["GlobalAppID"])
        except (ValueError, TypeError):
            problems.append(f"GlobalAppID must be integer")

    for field in ["RetryCount", "RetryDelay", "DAGTaskID", "PortfolioID", "DailyDays"]:
        if field in json_obj:
            try:
                int(json_obj[field])
            except (ValueError, TypeError):
                problems.append(f"{field} must be integer")

    return problems


def _ask_llm_for_standard_json(consolidated_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Call Azure OpenAI to generate Standard JSON from Excel data."""
    client_info = _init_azure_client()
    if client_info is None:
        return None
    client, deployment = client_info

    system = _build_system_prompt_json()
    user = _build_user_prompt_json(consolidated_data)

    try:
        print("[LLM] ============ Requesting Standard JSON from Azure OpenAI ============")
        
        # Log what we're sending to LLM
        app_type = consolidated_data.get("AppType", "")
        gmsa_extracted = consolidated_data.get("GMSA_Enabled_Extracted", "")
        if gmsa_extracted:
            print(f"[LLM] Input Data: AppType='{app_type}', GMSA_Enabled='{gmsa_extracted}'")
        else:
            print(f"[LLM] Input Data: AppType='{app_type}', GMSA_Enabled='NOT FOUND'")
        
        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
        )
        raw = resp.choices[0].message.content or ""
        json_text = _extract_json_from_fence(raw)

        if not json_text.strip():
            print("[LLM] Empty JSON from model")
            return None

        try:
            json_obj = json.loads(json_text)
        except json.JSONDecodeError as e:
            print(f"[LLM] JSON parse error: {e}")
            return None

        # Log what was generated
        if "OperatorList" in json_obj:
            for op_key, op_val in json_obj["OperatorList"].items():
                if isinstance(op_val, dict) and "Properties" in op_val:
                    props = op_val["Properties"]
                    task_type = props.get("task_type", "")
                    command = props.get("command", "")
                    print(f"[LLM] Generated JSON: Operator='{op_key}', task_type='{task_type}', command='{command}'")

        # Validate and repair if needed
        problems = _validate_standard_json(json_obj)
        if problems:
            print(f"[LLM] Validation issues found:\n  - " + "\n  - ".join(problems))
            repair_request = (
                "The generated JSON has these issues:\n" + "\n".join(f"  - {p}" for p in problems) +
                "\n\nFix these issues and return ONLY corrected JSON fenced with ```json ... ```."
            )
            repair = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": repair_request},
                ],
                temperature=0.0,
            )
            json_text = _extract_json_from_fence(repair.choices[0].message.content or "")
            try:
                json_obj = json.loads(json_text)
                print("[LLM] Standard JSON repaired by model")
            except json.JSONDecodeError as e:
                print(f"[LLM] Repair failed: {e}")
                return None
        else:
            print("[LLM] Standard JSON generated and validated successfully")

        return json_obj

    except Exception as e:
        print(f"[LLM] Azure call failed: {e}")
        return None


def strip_platinum_prefix(value: str) -> str:
    """
    Strip everything up to and including the LAST occurrence of 'platinum\\'
    (case-insensitive) from an ExecutableLocation/ExecutablePath value.
    Example: 'D:\\Platinum\\383\\ImpactIndexBatchJob.exe' -> '383\\ImpactIndexBatchJob.exe'
    If 'platinum\\' is not found, the original value is returned unchanged.
    Applies ONLY to the 1C format command derivation.
    """
    if not value:
        return value
    match = None
    for m in re.finditer(r"platinum\\", value, flags=re.IGNORECASE):
        match = m
    if match:
        return value[match.end():]
    return value


def generate_standard_json_with_llm_from_consolidated(
    consolidated_json: Dict[str, Any],
    appid: str,
    out_dir: Path
) -> Path:
    """Generates Standard JSON from consolidated Excel data using Azure OpenAI LLM."""

    # Request Standard JSON from LLM
    std_json = _ask_llm_for_standard_json(consolidated_json)

    if std_json is None or not isinstance(std_json, dict):
        raise RuntimeError("Failed to generate Standard JSON. LLM unavailable or returned invalid response.")

    # Cleanup: Ensure correct operator key
    raw_task = std_json.get("TaskName")
    job_type = str(std_json.get("JobType", "")).strip().lower()
    is_sql_job = job_type == "sql jobs"

    if not raw_task or not str(raw_task).strip():
        operator_key = "CL_ExecuteTask"
    else:
        raw_task = str(raw_task)
        if "-" in raw_task:
            parts = raw_task.split("-", 1)
            if parts[0].isdigit():
                raw_task = parts[1]
        
        # OperatorList key (and TaskName/task_id/taskid_new): both spaces and
        # hyphens become underscores; preserve original casing.
        raw_task = raw_task.replace("-", "_").replace(" ", "_")

        # Strip known synthetic AppType prefixes only; keep genuine 'BJ_' as-is.
        if raw_task.startswith("NON1C_"):
            raw_task = raw_task[6:]
        elif raw_task.startswith("1C_"):
            raw_task = raw_task[3:]

        operator_key = f"{raw_task}_ExecuteTask"

    # Ensure OperatorList has only the correct operator
    if "OperatorList" not in std_json or not isinstance(std_json["OperatorList"], dict):
        std_json["OperatorList"] = {}

    if operator_key not in std_json["OperatorList"]:
        existing_ops = list(std_json["OperatorList"].items())
        if existing_ops:
            _, op_val = existing_ops[0]
            std_json["OperatorList"] = {operator_key: op_val}
        else:
            std_json["OperatorList"] = {
                operator_key: {
                    "OperatorType": "Scheduled Task",
                    "TaskName": operator_key,
                    "Editable": False,
                    "Properties": {}
                }
            }
    else:
        std_json["OperatorList"] = {operator_key: std_json["OperatorList"][operator_key]}

    # Ensure AppType is preserved from Excel data
    # Always set it from consolidated_json if available
    if "AppType" in consolidated_json:
        original_apptype = consolidated_json["AppType"]
        std_json["AppType"] = original_apptype
        print(f"[LLM] Preserving AppType from Excel: {original_apptype}")
        
        # Also ensure Nonc matches AppType for backward compatibility
        if "Nonc" not in std_json or std_json["Nonc"] == "1C":
            print(f"[LLM] Updating Nonc field to match AppType: {original_apptype}")
            std_json["Nonc"] = original_apptype

    # Ensure CriticalJob is consistent with AppCriticality
    app_criticality = str(std_json.get("AppCriticality", "")).strip().lower()
    if app_criticality == "critical":
        std_json["CriticalJob"] = "Yes"
    elif app_criticality in {"non-critical", "non critical", "noncritical"}:
        std_json["CriticalJob"] = "No"
    
    # Handle GMSA Enabled for 1C format
    # Rule: If GMSA Enabled="yes", task_type='Task Scheduler' and command=EXACT ExecutableLocation value from Excel
    #       If GMSA Enabled="no", task_type='Application Executable' with .exe command
    app_type = str(std_json.get("AppType", std_json.get("Nonc", ""))).strip()
    if app_type == "1C":
        # Extract GMSA Enabled and ExecutableLocation from sheets structure
        gmsa_enabled = ""
        executable_location = ""
        if isinstance(consolidated_json, dict) and "sheets" in consolidated_json:
            sheets = consolidated_json["sheets"]
            if isinstance(sheets, dict):
                batch_jobs = sheets.get("BatchJobInventory", [])
                if isinstance(batch_jobs, list) and len(batch_jobs) > 0:
                    gmsa_enabled = str(batch_jobs[0].get("GMSA Enabled", "")).strip().lower()
                    executable_location = str(
                        batch_jobs[0].get("ExecutableLocation", batch_jobs[0].get("ExecutablePath", ""))
                    ).strip()
        
        if gmsa_enabled == "yes":
            # Task Scheduler: command = exact ExecutableLocation value.
            if "OperatorList" in std_json and isinstance(std_json["OperatorList"], dict):
                for op_key, op_val in std_json["OperatorList"].items():
                    if isinstance(op_val, dict) and "Properties" in op_val:
                        props = op_val["Properties"]
                        props["task_type"] = "Task Scheduler"
                        if executable_location:
                            props["command"] = executable_location
                        current_command = str(props.get("command", "")).strip()
                        print(f"[LLM] GMSA Enabled='yes' for 1C. task_type='Task Scheduler', command='{current_command}' for {op_key}")
        elif gmsa_enabled == "no":
            # Application Executable: command should be .exe format.
            if "OperatorList" in std_json and isinstance(std_json["OperatorList"], dict):
                for op_key, op_val in std_json["OperatorList"].items():
                    if isinstance(op_val, dict) and "Properties" in op_val:
                        props = op_val["Properties"]
                        props["task_type"] = "Application Executable"
                        command = strip_platinum_prefix(str(props.get("command", "")).strip())
                        props["command"] = command
                        if command and not command.lower().endswith(".exe"):
                            print(f"[LLM] WARNING: GMSA Enabled='no' but command does not end with .exe: {command}")
                        print(f"[LLM] GMSA Enabled='no' for 1C. task_type='Application Executable', command='{command}' for {op_key}")

    # Save JSON
    out_dir_path = Path(out_dir)
    ensure_dir(out_dir_path)

    global_app_id = std_json.get("GlobalAppID", appid)
    task_name = std_json.get("TaskName", "CL").replace(" ", "-")

    # Keep genuine 'BJ-'/'BJ_' prefix as-is in filename.

    if task_name.startswith(f"{global_app_id}-"):
        json_filename = f"{task_name}.json"
    else:
        json_filename = f"{global_app_id}-{task_name}.json"

    json_file = out_dir_path / json_filename
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(std_json, f, indent=2, ensure_ascii=False)

    print(f"✅ Standard JSON generated: {json_file}")
    return json_file
