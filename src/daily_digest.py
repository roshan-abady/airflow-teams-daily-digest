import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.configuration import conf
from airflow.models import Variable
from airflow.models.dagbag import DagBag
from sqlalchemy import or_, and_
from urllib.parse import quote_plus
import os
import re

local_tz = pendulum.timezone("Australia/Melbourne")
# Update this to match your Airflow configuration section name
environment = conf.get('airflow', 'environment', fallback='local')

# Check if we're running locally
is_local = environment == 'local' or os.getenv('AIRFLOW_ENV') == 'local'

default_args = {
    'workflow_id': 'your-workflow-id-here',  # Replace with your workflow ID or remove if not needed
    'email': ["your-email@company.com", "your-teams-channel@au.teams.ms"],  # Update with your email addresses
    'owner': 'operations-analytics',  # Update with your team name
    'start_date': datetime(2025, 11, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def format_airflow_dag_url(airflow_url, dag_id, execution_date, run_id):
    """
    Format an Airflow DAG URL with execution_date and run_id parameters.
    Format: 2025-11-18 19:00:00+00:00 -> 2025-11-18+19%3A00%3A00%2B00%3A00
    Format: 2025-11-18 19:00:00-05:00 -> 2025-11-18+19%3A00%3A00%2D05%3A00
    """
    try:
        if not airflow_url or not execution_date or not run_id:
            return None
        
        # Format execution_date: 2025-11-18 19:00:00+00:00 -> 2025-11-18+19%3A00%3A00%2B00%3A00
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S%z')
        # Step 1: Replace space between date and time with +
        exec_date_str = exec_date_str.replace(' ', '+', 1)
        # Step 2: Replace all colons with %3A
        exec_date_str = exec_date_str.replace(':', '%3A')
        # Step 3: Handle timezone offset sign (+ or -)
        # After steps 1-2, the string has format: YYYY-MM-DD+HH%3AMM%3ASS+HHMM or YYYY-MM-DD+HH%3AMM%3ASS-HHMM
        # We need to encode the timezone sign. The timezone sign is the last + or - in the string
        # (the first + is the space replacement, any subsequent + or - is the timezone)
        # Match timezone pattern at the end: +HHMM, -HHMM, +HH%3AMM, or -HH%3AMM
        # Replace timezone + with %2B
        exec_date_str = re.sub(r'\+(\d{4}|\d{2}%3A\d{2})$', r'%2B\1', exec_date_str)
        # Replace timezone - with %2D (URL encode minus sign)
        exec_date_str = re.sub(r'-(\d{4}|\d{2}%3A\d{2})$', r'%2D\1', exec_date_str)
        
        # Encode run_id: replace : with %3A and + with %2B
        run_id_encoded = run_id.replace(':', '%3A').replace('+', '%2B')
        
        url = f"{airflow_url}/dags/{dag_id}/grid?execution_date={exec_date_str}&tab=graph&dag_run_id={run_id_encoded}"
        return url
    except Exception as e:
        print(f"Error generating URL for {dag_id}: {e}")
        return None


@provide_session
def generate_morning_digest_stats(session=None, **context):
    """
    Query Airflow's database for today's DAG runs and return statistics.
    """
    now_local = pendulum.now(local_tz)
    start_of_day_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Convert to UTC for database comparison (database stores in UTC)
    now_utc = now_local.in_timezone('UTC')
    start_of_day_utc = start_of_day_local.in_timezone('UTC')
    
    # Get Airflow URL for generating links
    try:
        airflow_url = Variable.get('airflow_url')
    except Exception:
        try:
            # Fallback: try to get from context
            airflow_url = context.get('var', {}).get('value', {}).get('airflow_url', None)
        except Exception:
            airflow_url = None
    
    # Hardcode base URL if variable is not set - UPDATE THIS WITH YOUR AIRFLOW URL
    if not airflow_url:
        airflow_url = 'https://your-airflow-instance.com'
    
    print(f"Airflow URL: {airflow_url}")
    
    print(f"\n{'='*60}")
    print(f"Querying Airflow database for DAG runs")
    print(f"From: {start_of_day_local.format('YYYY-MM-DD HH:mm:ss zz')}")
    print(f"To:   {now_local.format('YYYY-MM-DD HH:mm:ss zz')}")
    print(f"(UTC: {start_of_day_utc.format('YYYY-MM-DD HH:mm:ss')} to {now_utc.format('YYYY-MM-DD HH:mm:ss')})")
    print(f"{'='*60}\n")
    
    # Include DAGs that:
    # 1. Started today (between start_of_day and now), OR
    # 2. Are currently running (regardless of when they started, but must have started before now)
    dag_runs = session.query(DagRun).filter(
        or_(
            # DAGs that started today
            and_(
                DagRun.start_date >= start_of_day_utc,
                DagRun.start_date <= now_utc
            ),
            # DAGs that are currently running (started before now)
            and_(
                DagRun.state == State.RUNNING,
                DagRun.start_date <= now_utc
            )
        ),
        ~DagRun.dag_id.like('%test%'),
        ~DagRun.dag_id.like('%example%'),
        DagRun.dag_id != 'daily_digest'
    ).all()
    
    # Filter by owner='operations-analytics' or 'product-analytics' in Python
    # Also include specific DAGs regardless of owner
    # Get DAG bag to access DAG owner information
    dagbag = DagBag()
    included_dag_ids = set()
    
    # Always include specific DAGs - UPDATE THIS LIST WITH YOUR DAG IDs
    included_dag_ids.add('example_important_dag')
    
    for dag_id, dag in dagbag.dags.items():
        # Check if DAG has owner='operations-analytics' or 'product-analytics' in default_args
        dag_owner = None
        if hasattr(dag, 'owner'):
            dag_owner = dag.owner
        elif hasattr(dag, 'default_args') and dag.default_args.get('owner'):
            dag_owner = dag.default_args.get('owner')
        
        # UPDATE THESE OWNER NAMES TO MATCH YOUR TEAM STRUCTURE
        if dag_owner in ['operations-analytics', 'product-analytics']:
            included_dag_ids.add(dag_id)
    
    # Filter dag_runs to only include included DAGs
    dag_runs = [dr for dr in dag_runs if dr.dag_id in included_dag_ids]
    
    print(f"Found {len(dag_runs)} DAG runs today\n")
    
    success = []
    failed = []
    running = []
    
    # Calculate DAG streak (consecutive days without failures)
    streak_days = 0
    current_date = start_of_day_local
    max_streak_check = 365  # Check up to 365 days back
    
    for day_offset in range(max_streak_check):
        check_date_start = current_date.subtract(days=day_offset)
        check_date_end = check_date_start.add(days=1)
        check_date_start_utc = check_date_start.in_timezone('UTC')
        check_date_end_utc = check_date_end.in_timezone('UTC')
        
        # Query for failed DAGs on this day
        failed_runs = session.query(DagRun).filter(
            DagRun.start_date >= check_date_start_utc,
            DagRun.start_date < check_date_end_utc,
            DagRun.state == State.FAILED,
            ~DagRun.dag_id.like('%test%'),
            ~DagRun.dag_id.like('%example%'),
            DagRun.dag_id != 'daily_digest'
        ).all()
        
        # Filter by operations-analytics or product-analytics owner
        # Also include specific DAGs regardless of owner
        dagbag = DagBag()
        included_dag_ids = set()
        
        # Always include specific DAGs
        included_dag_ids.add('example_important_dag')
        
        for dag_id, dag in dagbag.dags.items():
            dag_owner = None
            if hasattr(dag, 'owner'):
                dag_owner = dag.owner
            elif hasattr(dag, 'default_args') and dag.default_args.get('owner'):
                dag_owner = dag.default_args.get('owner')
            
            if dag_owner in ['operations-analytics', 'product-analytics']:
                included_dag_ids.add(dag_id)
        
        failed_runs_filtered = [dr for dr in failed_runs if dr.dag_id in included_dag_ids]
        
        # Also check task-level DAGs (DAGs with multiple projects in loops)
        # UPDATE THIS LIST WITH YOUR TASK-LEVEL DAG IDs
        task_level_dags = [
            'common_schedules',
            'example_important_dag',
            'product_analytics_transform',
            'product_analytics_transform_weekly',
            'example_inventory_management'
        ]
        failed_tasks = session.query(TaskInstance).join(
            DagRun, 
            and_(
                TaskInstance.dag_id == DagRun.dag_id,
                TaskInstance.run_id == DagRun.run_id
            )
        ).filter(
            DagRun.start_date >= check_date_start_utc,
            DagRun.start_date < check_date_end_utc,
            DagRun.dag_id.in_(task_level_dags),
            TaskInstance.state == State.FAILED,
            TaskInstance.task_id.like('run_dbt_task_for_%')
        ).all()
        
        # If we found failures on this day, break the streak
        if failed_runs_filtered or failed_tasks:
            break
        
        streak_days += 1
    
    # Separate task-level DAGs (DAGs with multiple projects in loops) from other DAGs
    task_level_dag_ids = [
        'common_schedules',
        'example_important_dag',
        'product_analytics_transform',
        'product_analytics_transform_weekly',
        'example_inventory_management'
    ]
    task_level_runs = [dr for dr in dag_runs if dr.dag_id in task_level_dag_ids]
    other_dag_runs = [dr for dr in dag_runs if dr.dag_id not in task_level_dag_ids]
    
    # Process other DAGs at DAG level
    for dag_run in other_dag_runs:
        dag_url = format_airflow_dag_url(
            airflow_url, 
            dag_run.dag_id, 
            dag_run.execution_date, 
            dag_run.run_id
        ) if airflow_url and dag_run.execution_date and dag_run.run_id else None
        
        # Fallback: if URL generation failed but we have airflow_url, create basic URL
        if not dag_url and airflow_url:
            dag_url = f"{airflow_url}/dags/{dag_run.dag_id}/grid"
        
        print(f"DAG: {dag_run.dag_id}, airflow_url: {airflow_url}, execution_date: {dag_run.execution_date}, run_id: {dag_run.run_id}, dag_url: {dag_url}")
        
        # Convert UTC datetime to local timezone for display
        start_time_local = None
        if dag_run.start_date:
            # Airflow stores datetimes in UTC - convert to local timezone
            # Convert to pendulum datetime, ensuring UTC timezone
            if dag_run.start_date.tzinfo is None:
                # Naive datetime - create pendulum datetime in UTC
                dt_utc = pendulum.datetime(
                    dag_run.start_date.year,
                    dag_run.start_date.month,
                    dag_run.start_date.day,
                    dag_run.start_date.hour,
                    dag_run.start_date.minute,
                    dag_run.start_date.second,
                    dag_run.start_date.microsecond,
                    tz=pendulum.timezone('UTC')
                )
            else:
                # Timezone-aware datetime - convert to UTC then to local
                dt_utc = pendulum.instance(dag_run.start_date).in_timezone('UTC')
            # Convert to local timezone
            start_time_local = dt_utc.in_timezone(local_tz)
        
        dag_info = {
            'dag_id': dag_run.dag_id,
            'actual_dag_id': dag_run.dag_id,  # Store actual DAG ID for URL
            'start_time': start_time_local.strftime('%I:%M %p') if start_time_local else 'N/A',
            'start_date': dag_run.start_date,  # Store actual datetime for sorting
            'run_id': dag_run.run_id,
            'execution_date': dag_run.execution_date,
            'dag_url': dag_url,
            'airflow_url': airflow_url  # Store airflow_url for fallback in template
        }
        
        if dag_run.state == State.SUCCESS:
            success.append(dag_info)
        elif dag_run.state == State.FAILED:
            failed.append(dag_info)
        elif dag_run.state == State.RUNNING:
            running.append(dag_info)
    
    # Process task-level DAGs at task level (individual projects)
    for dag_run in task_level_runs:
        # Query tasks for this DAG run
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.task_id.like('run_dbt_task_for_%')
        ).all()
        
        for task_instance in task_instances:
            # Extract project name from task_id (remove "run_dbt_task_for_" prefix)
            project_name = task_instance.task_id.replace('run_dbt_task_for_', '')
            display_name = f"{dag_run.dag_id} - {project_name}"
            
            dag_url = format_airflow_dag_url(
                airflow_url,
                dag_run.dag_id,
                dag_run.execution_date,
                dag_run.run_id
            ) if airflow_url and dag_run.execution_date and dag_run.run_id else None
            
            # Fallback: if URL generation failed but we have airflow_url, create basic URL
            if not dag_url and airflow_url:
                dag_url = f"{airflow_url}/dags/{dag_run.dag_id}/grid"
            
            # Convert UTC datetime to local timezone for display
            task_start_time_local = None
            if task_instance.start_date:
                # Airflow stores datetimes in UTC - convert to local timezone
                # Convert to pendulum datetime, ensuring UTC timezone
                if task_instance.start_date.tzinfo is None:
                    # Naive datetime - create pendulum datetime in UTC
                    dt_utc = pendulum.datetime(
                        task_instance.start_date.year,
                        task_instance.start_date.month,
                        task_instance.start_date.day,
                        task_instance.start_date.hour,
                        task_instance.start_date.minute,
                        task_instance.start_date.second,
                        task_instance.start_date.microsecond,
                        tz=pendulum.timezone('UTC')
                    )
                else:
                    # Timezone-aware datetime - convert to UTC then to local
                    dt_utc = pendulum.instance(task_instance.start_date).in_timezone('UTC')
                # Convert to local timezone
                task_start_time_local = dt_utc.in_timezone(local_tz)
            
            task_info = {
                'dag_id': display_name,
                'actual_dag_id': dag_run.dag_id,  # Store actual DAG ID for URL
                'start_time': task_start_time_local.strftime('%I:%M %p') if task_start_time_local else 'N/A',
                'start_date': task_instance.start_date or dag_run.start_date,  # Use task start or DAG start
                'run_id': dag_run.run_id,
                'execution_date': dag_run.execution_date,
                'dag_url': dag_url,
                'airflow_url': airflow_url  # Store airflow_url for fallback in template
            }
            
            if task_instance.state == State.SUCCESS:
                success.append(task_info)
            elif task_instance.state == State.FAILED:
                failed.append(task_info)
            elif task_instance.state == State.RUNNING:
                running.append(task_info)
    
    # Sort lists by start_date (ascending), then by dag_id (ascending)
    # Use a very early datetime for None values to push them to the end
    min_datetime = pendulum.datetime(1900, 1, 1)
    success.sort(key=lambda x: (x['start_date'] if x['start_date'] else min_datetime, x['dag_id']))
    failed.sort(key=lambda x: (x['start_date'] if x['start_date'] else min_datetime, x['dag_id']))
    running.sort(key=lambda x: (x['start_date'] if x['start_date'] else min_datetime, x['dag_id']))
    
    if failed:
        status_emoji = "⚠️"
    elif running:
        status_emoji = "🔄"
    else:
        status_emoji = "✅"
    
    stats = {
        'status_emoji': status_emoji,
        'success_count': len(success),
        'failed_count': len(failed),
        'running_count': len(running),
        'streak_days': streak_days,
        'success_dags': success,
        'failed_dags': failed,
        'running_dags': running,
        'timestamp': now_local.strftime('%d/%m/%Y %I:%M %p')
    }
    
    # Print summary for local testing
    print(f"{'='*60}")
    print(f"DIGEST SUMMARY")
    print(f"{'='*60}")
    print(f"Status: {status_emoji}")
    print(f"✅ Successful: {len(success)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"🔄 Running: {len(running)}")
    print(f"🔥 Streak: {streak_days} days")
    
    if failed:
        print(f"\nFailed DAGs:")
        for dag in failed:
            print(f"  • {dag['dag_id']} - Started {dag['start_time']}")
    
    if running:
        print(f"\nRunning DAGs:")
        for dag in running:
            print(f"  • {dag['dag_id']} - Started {dag['start_time']}")
    
    print(f"{'='*60}\n")
    
    return stats


def print_email_preview(**context):
    """For local testing - print what the email would look like"""
    stats = context['ti'].xcom_pull(task_ids='generate_morning_stats')
    
    print("\n" + "="*60)
    print("EMAIL PREVIEW (would be sent to Teams)")
    print("="*60)
    print(f"Subject: {stats['status_emoji']} Airflow Daily Digest - {stats['timestamp'].split()[0]}")
    print("\nBody:")
    print(f"{stats['status_emoji']} Morning Airflow Status")
    print(f"As of: {stats['timestamp']}")
    print(f"\n✅ Succeeded: {stats['success_count']}")
    print(f"❌ Failed: {stats['failed_count']}")
    print(f"🔄 Running: {stats['running_count']}")
    print(f"🔥 Streak: {stats['streak_days']} days")
    
    if stats['success_dags']:
        print(f"\n✅ Successful DAGs:")
        for dag in stats['success_dags'][:10]:  # Show first 10
            print(f"  • {dag['dag_id']} - Started {dag['start_time']}")
    
    if stats['failed_dags']:
        print(f"\n⚠️ Failed DAGs:")
        for dag in stats['failed_dags']:
            print(f"  • {dag['dag_id']} - Started {dag['start_time']}")
    
    if stats['running_dags']:
        print(f"\n🔄 Still Running:")
        for dag in stats['running_dags']:
            print(f"  • {dag['dag_id']} - Started {dag['start_time']}")
    
    print("="*60 + "\n")


with DAG(
    'daily_digest',
    default_args=default_args,
    schedule_interval='0 10 * * *' if not is_local else None,  # Disable schedule locally
    catchup=False,
    tags=['monitoring', 'operations-analytics', 'digest']
) as dag:
    
    generate_stats = PythonOperator(
        task_id='generate_morning_stats',
        python_callable=generate_morning_digest_stats,
        provide_context=True
    )
    
    if is_local:
        # Local testing - just print what would be sent
        preview_email = PythonOperator(
            task_id='preview_email_locally',
            python_callable=print_email_preview,
            provide_context=True
        )
        
        generate_stats >> preview_email
    
    else:
        # Production - send formatted email to Teams
        smtp_host = conf.get('smtp', 'smtp_host', fallback=None)
        if environment == 'prod' and smtp_host and smtp_host != 'localhost':
            
            send_digest = EmailOperator(
                task_id='send_digest_to_teams',
                to=['your-teams-channel@au.teams.ms'],  # UPDATE WITH YOUR TEAMS CHANNEL EMAIL
                subject='{{ ti.xcom_pull(task_ids="generate_morning_stats")["status_emoji"] }} Airflow Digest {{ ti.xcom_pull(task_ids="generate_morning_stats")["timestamp"].split()[0] }}',
                html_content="""
                {% set stats = ti.xcom_pull(task_ids="generate_morning_stats") %}
                <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background: white; padding: 20px; border: 1px solid #e1e8ed; border-radius: 8px;">
                        <div style="color: #1f2937; margin-bottom: 20px;">
                            Airflow status as of: {{ stats["timestamp"] }}
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px;">
                            <div style="background: #f0f9ff; padding: 12px; border-radius: 6px; border-left: 4px solid #10b981;">
                                <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">✅ Succeeded</div>
                                <div style="font-size: 24px; font-weight: bold; color: #10b981;">{{ stats["success_count"] }}</div>
                            </div>
                            <div style="background: #fef2f2; padding: 12px; border-radius: 6px; border-left: 4px solid #ef4444;">
                                <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">❌ Failed</div>
                                <div style="font-size: 24px; font-weight: bold; color: #ef4444;">{{ stats["failed_count"] }}</div>
                            </div>
                            <div style="background: #fffbeb; padding: 12px; border-radius: 6px; border-left: 4px solid #f59e0b;">
                                <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">🔄 Running</div>
                                <div style="font-size: 24px; font-weight: bold; color: #f59e0b;">{{ stats["running_count"] }}</div>
                            </div>
                            <div style="background: #fef2f2; padding: 12px; border-radius: 6px; border-left: 4px solid #f97316;">
                                <div style="font-size: 11px; color: #6b7280; margin-bottom: 4px;">🔥 Streak</div>
                                <div style="font-size: 24px; font-weight: bold; color: #f97316;">{{ stats["streak_days"] }}d</div>
                            </div>
                        </div>
                        
                        <div style="color: #6b7280; font-size: 11px; font-style: italic; margin-bottom: 20px; text-align: center;">
                            💡 Click on a DAG name to open in Airflow
                        </div>
                        
                        {% set failed_dags = stats["failed_dags"] %}
                        {% if failed_dags|length > 0 %}
                        <div style="margin-top: 20px; padding-top: 15px; border-top: 2px solid #e5e7eb;">
                            <div style="color: #ef4444; margin: 0 0 10px 0; font-size: 16px; font-weight: 600;">
                                ⚠️ Failed DAGs: ({{ failed_dags|length }})
                            </div>
                            <ul style="margin: 0; padding-left: 20px; list-style: none;">
                                {% for dag in failed_dags[:15] %}
                                <li style="padding: 4px 0; border-bottom: 1px solid #f3f4f6;">
                                    {% if dag.dag_url %}
                                    <a href="{{ dag.dag_url }}" style="color: #ef4444; text-decoration: none; font-weight: 600; font-size: 14px;">
                                        <strong>{{ dag.dag_id }}</strong>
                                    </a>
                                    {% else %}
                                    <strong style="color: #ef4444; font-size: 14px;">{{ dag.dag_id }}</strong>
                                    {% endif %}
                                    <span style="color: #6b7280; font-size: 12px; margin-left: 8px;">- Started {{ dag.start_time }}</span>
                                </li>
                                {% endfor %}
                            </ul>
                            {% if failed_dags|length > 15 %}
                            <div style="color: #6b7280; font-size: 12px; font-style: italic; margin-top: 8px; padding-left: 20px;">
                                Showing first 15 of {{ failed_dags|length }} failed DAGs
                            </div>
                            {% endif %}
                        </div>
                        {% endif %}
                        
                        {% set running_dags = stats["running_dags"] %}
                        {% if running_dags|length > 0 %}
                        <div style="margin-top: 20px; padding-top: 15px; border-top: 2px solid #e5e7eb;">
                            <div style="color: #f59e0b; margin: 0 0 10px 0; font-size: 16px; font-weight: 600;">
                                🔄 Still Running: ({{ running_dags|length }})
                            </div>
                            <ul style="margin: 0; padding-left: 20px; list-style: none;">
                                {% for dag in running_dags[:10] %}
                                <li style="padding: 4px 0; border-bottom: 1px solid #f3f4f6;">
                                    {% if dag.dag_url %}
                                    <a href="{{ dag.dag_url }}" style="color: #f59e0b; text-decoration: none; font-weight: 600; font-size: 14px;">
                                        <strong>{{ dag.dag_id }}</strong>
                                    </a>
                                    {% else %}
                                    <strong style="color: #1f2937; font-size: 14px;">{{ dag.dag_id }}</strong>
                                    {% endif %}
                                    <span style="color: #6b7280; font-size: 12px; margin-left: 8px;">- Started {{ dag.start_time }}</span>
                                </li>
                                {% endfor %}
                            </ul>
                            {% if running_dags|length > 10 %}
                            <div style="color: #6b7280; font-size: 12px; font-style: italic; margin-top: 8px; padding-left: 20px;">
                                Showing first 10 of {{ running_dags|length }} running DAGs
                            </div>
                            {% endif %}
                        </div>
                        {% endif %}
                        
                        {% if var.value.get("airflow_url", None) %}
                        <div style="margin-top: 25px; padding-top: 20px; border-top: 2px solid #e5e7eb; text-align: center;">
                            <a href="{{ var.value.airflow_url }}/home" style="display: inline-block; background: #667eea; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: 600;">
                                View Airflow Dashboard
                            </a>
                        </div>
                        {% endif %}
                    </div>
                </div>
                """
            )
            
            generate_stats >> send_digest
