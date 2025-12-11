# Airflow Daily Digest in Teams

An automated monitoring system that delivers daily Airflow pipeline health reports directly to Microsoft Teams via Power Automate Adaptive Cards.

## 🎯 Overview

This project automates the delivery of daily Airflow DAG run statistics to a Microsoft Teams channel, transforming manual monitoring into a single, beautifully formatted card that appears every morning. The system tracks:

- ✅ Successful DAG runs
- ❌ Failed DAG runs  
- 🔄 Currently running DAGs
- 🔥 Consecutive days without failures (streak)

## 🏗️ Architecture

```
Airflow DAG → Email (HTML) → Power Automate → Teams Adaptive Card
```

1. **Airflow DAG** (`daily_digest.py`): Queries Airflow's metadata database for DAG run statistics
2. **Email Operator**: Sends formatted HTML email to Teams channel email address
3. **Power Automate Flow**: Receives email, extracts content, and formats as Adaptive Card
4. **Microsoft Teams**: Displays the card in the channel

## 🤔 Why Email Instead of Webhooks?

**Design Decision**: This solution uses email as the integration mechanism rather than direct Airflow webhooks or API calls to Teams.

**Rationale**:
- **Access Constraints**: Direct webhook access to Airflow's infrastructure requires platform team approval and elevated permissions that were not available
- **Security Concerns**: The platform team had security concerns about exposing webhook endpoints or granting outbound API access from Airflow to external services
- **Existing Infrastructure**: Email infrastructure was already configured and approved for use, requiring no additional security reviews
- **Power Automate Capabilities**: Power Automate's email trigger provides a reliable, no-code integration point that doesn't require API credentials or webhook management

This email-based approach provides a pragmatic solution that works within existing security constraints while still delivering the desired functionality.

## 📁 Project Structure

```
.
├── README.md
├── src/
│   └── daily_digest.py          # Airflow DAG implementation
├── power-automate/
│   └── adaptive_card.json       # Power Automate Adaptive Card template
└── docs/
    └── ARCHITECTURE.md          # Detailed architecture documentation
```

## 🚀 Features

### Airflow DAG Features

- **Intelligent DAG Filtering**: Automatically includes DAGs owned by `operations-analytics` or `product-analytics`
- **Task-Level Monitoring**: Handles DAGs with multiple projects, tracking individual task status
- **Timezone Handling**: Converts UTC timestamps to local timezone (Australia/Melbourne) for display
- **URL Generation**: Creates clickable links directly to Airflow DAG runs
- **Streak Calculation**: Tracks consecutive days without failures (up to 365 days)
- **Local Development Mode**: Disables scheduling and email sending when running locally

### Power Automate Features

- **Adaptive Card Formatting**: Beautiful, responsive card layout
- **Dynamic Status Styling**: Card header changes colour based on pipeline health
- **Failed DAG Listing**: Displays up to 6 failed DAGs with clickable links
- **Real-time Timestamp**: Shows current time in Australian Eastern Standard Time
- **Direct Airflow Link**: Quick access button to Airflow dashboard

## 📋 Prerequisites

### Airflow Requirements

- Apache Airflow 2.x
- Access to Airflow metadata database
- SMTP server configured in Airflow
- Python packages:
  - `pendulum` (for timezone handling)
  - `sqlalchemy` (for database queries)

### Power Automate Requirements

- Microsoft Power Automate license
- Teams channel email address
- Email trigger configured in Power Automate

## 🔧 Setup

### 1. Airflow Configuration

1. Copy `src/daily_digest.py` to your Airflow `dags/` directory
2. Set Airflow Variables:
   ```bash
   airflow variables set airflow_url "https://your-airflow-instance.com"
   ```
3. Configure SMTP settings in `airflow.cfg` or via connections
4. Update the Teams channel email address in the DAG (line 25):
   ```python
   'email': ["your-teams-channel@au.teams.ms"]
   ```

### 2. Power Automate Setup

1. Create a new Power Automate flow with an "When a new email arrives" trigger
2. Configure the trigger to monitor the Teams channel email inbox
3. Add an "HTML to text" action to extract email content
4. Add an "Post adaptive card in a chat or channel" action
5. Import the Adaptive Card JSON from `power-automate/adaptive_card.json`
6. Map the email content to the Adaptive Card template expressions

### 3. Schedule Configuration

The DAG is scheduled to run daily at 10:00 AM (Australia/Melbourne timezone):

```python
schedule_interval='0 10 * * *'  # Daily at 10 AM
```

## 📊 What Gets Monitored

The system monitors:

- All DAGs with `owner='operations-analytics'` or `owner='product-analytics'`
- Specific DAGs regardless of owner (e.g., `fs_getting_paid`)
- Task-level status for multi-project DAGs:
  - `common_schedules`
  - `fs_getting_paid`
  - `product_analytics_transform`
  - `product_analytics_transform_weekly`
  - `sme_inventory_management_sf`

## 🎨 Adaptive Card Features

The Adaptive Card includes:

- **Header Section**: Airflow logo and timestamp
- **Statistics Grid**: Four-column layout showing success, failed, running, and streak counts
- **Status-Based Styling**: Header colour changes based on pipeline health
- **Failed DAG List**: Shows failed DAGs with direct links to Airflow
- **Quick Actions**: Button to view Airflow dashboard

## 🔍 Local Development

When running locally, the DAG:

- Disables scheduling (`schedule_interval=None`)
- Prints email preview instead of sending
- Allows testing without SMTP configuration

Set environment variable:
```bash
export AIRFLOW_ENV=local
```

## 📝 Customization

### Adding More DAGs to Monitor

Edit the DAG filtering logic in `generate_morning_digest_stats()`:

```python
# Always include specific DAGs
included_dag_ids.add('your_dag_id')
```

### Changing Schedule

Modify the `schedule_interval` in the DAG definition:

```python
schedule_interval='0 10 * * *'  # Cron expression
```

### Customizing Email Template

The HTML email template is embedded in the `EmailOperator`. Modify the `html_content` parameter to change styling or content.

## 🐛 Troubleshooting

### Email Not Sending

- Verify SMTP configuration in Airflow
- Check Teams channel email address is correct
- Ensure environment is set to 'prod' (not 'local')

### Adaptive Card Not Displaying

- Verify Power Automate flow is active
- Check email trigger is configured correctly
- Validate Adaptive Card JSON syntax
- Ensure Teams channel has Power Automate permissions

### Incorrect Timezone Display

- Verify `local_tz` is set correctly (line 17)
- Check Power Automate timezone conversion expression

## 📈 Impact

This automation:

- **Eliminates manual monitoring**: Team no longer needs to check Airflow dashboard daily
- **Improves visibility**: Pipeline health is immediately visible in Teams
- **Reduces response time**: Failed DAGs are identified instantly
- **Builds team culture**: Streak counter gamifies reliability

## 🤝 Contributing

This is a personal project documenting a production automation. Feel free to fork and adapt for your own use!

## 📄 License

This project is provided as-is for reference and educational purposes.

## 🙏 Acknowledgments

Built for the Operations Analytics team at MYOB to improve daily workflow visibility and team collaboration.

---

**Author**: Roshan Abady  
**Created**: 2025  
**Tech Stack**: Apache Airflow, Python, Power Automate, Microsoft Teams, Adaptive Cards
