# Architecture Documentation - Email-Based Integration

## System Overview

The Airflow Teams Daily Digest is a multi-component system that automates the delivery of pipeline health reports from Apache Airflow to Microsoft Teams. **This solution intentionally uses email integration instead of webhooks** due to platform team security constraints and access limitations.

## Design Decisions

### Why Email Instead of Webhooks?

**Initial Consideration**: The ideal architecture would use Airflow webhooks or direct API calls to Microsoft Teams for real-time integration.

**Constraints Encountered**:
1. **Access Limitations**: Direct webhook access to Airflow's infrastructure requires platform team approval and elevated permissions that were not available to the analytics team
2. **Security Concerns**: The platform team raised security concerns about:
   - Exposing webhook endpoints that could be accessed externally
   - Granting outbound API access from Airflow to external services (Teams)
   - Managing additional authentication credentials and secrets
   - Potential network security policy violations

**Chosen Solution**: Email-based integration using existing SMTP infrastructure.

**Benefits of Email Approach**:
- ✅ No additional security reviews required (email already approved)
- ✅ No webhook endpoint exposure
- ✅ No API credentials to manage
- ✅ Leverages existing, approved infrastructure
- ✅ Power Automate email triggers provide reliable, no-code integration
- ✅ Works within existing security policies

**Trade-offs**:
- ⚠️ Slight delay (email delivery vs direct API call)
- ⚠️ Additional parsing step required (HTML to text extraction)
- ⚠️ Less real-time than webhook approach

This pragmatic solution delivers the desired functionality while working within organizational security constraints.

## Component Flow

```
┌─────────────────┐
│  Airflow DAG   │
│  (daily_digest) │
└────────┬────────┘
         │
         │ 1. Queries metadata DB
         │ 2. Generates statistics
         │ 3. Formats HTML email
         ▼
┌─────────────────┐
│  Email Operator │
│  (SMTP)         │
└────────┬────────┘
         │
         │ Sends HTML email
         ▼
┌─────────────────┐
│ Teams Channel   │
│ Email Address    │
└────────┬────────┘
         │
         │ Triggers on new email
         ▼
┌─────────────────┐
│ Power Automate  │
│ Flow            │
└────────┬────────┘
         │
         │ 1. Extracts HTML content
         │ 2. Parses statistics
         │ 3. Formats Adaptive Card
         ▼
┌─────────────────┐
│ Microsoft Teams │
│ Adaptive Card   │
└─────────────────┘
```

## Component Details

### 1. Airflow DAG (`daily_digest.py`)

**Purpose**: Collects DAG run statistics and generates formatted email content.

**Key Functions**:

- `generate_morning_digest_stats()`: Main function that:
  - Queries Airflow metadata database for DAG runs
  - Filters DAGs by owner (configurable, e.g., `operations-analytics`, `product-analytics`)
  - Handles task-level monitoring for multi-project DAGs
  - Calculates streak (consecutive days without failures)
  - Generates clickable URLs to Airflow DAG runs
  - Converts UTC timestamps to local timezone

- `format_airflow_dag_url()`: URL encoding utility for Airflow DAG links

**Data Flow**:
1. Query `dag_run` and `task_instance` tables
2. Filter by date range (today) and owner
3. Categorize into success/failed/running
4. Calculate streak by checking historical failures
5. Format data structure for email template

### 2. Email Operator

**Purpose**: Sends formatted HTML email to Teams channel.

**Configuration**:
- Recipient: Teams channel email address (configurable)
- Subject: Dynamic based on status emoji and date
- Content: Jinja2 template with embedded HTML/CSS

**Email Format**:
- Header with timestamp
- Four-column statistics grid (Success, Failed, Running, Streak)
- Failed DAG list with clickable links
- Running DAG list
- Button to Airflow dashboard

### 3. Power Automate Flow

**Purpose**: Receives email and converts to Adaptive Card.

**Flow Steps**:

1. **Trigger**: "When a new email arrives"
   - Monitors Teams channel inbox
   - Filters by sender/subject if needed

2. **Action**: "HTML to text"
   - Extracts plain text from HTML email
   - Removes HTML tags while preserving structure

3. **Action**: "Post adaptive card in a chat or channel"
   - Uses Adaptive Card JSON template
   - Maps email content to card fields using Power Automate expressions
   - Posts to specified Teams channel

**Power Automate Expressions**:

The Adaptive Card uses complex expressions to parse the email text:

```power-automate
# Extract failed count
split(split(replace(body('Html_to_text'), decodeUriComponent('%0A'), ' '), '❌ Failed ')[1], ' 🔄')[0]

# Extract failed DAG names
split(split(split(body('Html_to_text'), '⚠️ Failed DAGs:')[1], 'Still Running')[0], '* ')[1]
```

### 4. Adaptive Card

**Purpose**: Displays formatted information in Teams.

**Structure**:

1. **Header Container**:
   - Airflow logo
   - Title and timestamp
   - Dynamic styling (green for success, red for failures)

2. **Statistics Grid**:
   - Four columns: Succeeded, Failed, Running, Streak
   - Large numbers with colour coding

3. **Status Section**:
   - Shows "All DAGs Succeeded" or "Failed DAGs: X"
   - Lists up to 6 failed DAGs

4. **Actions**:
   - Button to open Airflow dashboard (URL configurable)

## Data Transformation

### Airflow → Email

```
DagRun/TaskInstance (SQLAlchemy objects)
    ↓
Python dictionaries
    ↓
Jinja2 template rendering
    ↓
HTML email string
```

### Email → Adaptive Card

```
HTML email
    ↓
HTML to text extraction
    ↓
Power Automate expression parsing
    ↓
Adaptive Card JSON
    ↓
Teams rendering
```

## Timezone Handling

- **Airflow**: Stores all timestamps in UTC
- **Display**: Converts to Australia/Melbourne (AEST/AEDT) - configurable
- **Power Automate**: Uses `convertFromUtc()` function for display

## Error Handling

### Airflow DAG

- URL generation failures: Falls back to basic DAG grid URL
- Missing Airflow URL variable: Uses hardcoded default (configurable)
- Local development: Disables email sending, prints preview

### Power Automate

- Missing email content: Expressions return default values ('0', empty strings)
- Parsing failures: Conditional rendering with `$when` clauses

## Scalability Considerations

- **DAG Filtering**: Only monitors specific owners to limit query scope
- **Email Size**: Limits displayed DAGs (15 failed, 10 running)
- **Streak Calculation**: Capped at 365 days to prevent excessive queries
- **Task-Level Monitoring**: Handles multi-project DAGs efficiently

## Security

- **Email Authentication**: SMTP credentials stored in Airflow connections
- **Teams Integration**: Uses Microsoft OAuth for Power Automate
- **URL Generation**: Properly encodes parameters to prevent injection
- **No Webhook Exposure**: Email approach avoids exposing endpoints
- **No External API Access**: Uses approved email infrastructure only

## Monitoring

The DAG itself is excluded from monitoring to prevent recursive notifications.

## Future Enhancements

Potential improvements:

1. **Direct API Integration**: Replace email with Teams webhook (if security constraints are resolved)
2. **Historical Trends**: Add charts showing success rates over time
3. **Alerting**: Separate critical failure notifications
4. **Customization**: Allow teams to configure which DAGs to monitor
5. **Slack Integration**: Alternative to Teams
