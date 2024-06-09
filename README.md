# Bizzi BI Hubspot-> BigQuery

This respository is a CI pipeline to replicate data from Bizzi Hubspot to BigQuery (Bizzi-home GCP Project), provide an efficient way to write reports and make actions
<h2>ETL Process

![ETL diagram](https://gitlab.com/bizzi-group/bizzi-bi/bizzi-bi-hubspot-bigquery/-/raw/main/stack.jpg?ref_type=heads)

<h2>Credentials

This pipeline requires 3 credentials stored as pipeline variable for Hubspot, Redash and BigQuery

<h3> Hubspot credentials

`#HUBSPOT_APPLICATION_CREDENTIALS:{"app_token": "your Hubspot App Token"}`

**Scope**:
 - `crm.objects.marketing_events.read`
 - `crm.objects.marketing_events.write`
 - `crm.objects.companies.write`
 - `crm.objects.companies.read`
 - `crm.objects.deals.read`
 - `crm.objects.deals.write`
 - `crm.schemas.contacts.read`
 - `crm.objects.contacts.read`
 - `crm.schemas.companies.read`
 - `crm.schemas.companies.write`
 - `crm.schemas.contacts.write`
 - `crm.schemas.deals.read`
 - `crm.schemas.deals.write`
 - `crm.objects.owners.read`
 - `crm.objects.contacts.write`
 - `sales-email-read`

<h3> Redash 

`#REDASH_APPLICATION_CREDENTIALS:{"api_key":"your Redash API Key"}`

**Scope**:
Read permission to those query ids:

 - `1417`
 - `1424`
 - `1633`
 - `1636`
 - `1634`

<h3> BigQuery

`#GOOGLE_APPLICATION_CREDENTIALS:<GOOGLE CLOUD SERVICE ACCOUNT IN JSON FORMAT>`

**Scope**:
 - `bigquery.datasets.get`
 - `bigquery.jobs.create`
 - `bigquery.tables.updateData`
 - `bigquery.tables.update`
 - `bigquery.tables.get`
 - `bigquery.tables.create`

<h2> Commands
<h4> Install necessary module with PyPip

`pip  install  -r  requirements.txt`

<h4> Replicate Hubspot contacts to BigQuery

`python  main.py  hubspot_contacts_bigquery`

<h4> Replicate Hubspot companies to BigQuery

`python  main.py  hubspot_companies_bigquery`
<h4> Replicate Hubspot deals to BigQuery

`python  main.py  hubspot_deals_bigquery`

<h4> Replicate Hubspot association to BigQuery

`python  main.py  hubspot_association_to_bigquery`
<h4> Replicate Hubspot deal log to BigQuery

`python  main.py  hubspot_deal_logs_bigquery`
<h4> Replicate Hubspot engagements to BigQuery

`python  main.py  hubspot_engagement_to_bigquery  --engagement  <engagement type> (engagement in emails, calls, notes, meetings,tasks,communication`

