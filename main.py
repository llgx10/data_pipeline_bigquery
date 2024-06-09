import argparse
from argparse import ArgumentParser, ArgumentError
from pipeline.hubspot_2_bigquery_migration.companies_pipeline import Hubspot_companies_to_BigQuery
from pipeline.hubspot_2_bigquery_migration.contacts_pipeline import Hubspot_contacts_to_BigQuery
from pipeline.hubspot_2_bigquery_migration.deals_pipeline import Hubspot_deals_to_BigQuery
from pipeline.hubspot_deal_logs.hubspot_deal_log_pipeline import Hubspot_deal_log_to_BigQuery
from pipeline.hubspot_engagement.hubspot_engagement_pipeline import Hubspot_engagement_to_BigQuery
from pipeline.email_read_log.email_read_log import mautic_email_log_to_bigquery
from pipeline.mautic_hubspot_email_log.mautic_hubspot_email_read_activities import Log_Mautic_email_read_to_Hubspot
from pipeline.hubspot_association_bigquery.hubspot_association_bigquery import Hubspot_Association_to_BigQuery


def parse_args():
    parser = ArgumentParser(description="Run the workflow script.")
    subparsers = parser.add_subparsers(dest="workflow", help="Available workflows")
    workflow_parser = subparsers.add_parser("hubspot_companies_bigquery", help="Run the Hubspot companies to BigQuery workflow")
    workflow_parser = subparsers.add_parser("hubspot_contacts_bigquery", help="Run the Hubspot contacts to BigQuery workflow")
    workflow_parser = subparsers.add_parser("hubspot_deals_bigquery", help="Run the Hubspot deals to BigQuery workflow")
    workflow_parser = subparsers.add_parser("hubspot_deal_logs_bigquery", help="Run the Hubspot deal logs to BigQuery workflow")
    workflow_parser = subparsers.add_parser("hubspot_engagement_to_bigquery", help="Run the Hubspot engagement to BigQuery workflow")
    workflow_parser.add_argument(
        "--engagement", required=True, help="engagement type"
    )

    workflow_parser = subparsers.add_parser("mautic_email_log_to_bigquery", help="Run the Mautic email log to BigQuery workflow")
    workflow_parser = subparsers.add_parser("mautic_hubspot_email_read_activities", help="Run the Mautic email read log to Hubspot workflow")
    workflow_parser = subparsers.add_parser("hubspot_association_to_bigquery", help="Run the Hubspot association to BigQuery workflow")

    return parser.parse_args()

    
def main():
    args = parse_args()
    if args.workflow == "hubspot_companies_bigquery":
        Hubspot_companies_to_BigQuery()
        
    elif args.workflow == "hubspot_contacts_bigquery":
        Hubspot_contacts_to_BigQuery()
    elif args.workflow == "hubspot_deals_bigquery":
        Hubspot_deals_to_BigQuery()
    elif args.workflow == "hubspot_deal_logs_bigquery":
        Hubspot_deal_log_to_BigQuery()   
    elif args.workflow == "hubspot_engagement_to_bigquery":
        engagement=args.engagement
        Hubspot_engagement_to_BigQuery(engagement)
    elif args.workflow == "mautic_email_log_to_bigquery":
        mautic_email_log_to_bigquery() 
    
    elif args.workflow == "mautic_hubspot_email_read_activities":
        Log_Mautic_email_read_to_Hubspot()  
    elif args.workflow == "hubspot_association_to_bigquery":
        Hubspot_Association_to_BigQuery()
    else:
        print(f"Unknown workflow: {args.workflow}")


if __name__ == "__main__":
    main()