// BigQuery on Lakehouse runtime catalog tables (Iceberg REST). Imported lazily
// so it stays out of the first-load bundle until someone switches the BigQuery
// pill to Lakehouse. It carries the same platform id ("google-bigquery") as the
// managed-tables file, so filters survive the toggle.
import bigqueryLakehouse from "../platforms/gcp/lakehouse/bigquery/bigquery.json";

export { bigqueryLakehouse };
