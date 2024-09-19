# Configure the GCP BigQuery key.
touch bigquery_credentials.json
export BIGQUERY_CREDENTIALS=$(readlink -f bigquery_credentials.json)
echo "Loading ${BIGQUERY_CREDENTIALS} to export analysis summaries to BigQuery/Metabase."
# ie: export BIGQUERY_VAULT_SECRET=v1/ci/kv/gcp/flink_sql_bigquery
vault kv get -field=json "${BIGQUERY_VAULT_SECRET}" > "${BIGQUERY_CREDENTIALS}"
# You may also copy your credential json directly to the bigquery_credentials.json for this to work.
chmod 600 "${BIGQUERY_CREDENTIALS}"