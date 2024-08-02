def create_pig_job_script(table_name):
    # Create a unique temporary directory for each table
    tmp_dir = f"/tmp/bgpload_{table_name}"

    if table_name.startswith("cur_"):
        config_path = f"gs://{STAGING_CONFIG_BUCKET_BO}/fdpt-mcd/historical_code/curation/{DEPLOY_ENV}/{table_name}_config.properties"
    else:
        config_path = f"gs://{STAGING_CONFIG_BUCKET_BO}/fdpt-mcd/historical_code/consumption/{DEPLOY_ENV}/{table_name}_config.properties"

    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pig_job": {
            "query_list": [
                {
                    "queries": [
                        f"fs -rm -r {tmp_dir};",
                        f"fs -mkdir {tmp_dir};",
                        f"fs -cp -f gs://{STAGING_CONFIG_BUCKET_BO}/fdpt-mcd/historical_code/gcs_bq_loader-v1.sh {tmp_dir}/;",
                        f"fs -cp -f {config_path} {tmp_dir}/;",
                        f"fs -chmod 750 {tmp_dir}/gcs_bq_loader-v1.sh;",
                        f"sh {tmp_dir}/gcs_bq_loader-v1.sh " + ' '.join(get_shell_script_args(table_name))
                    ]
                }
            ]
        },
        "labels": {
            "owner": "finance-mpf",
            "cost_centre": "A15358",
            "cmdpb_appid": "a1b0334",
            "dataclassification": "limited",
            "table_name": table_name
        }
    }
