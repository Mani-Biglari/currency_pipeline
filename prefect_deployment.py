from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/Mani-Biglari/currency_pipeline.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="flow.py:currency_pipeline_flow", # Specific flow to run
    ).deploy(
        name="Currency Pipeline Dev",
        work_pool_name="currency_pipeline_pool", # Work pool target
        cron="* * * * *", # Cron schedule (every minute)
    )
