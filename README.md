Steps to set up locally:

git clone ...

python -m venv venv && source venv/bin/activate

pip install -r requirements.txt

Copy .env.example to .env, set RAPIDAPI_KEY and DATABASE_URL.

Initialize DB: python db.py (the file defines init_db() and will create tables).

Ingest airports: python data_ingest.py (adapt the script to add flights & aircraft ingestion using the correct AeroDataBox flight endpoint).

Run Streamlit: streamlit run streamlit_app.py

Deploying to Streamlit Cloud:

Push the repository to GitHub, include requirements.txt and .streamlit if needed.

On Streamlit Cloud, link the GitHub repo, add environment variables (RAPIDAPI_KEY and DATABASE_URL — for remote DB use Postgres or MySQL), then deploy.

For SQLite on Streamlit Cloud, note filesystem is ephemeral — use a managed DB (Heroku Postgres / Supabase / RDS) for persistent data.

Notes on production:

Use a proper RDBMS (Postgres/MySQL) for scale.

Add API rate-limit handling, exponential backoff and logging.

Add unit tests where possible and handle edge cases in JSON parsing.

Use background workers (e.g., Airflow / cron / Prefect) for ingestion in production. (For the capstone, a manual/one-shot ingestion script is acceptable.)