name: Daily ETL Pipeline

on:
  schedule:
    # Runs every day at 6:00 AM UTC (after US market open)
    - cron: '0 6 * * *'
  workflow_dispatch:
    # Also allows manual trigger from GitHub Actions UI anytime

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install requests pandas psycopg2-binary pymongo python-dotenv

      - name: Run ETL pipeline
        env:
          NEWS_API_KEY:       ${{ secrets.NEWS_API_KEY }}
          FRED_API_KEY:       ${{ secrets.FRED_API_KEY }}
          SUPABASE_HOST:      ${{ secrets.SUPABASE_HOST }}
          SUPABASE_PORT:      ${{ secrets.SUPABASE_PORT }}
          SUPABASE_DB:        ${{ secrets.SUPABASE_DB }}
          SUPABASE_USER:      ${{ secrets.SUPABASE_USER }}
          SUPABASE_PASSWORD:  ${{ secrets.SUPABASE_PASSWORD }}
          MONGO_URI:          ${{ secrets.MONGO_URI }}
          ALPACA_API_KEY:     ${{ secrets.ALPACA_API_KEY }}
          ALPACA_SECRET_KEY:  ${{ secrets.ALPACA_SECRET_KEY }}
        run: python pipeline.py

      - name: Upload run log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: pipeline-log-${{ github.run_id }}
          path: pipeline_log.txt
          if-no-files-found: ignore
          retention-days: 7
