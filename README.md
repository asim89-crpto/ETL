# My ETL Project
This is my ETL pipeline...


# Sentiment Analysis ETL Pipeline

This project demonstrates a full ETL workflow:
- **Extract**: Data from a .NET Core Sentiment Reviews API
- **Transform**: Normalize sentiment labels (Positive, Negative, Neutral), deduplicate by ApiId
- **Load**: Store curated data into SQL Server (`DS_Portfolio.dbo.Sentiment_Reviews`)
- **Analyze**: Power BI dashboard for sentiment trends and distribution

## Project Structure
- `etl_reviews_pipeline.ipynb` → Notebook with pipeline
- `etl_reviews_pipeline.py` → Executable script
- `sql/` → SQL views (`vw_SentimentDaily`, `vw_SentimentDistribution`)
- `SentimentAnalysisDashboard.pbix` → Power BI dashboard file
- `requirements.txt` → Dependencies

## Power BI Dashboard
KPIs:
- Total Reviews
- Positive / Negative / Neutral %
- Daily trend line
- Sentiment share donut chart

![Dashboard Screenshot](sentimentdashboad.png)

## How to Run
1. Clone repo  
2. Install dependencies: `pip install -r requirements.txt`  
3. Start .NET API: `dotnet run --launch-profile NewApp`  
4. Run ETL script: `python etl_reviews_pipeline.py`  
5. Connect Power BI to SQL Server → `DS_Portfolio` DB
