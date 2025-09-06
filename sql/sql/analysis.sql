-- Daily sentiment trends
CREATE OR ALTER VIEW dbo.vw_SentimentDaily AS
SELECT CAST([Date] AS date) AS Day, Sentiment, COUNT(*) AS ReviewCount
FROM dbo.Sentiment_Reviews
GROUP BY CAST([Date] AS date), Sentiment;
GO

-- Sentiment distribution
CREATE OR ALTER VIEW dbo.vw_SentimentDistribution AS
SELECT Sentiment, COUNT(*) AS ReviewCount
FROM dbo.Sentiment_Reviews
GROUP BY Sentiment;
GO

-- Useful queries
SELECT Sentiment, COUNT(*) AS Cnt
FROM dbo.Sentiment_Reviews
GROUP BY Sentiment
ORDER BY Cnt DESC;

SELECT TOP 10 * 
FROM dbo.Sentiment_Reviews
ORDER BY [Date] DESC;
