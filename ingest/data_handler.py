import yfinance as yf
import boto3
from datetime import datetime, timedelta
from io import BytesIO

BUCKET="tech-challenge-ingestion"
FOLDER="raw"
TICKER="^BVSP"

def get_ticker_df(ticker):
	yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
	df = yf.download('^BVSP', period= "1d", interval = '1d', start=yesterday).reset_index()
	if df.empty:
		raise Exception(f"the ticker {ticker} returned empty from yfinance")
	df.reset_index(inplace=True)
	df.columns = df.columns.droplevel(1)
	df["Date"] = df["Date"].astype("datetime64[ms]")
	return df

def handler(event, context):
	df = get_ticker_df(TICKER)
	current_date = df.loc[0, 'Date'].strftime('%Y-%m-%d')
	s3_key = f"{FOLDER}/{TICKER}/dt={current_date}/data.parquet"

	buffer = BytesIO()
	df.to_parquet(
		buffer, 
		engine="pyarrow", 
		coerce_timestamps="ms", 
		allow_truncated_timestamps=True)
	buffer.seek(0)

	s3 = boto3.client("s3", region_name="us-east-1")
	s3.put_object(
		Bucket = BUCKET, 
		Key = s3_key, 
		Body = buffer.getvalue(),
		ContentType = "application/octet-stream")

	return { 
		"status" : "ok", 
		 "bucket": BUCKET, 
		 "key": s3_key, 
		 "ticker": TICKER, 
		 "dt": current_date, 
		 "rows": int(len(df))}
