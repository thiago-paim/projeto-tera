from prefect import flow, get_run_logger
from candidates import process_candidates_dataset, scrape_twitter_data, scrape_tweets_count


@flow(name="Full Candidates flow")
def full_candidates_flow():
    logger = get_run_logger()
    logger.info("Full Candidates flow is starting!")
    
    df = process_candidates_dataset()
    df = scrape_twitter_data(df)
    df = scrape_tweets_count(df)
    return df.shape
    
    
if __name__ == "__main__":
    full_candidates_flow()