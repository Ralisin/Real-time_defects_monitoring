import argparse
import logging
import requests
import umsgpack
import numpy as np
from sklearn.cluster import DBSCAN
from PIL import Image
import io

# Configure the logging system to track operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("demo_client")


def main():
    """
    Main function that handles the demo client workflow.
    The client connects to a server, creates a benchmark, processes image batches,
    and sends the analysis results.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Demo Client")
    parser.add_argument("endpoint", type=str, help="URL of the server endpoint")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of batches to process")
    args = parser.parse_args()

    url = args.endpoint
    limit = args.limit
    session = requests.Session()  # HTTP session to maintain connection

    logger.info("Starting demo client")

    # BENCHMARK CREATION
    # Create a new benchmark on the server with specific configurations
    create_response = session.post(
        f"{url}/api/create",
        json={"apitoken": "polimi-deib", "name": "unoptimized", "test": True, "max_batches": limit},
    )
    create_response.raise_for_status()
    bench_id = create_response.json()  # Get the ID of the created benchmark
    logger.info(f"Created bench {bench_id}")

    # START BENCHMARK
    start_response = session.post(f"{url}/api/start/{bench_id}")
    assert start_response.status_code == 200
    logger.info(f"Started bench {bench_id}")

    # BATCH PROCESSING LOOP
    i = 0
    while not limit or i < limit:  # Continue until reaching the limit or no more batches are available
        logger.info(f"Getting batch {i}")

        # Request the next batch of data from the server
        next_batch_response = session.get(f"{url}/api/next_batch/{bench_id}")
        if next_batch_response.status_code == 404:
            print("---- getting next batch 404 ----")
            break  # No more batches available
        next_batch_response.raise_for_status()

        i += 1

        logger.info(f"Next batch {i}")

    # END BENCHMARK
    # end_response = session.post(f"{url}/api/end/{bench_id}")
    # end_response.raise_for_status()
    # result = end_response.text
    # logger.info(f"Completed bench {bench_id}")
    #
    # print(f"Result: {result}")

if __name__ == "__main__":
    main()
