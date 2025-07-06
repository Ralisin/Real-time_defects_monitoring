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
            break  # No more batches available
        next_batch_response.raise_for_status()

        # BATCH PROCESSING
        # Deserialize the batch data using umsgpack
        batch_input = umsgpack.unpackb(next_batch_response.content)
        result = process(batch_input)  # Process the batch (function defined below)

        # SEND RESULT
        logger.info(f"Sending batch result {i}")
        logger.info(f"[POST] result: {result}")
        result_serialized = umsgpack.packb(result)  # Serialize the result
        logger.info(f"[POST] result_serialized: {result_serialized}")
        result_response = session.post(
            f"{url}/api/result/0/{bench_id}/{i}",
            data=result_serialized
        )
        assert result_response.status_code == 200
        print(result_response.content)
        i += 1

    # END BENCHMARK
    end_response = session.post(f"{url}/api/end/{bench_id}")
    end_response.raise_for_status()
    result = end_response.text
    logger.info(f"Completed bench {bench_id}")

    print(f"Result: {result}")


def compute_outliers(image3d, empty_threshold, saturation_threshold, distance_threshold, outlier_threshold):
    """
    Computes outliers (anomalies) in a 3D image by comparing each pixel
    with its nearest and farthest neighbors.

    Args:
        image3d: 3D numpy array (depth, width, height) representing the image
        empty_threshold: Threshold below which a pixel is considered "empty"
        saturation_threshold: Threshold above which a pixel is considered "saturated"
        distance_threshold: Radius to define "close" neighbors
        outlier_threshold: Threshold to consider a pixel as outlier

    Returns:
        List of outliers in the format (x, y, deviation)
    """
    image3d = image3d.astype(np.float64)
    depth, width, height = image3d.shape

    def get_padded(image, d, x, y, pad=0.0):
        """
        Helper function to get a value from the image with padding.
        Returns the padding value if coordinates are out of bounds.
        """
        if d < 0 or d >= image.shape[0]:
            return pad
        if x < 0 or x >= image.shape[1]:
            return pad
        if y < 0 or y >= image.shape[2]:
            return pad
        return image[d, x, y]

    outliers = []

    # SCAN EVERY PIXEL OF THE LAST LAYER
    for y in range(height):
        for x in range(width):
            # Skip empty or saturated pixels in the last layer
            if image3d[-1, x, y] <= empty_threshold or image3d[-1, x, y] >= saturation_threshold:
                continue

            # CALCULATE MEAN OF NEAR NEIGHBORS
            # Consider all pixels within distance_threshold using Manhattan distance
            cn_sum = 0  # Sum of close neighbors
            cn_count = 0  # Count of close neighbors
            for j in range(-distance_threshold, distance_threshold + 1):
                for i in range(-distance_threshold, distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance <= distance_threshold:
                            cn_sum += get_padded(image3d, d, x + i, y + j)
                            cn_count += 1

            # CALCULATE MEAN OF FAR NEIGHBORS
            # Consider pixels between distance_threshold and 2*distance_threshold
            on_sum = 0  # Sum of far neighbors
            on_count = 0  # Count of far neighbors
            for j in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                for i in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance > distance_threshold and distance <= 2 * distance_threshold:
                            on_sum += get_padded(image3d, d, x + i, y + j)
                            on_count += 1

            # COMPARE MEANS
            close_mean = cn_sum / cn_count
            outer_mean = on_sum / on_count
            dev = abs(close_mean - outer_mean)

            # IDENTIFY OUTLIERS
            # A pixel is an outlier if the deviation exceeds the threshold
            if image3d[-1, x, y] > empty_threshold and image3d[
                -1, x, y] < saturation_threshold and dev > outlier_threshold:
                outliers.append((x, y, dev))

    return outliers


def cluster_outliers_2d(outliers, eps=20, min_samples=5):
    """
    Groups outliers into clusters using the DBSCAN algorithm.
    Computes the centroid of each cluster to identify problematic areas.

    Args:
        outliers: List of outliers in the format (x, y, deviation)
        eps: Maximum distance between points in the same cluster
        min_samples: Minimum number of points to form a cluster

    Returns:
        List of centroids with coordinates and cluster size
    """
    if len(outliers) == 0:
        return []

    # Extract only 2D coordinates (x, y) for clustering
    positions = np.array([(outlier[0], outlier[1]) for outlier in outliers])

    # Apply DBSCAN clustering algorithm
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(positions)
    labels = clustering.labels_  # Cluster labels (-1 = noise)

    # CALCULATE CENTROIDS
    centroids = []
    for label in set(labels):
        if label == -1:
            continue  # Skip noise points

        # Get all points in the current cluster
        cluster_points = positions[labels == label]

        # Compute centroid as mean of coordinates
        centroid = cluster_points.mean(axis=0)
        centroids.append({
            'x': centroid[0],
            'y': centroid[1],
            'count': len(cluster_points)  # Cluster size
        })

    return centroids


# GLOBAL MAP TO STORE SLIDING WINDOWS
# Each tile keeps a window of 3 consecutive images
tile_map = dict()


def process(batch):
    """
    Processes a single batch of data containing one image layer.
    Maintains a sliding window of 3 layers per tile and analyzes
    outliers when the window is full.

    Args:
        batch: Dictionary containing batch data (print_id, tile_id, layer, image)

    Returns:
        Dictionary with the analysis results (saturated pixels, outlier centroids)
    """
    # CONFIGURATION PARAMETERS
    EMPTY_THRESH = 5000
    SATURATION_THRESH = 65000
    DISTANCE_FACTOR = 2
    OUTLIER_THRESH = 6000
    DBSCAN_EPS = 20
    DBSCAN_MIN = 5

    # Extract data from batch
    print_id = batch["print_id"]
    tile_id = batch["tile_id"]
    batch_id = batch["batch_id"]
    layer = batch["layer"]
    image = Image.open(io.BytesIO(batch["tif"]))  # Load TIF image from bytes

    logger.info(f"Processing layer {layer} of print {print_id}, tile {tile_id}")

    # SLIDING WINDOW MANAGEMENT
    # Initialize the window for this tile if it doesn't exist
    if not (print_id, tile_id) in tile_map:
        tile_map[(print_id, tile_id)] = []

    window = tile_map[(print_id, tile_id)]

    # Keep only the last 3 images (sliding window)
    if len(window) == 3:
        window.pop(0)  # Remove oldest image

    window.append(image)  # Add new image

    # COUNT SATURATED PIXELS
    # Count how many pixels exceed the saturation threshold
    saturated = np.count_nonzero(np.array(image) > SATURATION_THRESH)

    # OUTLIER ANALYSIS
    centroids = []
    if len(window) == 3:  # Proceed only when 3 consecutive layers are available
        # Create 3D matrix by stacking 3 layers
        image3d = np.stack(window, axis=0)

        # Padding con NaN
        image3d_padded = np.pad(image3d, ((0, 0), (2, 2), (2, 2)), mode='constant', constant_values=np.nan)

        # Maschere fisse
        shape = (3, 5, 5)
        center = (2, 2, 2)
        l, x, y = np.indices(shape)
        dist = np.abs(l - center[0]) + np.abs(x - center[1]) + np.abs(y - center[2])
        mask_nearest = dist <= 2
        mask_external = (dist > 2) & (dist <= 4)

        top_layer_idx = image3d.shape[0] - 1
        height, width = image3d.shape[1], image3d.shape[2]

        exceeding_temp_dev_points = []

        for x_p in range(height):
            for y_p in range(width):
                patch = image3d_padded[top_layer_idx - 2:top_layer_idx + 1, x_p:x_p + 5, y_p:y_p + 5]  # shape (3,5,5)

                local_neighbors = patch[mask_nearest]
                external_neighbors = patch[mask_external]

                avg_local = np.nanmean(local_neighbors)
                avg_external = np.nanmean(external_neighbors)

                if np.isnan(avg_local) or np.isnan(avg_external):
                    continue  # salta se non hai dati reali

                local_temp_deviation = abs(avg_local - avg_external)

                if local_temp_deviation > 6000:
                    print(
                        f"layer: {top_layer_idx}, tile_id: {tile_id}, point: ({x_p}, {y_p}), local_temp_deviation: {local_temp_deviation}")
                    # exceeding_temp_dev_points.append((x_p, y_p))

        # Compute outliers by comparing each pixel to its neighbors
        outliers = compute_outliers(image3d, EMPTY_THRESH, SATURATION_THRESH, DISTANCE_FACTOR, OUTLIER_THRESH)

        # Cluster outliers and compute centroids
        centroids = cluster_outliers_2d(outliers, DBSCAN_EPS, DBSCAN_MIN)

        # Optionally: sort centroids by cluster size
        # centroids = sorted(centroids, key=lambda x: -x['count'])

    # PREPARE RESULT
    result = {
        "batch_id": batch_id,
        "print_id": print_id,
        "tile_id": tile_id,
        "saturated": saturated,  # Number of saturated pixels
        "centroids": centroids  # Centroids of outlier clusters
    }

    print(result)
    return result


if __name__ == "__main__":
    main()
