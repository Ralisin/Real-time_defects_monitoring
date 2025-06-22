import argparse
import logging
import requests
import umsgpack
import numpy as np
from sklearn.cluster import DBSCAN
from PIL import Image
import io

# Configurazione del sistema di logging per tracciare le operazioni
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("demo_client")


def main():
    """
    Funzione principale che gestisce il flusso di lavoro del client demo.
    Il client si connette a un server, crea un benchmark, elabora batch di immagini
    e invia i risultati dell'analisi.
    """
    # Parsing degli argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Demo Client")
    parser.add_argument("endpoint", type=str, help="URL dell'endpoint del server")
    parser.add_argument("--limit", type=int, default=None, help="Numero massimo di batch da elaborare")
    args = parser.parse_args()

    url = args.endpoint
    limit = args.limit
    session = requests.Session()  # Sessione HTTP per mantenere la connessione

    logger.info("Starting demo client")

    # CREAZIONE DEL BENCHMARK
    # Crea un nuovo benchmark sul server con configurazioni specifiche
    create_response = session.post(
        f"{url}/api/create",
        json={"apitoken": "polimi-deib", "name": "unoptimized", "test": True, "max_batches": limit},
    )
    create_response.raise_for_status()
    bench_id = create_response.json()  # Ottiene l'ID del benchmark creato
    logger.info(f"Created bench {bench_id}")

    # AVVIO DEL BENCHMARK
    start_response = session.post(f"{url}/api/start/{bench_id}")
    assert start_response.status_code == 200
    logger.info(f"Started bench {bench_id}")

    # CICLO DI ELABORAZIONE DEI BATCH
    i = 0
    while not limit or i < limit:  # Continua finché non raggiunge il limite o non ci sono più batch
        logger.info(f"Getting batch {i}")

        # Richiede il prossimo batch di dati dal server
        next_batch_response = session.get(f"{url}/api/next_batch/{bench_id}")
        if next_batch_response.status_code == 404:
            break  # Non ci sono più batch disponibili
        next_batch_response.raise_for_status()

        # ELABORAZIONE DEL BATCH
        # Deserializza i dati del batch usando umsgpack
        batch_input = umsgpack.unpackb(next_batch_response.content)
        result = process(batch_input)  # Elabora il batch (funzione definita sotto)

        # INVIO DEL RISULTATO
        logger.info(f"Sending batch result {i}")
        result_serialized = umsgpack.packb(result)  # Serializza il risultato
        result_response = session.post(
            f"{url}/api/result/0/{bench_id}/{i}",
            data=result_serialized
        )
        assert result_response.status_code == 200
        print(result_response.content)
        i += 1

    # CHIUSURA DEL BENCHMARK
    end_response = session.post(f"{url}/api/end/{bench_id}")
    end_response.raise_for_status()
    result = end_response.text
    logger.info(f"Completed bench {bench_id}")

    print(f"Result: {result}")


def compute_outliers(image3d, empty_threshold, saturation_threshold, distance_threshold, outlier_threshold):
    """
    Calcola gli outliers (anomalie) in un'immagine 3D confrontando ogni pixel
    con i suoi vicini più prossimi e più lontani.

    Args:
        image3d: Array 3D numpy (depth, width, height) rappresentante l'immagine
        empty_threshold: Soglia sotto la quale un pixel è considerato "vuoto"
        saturation_threshold: Soglia sopra la quale un pixel è considerato "saturo"
        distance_threshold: Raggio per definire i vicini "vicini"
        outlier_threshold: Soglia per considerare un pixel come outlier

    Returns:
        Lista di outliers nel formato (x, y, deviazione)
    """
    image3d = image3d.astype(np.float64)
    depth, width, height = image3d.shape

    def get_padded(image, d, x, y, pad=0.0):
        """
        Funzione helper per ottenere un valore dall'immagine con padding.
        Restituisce il valore di padding se le coordinate sono fuori dai limiti.
        """
        if d < 0 or d >= image.shape[0]:
            return pad
        if x < 0 or x >= image.shape[1]:
            return pad
        if y < 0 or y >= image.shape[2]:
            return pad
        return image[d, x, y]

    outliers = []

    # SCANSIONE DI OGNI PIXEL DELL'ULTIMO LAYER
    for y in range(height):
        for x in range(width):
            # Salta pixel vuoti o saturi nell'ultimo layer
            if image3d[-1, x, y] <= empty_threshold or image3d[-1, x, y] >= saturation_threshold:
                continue

            # CALCOLO MEDIA DEI VICINI VICINI
            # Considera tutti i pixel entro distance_threshold usando distanza Manhattan
            cn_sum = 0  # Somma dei vicini vicini
            cn_count = 0  # Conteggio dei vicini vicini
            for j in range(-distance_threshold, distance_threshold + 1):
                for i in range(-distance_threshold, distance_threshold + 1):
                    for d in range(depth):
                        # Distanza Manhattan 3D
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance <= distance_threshold:
                            cn_sum += get_padded(image3d, d, x + i, y + j)
                            cn_count += 1

            # CALCOLO MEDIA DEI VICINI LONTANI
            # Considera pixel tra distance_threshold e 2*distance_threshold
            on_sum = 0  # Somma dei vicini lontani
            on_count = 0  # Conteggio dei vicini lontani
            for j in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                for i in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance > distance_threshold and distance <= 2 * distance_threshold:
                            on_sum += get_padded(image3d, d, x + i, y + j)
                            on_count += 1

            # CONFRONTO DELLE MEDIE
            close_mean = cn_sum / cn_count  # Media dei vicini vicini
            outer_mean = on_sum / on_count  # Media dei vicini lontani
            dev = abs(close_mean - outer_mean)  # Deviazione assoluta

            # IDENTIFICAZIONE OUTLIERS
            # Un pixel è outlier se la deviazione supera la soglia
            if image3d[-1, x, y] > empty_threshold and image3d[
                -1, x, y] < saturation_threshold and dev > outlier_threshold:
                outliers.append((x, y, dev))

    return outliers


def cluster_outliers_2d(outliers, eps=20, min_samples=5):
    """
    Raggruppa gli outliers in cluster usando l'algoritmo DBSCAN.
    Calcola i centroidi di ogni cluster per identificare aree problematiche.

    Args:
        outliers: Lista di outliers nel formato (x, y, deviazione)
        eps: Distanza massima tra punti nello stesso cluster
        min_samples: Numero minimo di punti per formare un cluster

    Returns:
        Lista di centroidi con coordinate e dimensione del cluster
    """
    if len(outliers) == 0:
        return []

    # Estrae solo le coordinate 2D (x, y) per il clustering
    positions = np.array([(outlier[0], outlier[1]) for outlier in outliers])

    # Applica l'algoritmo DBSCAN per il clustering
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(positions)
    labels = clustering.labels_  # Etichette dei cluster (-1 = rumore)

    # CALCOLO DEI CENTROIDI
    centroids = []
    for label in set(labels):
        if label == -1:
            continue  # Salta i punti di rumore

        # Ottiene tutti i punti del cluster corrente
        cluster_points = positions[labels == label]

        # Calcola il centroide come media delle coordinate
        centroid = cluster_points.mean(axis=0)
        centroids.append({
            'x': centroid[0],
            'y': centroid[1],
            'count': len(cluster_points)  # Dimensione del cluster
        })

    return centroids


# MAPPA GLOBALE PER MEMORIZZARE LE FINESTRE SCORREVOLI
# Ogni tile mantiene una finestra di 3 immagini consecutive
tile_map = dict()


def process(batch):
    """
    Elabora un singolo batch di dati contenente un'immagine di un layer.
    Mantiene una finestra scorrevole di 3 layer per ogni tile e analizza
    gli outliers quando la finestra è completa.

    Args:
        batch: Dizionario contenente i dati del batch (print_id, tile_id, layer, immagine)

    Returns:
        Dizionario con i risultati dell'analisi (pixel saturi, centroidi degli outliers)
    """
    # PARAMETRI DI CONFIGURAZIONE
    EMPTY_THRESH = 5000  # Soglia per pixel vuoti
    SATURATION_THRESH = 65000  # Soglia per pixel saturi
    DISTANCE_FACTOR = 2  # Raggio per i vicini nell'analisi outliers
    OUTLIER_THRESH = 6000  # Soglia per identificare outliers
    DBSCAN_EPS = 20  # Parametro eps per DBSCAN
    DBSCAN_MIN = 5  # Numero minimo di campioni per DBSCAN

    # Estrazione dei dati dal batch
    print_id = batch["print_id"]
    tile_id = batch["tile_id"]
    batch_id = batch["batch_id"]
    layer = batch["layer"]
    image = Image.open(io.BytesIO(batch["tif"]))  # Carica l'immagine TIF da bytes

    logger.info(f"Processing layer {layer} of print {print_id}, tile {tile_id}")

    # GESTIONE DELLA FINESTRA SCORREVOLE
    # Inizializza la finestra per questo tile se non esiste
    if not (print_id, tile_id) in tile_map:
        tile_map[(print_id, tile_id)] = []

    window = tile_map[(print_id, tile_id)]

    # Mantiene solo le ultime 3 immagini (finestra scorrevole)
    if len(window) == 3:
        window.pop(0)  # Rimuove l'immagine più vecchia

    window.append(image)  # Aggiunge la nuova immagine

    # CONTEGGIO PIXEL SATURI
    # Conta quanti pixel superano la soglia di saturazione
    saturated = np.count_nonzero(np.array(image) > SATURATION_THRESH)

    # ANALISI DEGLI OUTLIERS
    centroids = []
    if len(window) == 3:  # Procede solo quando ha 3 layer consecutivi
        # Crea una matrice 3D impilando i 3 layer
        image3d = np.stack(window, axis=0)

        # Calcola gli outliers confrontando ogni pixel con i suoi vicini
        outliers = compute_outliers(image3d, EMPTY_THRESH, SATURATION_THRESH, DISTANCE_FACTOR, OUTLIER_THRESH)

        # Raggruppa gli outliers in cluster e calcola i centroidi
        centroids = cluster_outliers_2d(outliers, DBSCAN_EPS, DBSCAN_MIN)

        # Opzionale: ordina i centroidi per dimensione del cluster
        # centroids = sorted(centroids, key=lambda x: -x['count'])

    # PREPARAZIONE DEL RISULTATO
    result = {
        "batch_id": batch_id,
        "print_id": print_id,
        "tile_id": tile_id,
        "saturated": saturated,  # Numero di pixel saturi
        "centroids": centroids  # Centroidi dei cluster di outliers
    }

    print(result)
    return result


if __name__ == "__main__":
    main()