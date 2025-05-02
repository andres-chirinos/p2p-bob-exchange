import pandas as pd
import requests


def get_binance_data(
    binance_url: str, fiat: str, asset: str, tradeType: str
) -> pd.DataFrame:
    """
    Obtiene datos de Binance P2P para un par de fiat y activo específico.
    """
    params = {
        "asset": asset,
        "fiat": fiat,
        "page": 1,
        "rows": 10,
        "payTypes": [],
        "countries": [],
        "publisherType": None,
        "tradeType": tradeType,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Content-Type": "application/json",
    }

    all_data = pd.DataFrame()

    while True:
        try:
            # Realizar la solicitud
            response = requests.post(binance_url, headers=headers, json=params)
            response.raise_for_status()
            data = response.json()

            # Validar si hay datos en la respuesta
            if "data" not in data or not data["data"]:
                print(f"No hay más datos para el asset {asset} vs {fiat} en {tradeType}.")
                break

            # Normalizar y agregar los datos al DataFrame
            page_data = pd.json_normalize(data["data"])
            all_data = pd.concat([all_data, page_data], ignore_index=True)

            # Actualizar el payload para la siguiente página
            params["page"] += 1

        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud: {e}")
            break
        except KeyError as e:
            print(f"Error procesando la respuesta: {e}")
            break

    return all_data
