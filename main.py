from connectors.connectors import *
from transforms import *
import json

def save_caldad_aire_posgres(): #Ej 1
    try:
        df = pd.read_csv("datasets/calidad-aire.csv")
        df = save_caldad_aire_posgres_transform(df)

        df_save_postgres(df,'calidad_aire','public','fecha')

    except Exception as e:
        return json.dumps({"error message": str(e)})

    return '200'