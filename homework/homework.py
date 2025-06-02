"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import glob
import zipfile
import os


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    dataframe = load_data()
    client = client_preprocesing(dataframe)
    campaing = campaign_preprocesing(dataframe)
    economics = economics_preprocesing(dataframe)
    save_output(client, "client")
    save_output(campaing, "campaign")
    save_output(economics, "economics")

    return


def client_preprocesing(dataframe):
    client = dataframe[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ].copy()

    client["job"] = (
        client["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    client["education"] = client["education"].str.replace(".", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA)

    client["credit_default"] = client["credit_default"].apply(
        lambda x: 1 if x == "yes" else 0
    )

    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    return client


def campaign_preprocesing(dataframe):
    campaign = dataframe[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()

    month_dict = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }
    campaign["month"] = campaign["month"].map(month_dict)

    campaign["last_contact_date"] = pd.to_datetime(
        "2022" + "-" + campaign["month"] + "-" + campaign["day"].astype(str),
        format="%Y-%m-%d",
    )
    campaign = campaign.drop(["month", "day"], axis=1)

    campaign["previous_outcome"] = campaign["previous_outcome"].apply(
        lambda x: 1 if x == "success" else 0
    )
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(
        lambda x: 1 if x == "yes" else 0
    )

    return campaign


def economics_preprocesing(dataframe):
    economics = dataframe[
        [
            "client_id",
            "cons_price_idx",
            "euribor_three_months",
        ]
    ].copy()

    return economics


def save_output(dataframe, name, output_directory="files/output"):
    """Save output to a file."""

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    dataframe.to_csv(
        f"{output_directory}/{name}.csv",
        index=False,
    )


def load_data():
    input_dir = "files/input/"

    # Lista para almacenar los DataFrames individuales
    dataframes = []

    # Iterar sobre todos los archivos en el directorio
    for file in os.listdir(input_dir):
        if file.endswith(".csv.zip"):
            # Ruta completa del archivo zip
            zip_path = os.path.join(input_dir, file)

            # Abrir el archivo zip
            with zipfile.ZipFile(zip_path, "r") as z:
                # Obtener el nombre del archivo CSV dentro del zip
                csv_filename = z.namelist()[0]

                # Leer el archivo CSV directamente desde el zip
                with z.open(csv_filename) as csvfile:
                    df = pd.read_csv(csvfile)
                    dataframes.append(df)

    # Concatenar todos los DataFrames en uno solo
    final_dataframe = pd.concat(dataframes, ignore_index=True)

    return final_dataframe


if __name__ == "__main__":
    clean_campaign_data()
