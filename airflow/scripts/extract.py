import pandas as pd

def extract_data():
    df = pd.read_csv('/opt/airflow/data/file.csv')
    print(df.head())

if __name__ == "__main__":
    extract_data()
