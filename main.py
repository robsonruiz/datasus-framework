from dash import Dash
import extract, transform, load, view

if __name__ == "__main__":
    df = extract.extract()
    df = transform.parquet_to_csv(df)
    df = load.load(df)
    #view.view(df)