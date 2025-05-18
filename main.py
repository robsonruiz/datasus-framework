from dash import Dash
import extract, transform, load, view

if __name__ == "__main__":
    # Example defaults, these can be made dynamic or from CLI later
    group = "RD"
    state = "SP"
    year = 2020

    sdf = extract.extract_data(group, state, year)
    sdf_transformed = transform.transform_data(sdf)
    csv_path = transform.parquet_to_csv(sdf_transformed)
    final_df = load.load_data(csv_path)
    view.view(final_df)
