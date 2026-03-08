from app import get_stations


def main():
    df = get_stations(use_local=True)
    print('Columnas disponibles en el DataFrame de estaciones:')
    print(df.columns.tolist())
    found = df[df['estacion'].str.contains('ORCOPAMPA', case=False, na=False)]
    if found.empty:
        print('No se encontró ORCOPAMPA')
        return
    row = found.iloc[0]
    keys = ['estacion', 'cod', 'ico', 'cod_old', 'categoria', 'estado', 'lat', 'lon']
    for k in keys:
        print(f"{k}: {row.get(k)}")
    # columnas adicionales que pueden contener el tipo
    print("CONVENCIONAL:", row.get('CONVENCIONAL'))
    print("AUTOMATICA:", row.get('AUTOMATICA'))


if __name__ == '__main__':
    main()
