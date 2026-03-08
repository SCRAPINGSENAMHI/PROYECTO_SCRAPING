from app import get_stations


def main():
    df = get_stations(use_local=True)
    found = df[df['estacion'].str.contains('ORCOPAMPA', case=False, na=False)]
    if found.empty:
        print('No se encontró ORCOPAMPA en la lista de estaciones locales.')
        return
    print('Filas encontradas:')
    print(found.to_string(index=False))


if __name__ == '__main__':
    main()
