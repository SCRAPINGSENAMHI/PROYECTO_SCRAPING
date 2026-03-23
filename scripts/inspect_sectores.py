import shapefile
sh=r'g:/1_PROYECTOS/WEBSCRAPING/Web_Scraping_SENAMHI_/DATA/SECTOR_CLIMATICO/SECTORES.shp'
try:
    sf=shapefile.Reader(sh)
    fields=[f[0] for f in sf.fields[1:]]
    print('FIELDS:', fields)
    recs=sf.records()
    print('NUM_FEATURES:', len(recs))
    for i,rec in enumerate(recs[:30]):
        d=dict(zip(fields, rec))
        print(i, d)
except Exception as e:
    import traceback
    traceback.print_exc()
