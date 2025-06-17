import geopandas as gpd

shapefile = gpd.read_file(r"E:\논문\On going\auto\shp 파일\LSMD_ADM_SECT_UMD_전북특별자치도\LSMD_ADM_SECT_UMD_52_202504.shp")
shapefile.to_file(r"E:\논문\On going\auto\shp 파일\전북특별자치도_emd.geojson", driver='GeoJSON')
