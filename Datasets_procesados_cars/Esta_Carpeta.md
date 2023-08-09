## ¿Por qué se hizo esta carpeta?
Se hizo para guardar los datasets resultantes después de transformar datasets de la carpeta **Datasets**

## ¿Qué contiene esta carpeta?
- "*taxi-zone.csv*": Contiene datos acerca de los distintos Boroughs y en qué zonas tienen permitido los taxis recoger pasajeros.
Los datasets descrtios a continuación contienen datos de varios vehículos dependiendo del tipo de combustible que utilizan.
- "*Vehicle_Alternative.csv*": Vehículos que utilizan un combustible alternativo a la gasolina o diesel. Estos combustibles pueden ser gas natural o E85.
- "*Vehicle-Electricity.csv*": Vehículos eléctricos.
- "*Vehicle-Gasoline.csv*": Vehículos que utilizan gasolina o diesel.
## ¿Qué csv's se editaron?
Los ficheros csv que fueron transfomrados son:
- "*taxi+_zone_lookup*": A partir de este archivo se obtuvo el dataset "*taxi-zone.csv*".
- "*Vehicle Fuel Economy data*": De este archivo se extraen tres datasets nuevos: "*Vehicle_Alternative.csv*", "*Vehicle-Electricity.csv*" y "*Vehicle-Gasoline.csv*". Se eligió este dataset porque contiene iformación acerca de las emisiones de CO2 y de esta manera es fácil revisar cuanto afecta al medio ambiente este tipo de vehículo. En estos datasets se eligieron solo los vehículos del tamaño adecuado para un taxi, es decir, no se tomaron en cuenta vehículos pequeños, camionetas, SUVs y vehículos de carga.