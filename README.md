# EVE Route Intel

## Captura manual del mercado ESI

Desde la raíz del repositorio, ejecuta:

```powershell
python scripts/capture_market_orders.py
```

El programa obtiene las regiones públicas de ESI y crea un archivo SQLite por región en `data/esi/mkt/[region_id].db`. Para capturar una región concreta:

```powershell
python scripts/capture_market_orders.py --region 10000002
```

Las páginas de cada región se descargan en paralelo con concurrencia limitada (6 por defecto, máximo 16). La base de esa región solo se reemplaza después de recibir todas las páginas, validar `X-Pages` y `Last-Modified`, y deduplicar por `order_id`. Una página fallida deja intacta la base anterior. Las regiones fallidas se muestran al final para poder reintentar solo esas con `--region ID`.

El programa respeta `Retry-After` y los encabezados de error/rate limit de ESI, reintenta errores transitorios y omite regiones cuya caché ESI siga vigente. No necesita token: las órdenes regionales públicas no requieren autenticación. Las órdenes privadas de estructuras Upwell no están incluidas.

### Esquema compacto

Cada archivo contiene `orders` y una fila en `capture`. La región se deduce del nombre del archivo, así que no se repite en cada orden. `issued` se guarda como segundos Unix y `is_buy_order` como `0`/`1`. `range_code` codifica `0=station`, `1=solarsystem`, `2=region` y `3+n=alcance numérico n`. La clave primaria es `order_id`; el índice secundario acelera búsquedas por tipo, lado de compra/venta y precio.

## Base de datos del universo

El workflow manual **Capture ESI regions** está disponible en la pestaña **Actions** de GitHub. Al ejecutarlo, descarga de ESI la lista de regiones y los detalles de cada una, guarda `data/esi/uni/regions.db` y hace push a `main` solo si la base cambió. No se ejecuta con cron ni sube artifacts.

También se puede generar localmente desde la raíz del repositorio:

```powershell
python scripts/capture_esi_regions.py
```

La base contiene `regions(region_id, name, description)` y `region_constellations(region_id, constellation_id)`. La captura se construye en un archivo temporal y reemplaza la base anterior solo después de completar y validar todas las regiones.

## Snapshot completo del SDE

El workflow manual **Capture EVE SDE** descarga el último ZIP JSONL oficial y crea una SQLite en `data/sde/` por cada archivo `.jsonl` que contiene, con el mismo nombre base (por ejemplo, `types.jsonl` genera `types.db`). Usa cuatro workers en un único job: uno prepara los siete datasets prioritarios y tres procesan el resto en paralelo. Publica el lote prioritario en cuanto está listo; publica todas las bases restantes juntas cuando termina su preparación. Cada base tiene `records(ordinal, document)` y `ordinal` conserva el orden. En todos los datasets, los campos multilingües conservan solo la traducción inglesa (`en`); los demás campos y tipos JSON se mantienen. Si una SQLite supera 100 MB, se guarda comprimida con Zstandard como `.db.zst` (por ejemplo, `mapMoons.db.zst`); se puede abrir tras descomprimirla con `zstd -d mapMoons.db.zst -o mapMoons.db`.

`metadata.json` conserva `last-modified` en ISO 8601 UTC, `max-age` como entero y `etag` incluyendo sus comillas. Si el ETag no cambió, el workflow informa y conserva el snapshot; la casilla **force** permite reconstruirlo aunque coincida. Al completar, elimina archivos obsoletos de `data/sde/` y escribe los metadatos al final. Cada archivo DB se comprueba contra el límite de 100 MB de GitHub; si una base comprimida también lo supera, la captura falla sin actualizar los metadatos.

## Recuento de páginas del mercado

El workflow manual **Capture market page counts** consulta la primera página de cada región con órdenes `all`, toma el encabezado `X-Pages` y descarta el cuerpo de órdenes. Guarda solo el recuento y la expiración de caché por región en `data/esi/mkt/pages.yml`. Usa como fuente los IDs de `data/esi/uni/regions.db`, limita la concurrencia a seis peticiones y no vuelve a consultar regiones cuya caché ESI siga vigente.

## Precios de mercado de ESI

El workflow **Capture ESI market prices** consulta `GET /markets/prices/` cada hora y también se puede ejecutar manualmente o mediante `repository_dispatch`. Guarda el snapshot en `data/esi/market/marketPrices.db`, con una fila por `type_id` y las columnas `adjusted_price` y `average_price`, más los metadatos de captura y caché de ESI. Usa `ETag` para conservar la base sin cambios cuando ESI responde `304 Not Modified`; el formulario manual permite forzar una descarga completa.

## Referencias de mercado de Jita 4-4

El workflow manual **Extract Jita 4-4 market reference prices** captura las páginas regionales de The Forge y conserva en `data/test/` las órdenes originales de la estación `60003760`: `jita4-4_buy_orders.jsonl` y `jita4-4_sell_orders.jsonl`. También genera `jita4-4_market_prices.yml`, con referencias, mejores precios, spread, ejecución, profundidad, liquidez, distribución de precios y desequilibrio del libro. Para cada artículo y lado, se validan y filtran una sola vez los outliers Tukey; las órdenes retenidas alimentan todas las métricas. Los umbrales de profundidad usan precios sin redondear y los JSONL conservan todas las órdenes capturadas, incluidas las excluidas del cálculo.

Los niveles de ejecución son una simulación del snapshot, no una garantía de ejecución: el cálculo usa órdenes situadas en la estación y no modela restricciones de `min_volume`, impuestos, comisiones ni cambios del libro entre captura y ejecución. Los JSONL mantienen los campos originales, incluidos `range` y `min_volume`, para inspeccionarlos.

Para recalcular el YAML usando los JSONL ya capturados, sin consultar ESI, ejecuta `python scripts/extract_jita_reference.py --from-jsonl`. Para una captura nueva, ejecuta `python scripts/extract_jita_reference.py --workers 6`. Las funciones reutilizables `calculateBuyReferencePrice` y `calculateSellReferencePrice` están en ese script; las pruebas unitarias están en `tests/test_jita_reference.py` y `tests/test_jita_execution.py`.

## Conversión local de SQLite

`scripts/convert_db.py` convierte localmente cualquier base `.db` o `.db.zst` del repositorio a `.json`, `.yml` y `.xlsx`, guardando los archivos junto a la base de entrada. Requiere Python, `openpyxl` y, para `.db.zst`, `zstandard`:

```powershell
python -m pip install openpyxl zstandard
python scripts/convert_db.py data/esi/market/marketPrices.db
python scripts/convert_db.py data/sde/mapMoons.db.zst --output-dir salida
```

Cada tabla SQLite se exporta por separado; las columnas `document` con JSON se convierten a objetos en JSON/YAML y a texto JSON en Excel. Tablas que excedan el máximo de filas de Excel se reparten en hojas consecutivas.

En Windows, el ejecutable `tools/EVE-SQLite-Converter.exe` permite arrastrar uno o varios `.db`/`.db.zst` encima; crea los tres formatos junto a cada archivo. También se puede abrir sin argumentos para elegir archivos. Para reconstruirlo desde el código, instala `pyinstaller`, `openpyxl` y `zstandard` y ejecuta `python -m PyInstaller --onefile --windowed --name EVE-SQLite-Converter --collect-all openpyxl --collect-all zstandard scripts/convert_db_gui.py`.
