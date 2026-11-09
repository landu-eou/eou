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

## Recuento de páginas del mercado

El workflow manual **Capture market page counts** consulta la primera página de cada región con órdenes `all`, toma el encabezado `X-Pages` y descarta el cuerpo de órdenes. Guarda solo el recuento y la expiración de caché por región en `data/esi/mkt/pages.yml`. Usa como fuente los IDs de `data/esi/uni/regions.db`, limita la concurrencia a seis peticiones y no vuelve a consultar regiones cuya caché ESI siga vigente.
