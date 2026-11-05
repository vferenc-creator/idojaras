# energofish-szezonalis-adat

Havonta (10-én 18:00 UTC) lefutó GitHub Actions adatgyűjtő az Energofish szezonális kitekintéséhez.
Eredmény: `adat/latest.json` (és archív `adat/szezonalis_numerikus_ÉÉÉÉ-HH.json`). Ezt olvassa be a Claude ütemezett feladata 11-én.

| Rész | Forrás | Licenc |
|---|---|---|
| Havi hőmérséklet/csapadék anomália + valószínűség, 6 hónap | Copernicus C3S multi-modell (`seasonal-postprocessed-single-levels`, CDS) | Copernicus licence – ingyenes, kereskedelmi célra is |
| Folyók havi vízhozama a klímához mérve | GloFAS szezonális + reanalízis 1991–2020 (EWDS) | CEMS-FLOODS licence – ingyenes |
| 1–2. hét hőmérséklet/csapadék | ECMWF nyílt ENS + ERA5 1991–2020 klíma | CC BY 4.0 / Copernicus licence |

Attribúció a dashboardon: „Copernicus Climate Change Service, Copernicus Emergency Management Service (GloFAS), ECMWF”.

## Beállítás (egyszeri, kb. 20 perc)

1. **ECMWF/Copernicus fiók** (ingyenes): regisztrálj a https://cds.climate.copernicus.eu oldalon, és jelentkezz be a https://ewds.climate.copernicus.eu oldalon is ugyanazzal a fiókkal.
2. **API-kulcs**: a CDS profiloldalán másold ki a *Personal Access Token*-t. Az EWDS profiloldalán is nézd meg; ha ott más token látszik, azt is jegyezd fel.
3. **Licencek elfogadása** (enélkül a letöltés hibát ad). Mindegyik adatkészlet oldalán a *Download* fül alján, a licencnél nyomd meg az *Accept* gombot:
   - CDS: *Seasonal forecast anomalies on single levels* (seasonal-postprocessed-single-levels)
   - CDS: *ERA5 hourly time-series data on single levels* (reanalysis-era5-single-levels-timeseries)
   - EWDS: *Seasonal forecasts of river discharge … GloFAS* (cems-glofas-seasonal)
   - EWDS: *River discharge and related historical data from the GloFAS* (cems-glofas-historical)
4. **GitHub repó**: hozz létre egy **nyilvános** repót `energofish-szezonalis-adat` néven, és töltsd fel ennek a mappának a tartalmát (a `.github` mappával együtt). Nyilvános repóban az Actions futásidő ingyenes. A repóba csak nyilvános időjárási származtatott adat kerül, céges adat nem.
5. **Titkos kulcs**: repó → *Settings → Secrets and variables → Actions → New repository secret*:
   - `CDS_KEY` = a CDS token
   - `EWDS_KEY` = csak akkor, ha az EWDS token eltér a CDS-étől
6. **Első futtatás**: *Actions* fül → *Havi szezonális adatgyűjtés* → *Run workflow*.
   Az első futás hosszabb (akár 1–3 óra), mert egyszer kiszámolja és a `adat/cache/` mappába menti a GloFAS és az ERA5 1991–2020-as klímát. Utána havonta jellemzően 10–40 perc (a Copernicus sorban állásától függ).
7. **Ellenőrzés**: nyisd meg az `adat/latest.json` fájlt. A `meta.reszek_sikeresek` mutatja, mely rész sikerült, a `meta.hibak` a hibákat.

## Megjegyzések

- Ha egy rész hibázik (pl. egy C3S központ még nem publikált), a többi elkészül, a hiba bekerül a `meta.hibak` listába.
- A heti rész kihagyható: *Run workflow* → `heti = 0`.
- GitHub nyilvános repóban 60 nap inaktivitás után leállítja az ütemezett futásokat; a havi automatikus commit ezt megelőzi. Ha mégis leállna, az Actions fülön egy kattintással újraengedélyezhető.
