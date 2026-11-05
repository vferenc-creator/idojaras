#!/usr/bin/env python3
"""
Energofish – szezonális numerikus adatgyűjtő (GitHub Actions, havonta 10-én).

Ingyenes, kereskedelmi célra is használható forrásokból számol:
  1. Copernicus C3S multi-modell szezonális előrejelzés (CDS)
       -> havi hőmérséklet- és csapadék-anomália + P(átlag felett), 6 hónapra, rendszerenként és összesítve
  2. GloFAS szezonális vízhozam-előrejelzés (EWDS) + GloFAS reanalízis 1991–2020 klíma
       -> folyónként havi medián / p25 / p75 vízhozam és eltérés a klímától
  3. ECMWF nyílt ENS (15 nap) + ERA5 1991–2020 klíma
       -> 1. és 2. hét hőmérséklet- és csapadék-anomália + P(átlag felett)

Kimenet: adat/szezonalis_numerikus_<ÉÉÉÉ-HH>.json és adat/latest.json
Minden rész külön fut; ami hibázik, az a meta.hibak listába kerül, a többi elkészül.

Licencek / attribúció: Copernicus Climate Change Service (C3S), Copernicus Emergency Management
Service (CEMS-FLOODS, GloFAS), ECMWF open data (CC BY 4.0), ERA5 (C3S).
"""
from __future__ import annotations

import calendar
import datetime as dt
import io
import json
import os
import pathlib
import sys
import tempfile
import time
import traceback
import zipfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout

os.environ.setdefault("TQDM_DISABLE", "1")  # ecmwf-opendata letöltési folyamatjelző ki

import numpy as np
import pandas as pd
import xarray as xr

ADAT = pathlib.Path("adat")
CACHE = ADAT / "cache"
CDS_URL = "https://cds.climate.copernicus.eu/api"
EWDS_URL = "https://ewds.climate.copernicus.eu/api"
KULCS_FAJL = pathlib.Path(__file__).resolve().parent / "copernikus.txt"  # tartalék, ha nincs secret
PARHUZAMOS = int(os.environ.get("PARHUZAMOS", "4"))  # egyszerre futó CDS/EWDS kérések
# A szezonális GloFAS "operational" jelenleg a v4-es modell (a v4 reanalízis egyezik az előrejelzés 1. napjával)
GLOFAS_KLIMA_VERZIO = os.environ.get("GLOFAS_KLIMA_VERZIO", "version_4_0")
GLOFAS_NAP_LEPES = int(os.environ.get("GLOFAS_NAP_LEPES", "3"))  # 1 = minden nap (lassabb: 30 kérés)
GLOFAS_KLIMA_TURELEM_PERC = float(os.environ.get("GLOFAS_KLIMA_TURELEM_PERC", "150"))  # ennyit vár az EWDS sorára

PONTOK = [  # név, szélesség, hosszúság, régiócsoport
    ("Győr", 47.68, 17.63, "NYUGAT"),
    ("Siófok (Balaton)", 46.91, 18.05, "NYUGAT"),
    ("Pécs", 46.07, 18.23, "NYUGAT"),
    ("Budapest", 47.50, 19.04, "KOZEP_ESZAK"),
    ("Miskolc", 48.10, 20.78, "KOZEP_ESZAK"),
    ("Szeged", 46.25, 20.15, "ALFOLD"),
    ("Debrecen", 47.53, 21.63, "ALFOLD"),
]
REGIOK = ["NYUGAT", "KOZEP_ESZAK", "ALFOLD"]
HU_AREA = [49, 16, 45, 23]  # N, W, S, E (1°-os C3S rácshoz)

FOLYOK = [  # folyó, szelvény, szélesség, hosszúság
    ("Duna", "Budapest", 47.49, 19.05),
    ("Duna", "Mohács", 45.99, 18.69),
    ("Tisza", "Szolnok", 47.17, 20.19),
    ("Tisza", "Szeged", 46.25, 20.16),
    ("Dráva", "Barcs", 45.95, 17.46),
    ("Hármas-Körös", "Gyoma", 46.94, 20.83),
]
FOLYO_AREA = [47.7, 17.3, 45.8, 21.0]

C3S_KOZPONTOK = ["ecmwf", "ukmo", "meteo_france", "dwd", "cmcc", "ncep", "jma", "eccc", "bom"]
C3S_VALTOZOK = {
    "homerseklet": "2m_temperature_anomaly",
    "csapadek": "total_precipitation_anomalous_rate_of_accumulation",
}

HIBAK: list[str] = []
FORRASOK: list[dict] = []


def naplo(msg: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc).replace(tzinfo=None):%H:%M:%S}] {msg}", flush=True)


def hiba(resz: str, exc: BaseException | str) -> None:
    szoveg = f"{resz}: {exc if isinstance(exc, str) else repr(exc)}"
    HIBAK.append(szoveg[:500])
    naplo("HIBA " + szoveg)
    if not isinstance(exc, str):
        traceback.print_exc()


def honap_plusz(ev: int, ho: int, n: int) -> tuple[int, int]:
    idx = ev * 12 + (ho - 1) + n
    return idx // 12, idx % 12 + 1


def r(x, n=2):
    return None if x is None or (isinstance(x, float) and not np.isfinite(x)) else round(float(x), n)


# --------------------------------------------------------------------------- kliensek
def kliens(url: str, kulcs_env: list[str]):
    from ecmwf.datastores import Client

    kulcs = next((os.environ[k] for k in kulcs_env if os.environ.get(k)), None)
    if not kulcs and KULCS_FAJL.exists():  # "url: …" / "key: …" formátum
        kulcs = next((sor.split(":", 1)[1].strip() for sor in KULCS_FAJL.read_text(encoding="utf-8").splitlines()
                      if sor.strip().lower().startswith("key")), None)
    if not kulcs:
        raise RuntimeError(f"Hiányzó API-kulcs ({' / '.join(kulcs_env)} secret)")
    return Client(url=url, key=kulcs, progress=False, maximum_tries=50, retry_after=30)


def letolt(cl, gyujtemeny: str, keres: dict, cel: pathlib.Path) -> pathlib.Path:
    naplo(f"Lekérés: {gyujtemeny} {json.dumps({k: v for k, v in keres.items() if k != 'leadtime_hour'})[:200]}")
    cl.retrieve(gyujtemeny, keres, target=str(cel))
    if zipfile.is_zipfile(cel):  # néha zip jön, akkor kicsomagoljuk az első adatfájlt
        with zipfile.ZipFile(cel) as z:
            nev = next(n for n in z.namelist() if n.endswith((".nc", ".grib", ".grib2", ".csv")))
            ki = cel.with_name(cel.stem + "_" + pathlib.Path(nev).name)
            ki.write_bytes(z.read(nev))
            return ki
    return cel


def megnyit(p: pathlib.Path) -> xr.Dataset:
    if p.suffix in (".grib", ".grib2"):
        return xr.open_dataset(p, engine="cfgrib", backend_kwargs={"indexpath": ""})
    try:
        return xr.open_dataset(p)
    except Exception:
        return xr.open_dataset(p, engine="cfgrib", backend_kwargs={"indexpath": ""})


def adatvaltozo(ds: xr.Dataset) -> xr.DataArray:
    jelolt = [v for v in ds.data_vars if ds[v].ndim >= 2]
    return ds[jelolt[0] if jelolt else list(ds.data_vars)[0]]


def dim_nev(da: xr.DataArray, jeloltek: tuple[str, ...]) -> str | None:
    return next((d for d in jeloltek if d in da.dims), None)


def lat_lon(da: xr.DataArray) -> tuple[str, str]:
    lat = dim_nev(da, ("latitude", "lat", "y"))
    lon = dim_nev(da, ("longitude", "lon", "x"))
    if not lat or not lon:
        raise ValueError(f"Nincs lat/lon dimenzió: {da.dims}")
    return lat, lon


# --------------------------------------------------------------------------- 1. C3S multi-modell havi
def c3s_rendszer(cl, kozpont: str, ev: int, ho: int) -> str | None:
    try:
        c = cl.apply_constraints("seasonal-postprocessed-single-levels",
                                 {"originating_centre": kozpont, "year": [str(ev)], "month": [f"{ho:02d}"]})
        rendszerek = c.get("system") or []
        if rendszerek:
            return sorted(rendszerek, key=lambda s: (len(str(s)), str(s)))[-1]
    except Exception as e:
        hiba(f"C3S constraints {kozpont}", e)
    return None


def c3s_feldolgoz(da: xr.DataArray, ev: int, ho: int, valtozo: str) -> dict:
    """Visszaad: {(pont, honap): (tag_anomaliak_tomb)} a pontokra; csapadéknál mm/hónap."""
    for d in ("forecast_reference_time", "indexing_time", "time"):
        if d in da.dims and da.sizes[d] == 1:
            da = da.isel({d: 0})
    tag = dim_nev(da, ("number", "realization", "member"))
    lead = dim_nev(da, ("forecastMonth", "leadtime_month", "step", "valid_time"))
    lat, lon = lat_lon(da)
    if lead is None:
        raise ValueError(f"Nincs előretekintési dimenzió: {da.dims}")
    eredmeny = {}
    for i in range(da.sizes[lead]):
        if lead in ("forecastMonth", "leadtime_month"):
            v = int(da[lead].values[i])
            vev, vho = honap_plusz(ev, ho, v - 1)
        elif lead == "valid_time":
            t = pd.Timestamp(da[lead].values[i]); vev, vho = t.year, t.month
        else:  # step (timedelta) – a hónap kezdetétől
            t = pd.Timestamp(dt.datetime(ev, ho, 1)) + pd.Timedelta(da[lead].values[i])
            vev, vho = t.year, t.month
        napok = calendar.monthrange(vev, vho)[1]
        for nev, plat, plon, _ in PONTOK:
            x = da.isel({lead: i}).sel({lat: plat, lon: plon}, method="nearest").values
            x = np.atleast_1d(np.asarray(x, dtype=float)).ravel() if tag else np.atleast_1d(float(x))
            if valtozo == "csapadek":
                x = x * 1000.0 * 86400.0 * napok  # m/s -> mm/hónap
            eredmeny[(nev, f"{vev}-{vho:02d}")] = x[np.isfinite(x)]
    return eredmeny


def c3s_havi(ev: int, ho: int) -> tuple[list, list]:
    cl = kliens(CDS_URL, ["CDS_KEY", "CDSAPI_KEY"])
    rendszerenkent = {}  # (kozpont, valtozo) -> {(pont, honap): tagok}
    hasznalt = []
    with tempfile.TemporaryDirectory() as tmp:
        feladatok = []
        for kozpont in C3S_KOZPONTOK:
            rendszer = c3s_rendszer(cl, kozpont, ev, ho)
            if not rendszer:
                hiba(f"C3S {kozpont}", "nincs elérhető rendszer erre az indításra (még nem jelent meg?)")
                continue
            feladatok += [(kozpont, rendszer, valtozo, cds_var) for valtozo, cds_var in C3S_VALTOZOK.items()]

        def egy(f):
            kozpont, rendszer, valtozo, cds_var = f
            try:
                p = letolt(cl, "seasonal-postprocessed-single-levels", {
                    "originating_centre": kozpont, "system": rendszer, "variable": [cds_var],
                    "product_type": ["monthly_mean"], "year": [str(ev)], "month": [f"{ho:02d}"],
                    "leadtime_month": [str(i) for i in range(1, 7)], "area": HU_AREA,
                    "data_format": "netcdf", "download_format": "unarchived",
                }, pathlib.Path(tmp) / f"c3s_{kozpont}_{valtozo}.nc")
                da = adatvaltozo(megnyit(p)).load()
                rendszerenkent[(kozpont, valtozo)] = c3s_feldolgoz(da, ev, ho, valtozo)
            except Exception as e:
                hiba(f"C3S {kozpont} {valtozo}", e)

        with ThreadPoolExecutor(PARHUZAMOS) as ex:
            list(ex.map(egy, feladatok))
        for kozpont, rendszer, _, _ in feladatok:
            if any(k[0] == kozpont for k in rendszerenkent) and f"{kozpont}:{rendszer}" not in hasznalt:
                hasznalt.append(f"{kozpont}:{rendszer}")
    rendszerenkent = {k: rendszerenkent[k] for k in sorted(rendszerenkent)}  # determinisztikus sorrend
    if not rendszerenkent:
        raise RuntimeError("Egyetlen C3S rendszer sem adott adatot")
    FORRASOK.append({"forras_id": "F-C3S", "nev": "Copernicus C3S multi-modell szezonális előrejelzés",
                     "dataset": "seasonal-postprocessed-single-levels", "rendszerek": hasznalt,
                     "inicializalas": f"{ev}-{ho:02d}", "licenc": "Copernicus licence (C3S)",
                     "attribucio": "Generated using Copernicus Climate Change Service information"})

    # Aggregálás: rendszeren belül pontátlag; rendszerek között egyenlő súly
    honapok = sorted({h for d in rendszerenkent.values() for (_, h) in d})
    havi, rendszer_sorok = [], []
    for valtozo in C3S_VALTOZOK:
        egyseg = "°C" if valtozo == "homerseklet" else "mm/hónap"
        for elotav, honap in enumerate(honapok):
            for terulet in ["Magyarország"] + REGIOK:
                pontok = [p for p in PONTOK if terulet == "Magyarország" or p[3] == terulet]
                anom, prob, tagok, rsz = [], [], 0, 0
                for (kozpont, vv), d in rendszerenkent.items():
                    if vv != valtozo:
                        continue
                    pa = [d[(p[0], honap)] for p in pontok if (p[0], honap) in d and d[(p[0], honap)].size]
                    if not pa:
                        continue
                    a = float(np.mean([x.mean() for x in pa]))
                    pr = float(np.mean([(x > 0).mean() for x in pa]))
                    anom.append(a); prob.append(pr); tagok += int(pa[0].size); rsz += 1
                    if terulet == "Magyarország":
                        rendszer_sorok.append({"honap": honap, "valtozo": valtozo, "rendszer": kozpont,
                                               "anomalia": r(a, 1 if valtozo == "homerseklet" else 0),
                                               "egyseg": egyseg, "p_atlag_felett": r(pr), "tagok": int(pa[0].size)})
                if not rsz:
                    continue
                p_mean = float(np.mean(prob))
                havi.append({
                    "honap": honap, "elotav_honap": elotav, "terulet": terulet, "valtozo": valtozo,
                    "anomalia": r(np.mean(anom), 1 if valtozo == "homerseklet" else 0), "egyseg": egyseg,
                    "p_atlag_felett": r(p_mean), "p_atlag_alatt": r(1 - p_mean),
                    "rendszerek_szama": rsz, "rendszerek_atlag_felett": int(sum(p > 0.5 for p in prob)),
                    "tagok_szama": tagok, "forras_id": "F-C3S",
                })
    return havi, rendszer_sorok


# --------------------------------------------------------------------------- 2. GloFAS vízhozam
def _lista(x) -> list[str]:
    if x is None:
        return []
    if isinstance(x, dict):
        x = list(x)
    return [str(v) for v in (x if isinstance(x, (list, tuple, set)) else [x])]


def glofas_valtozo(cl, gyujtemeny: str, alap: dict, verzio: str | None = None) -> dict:
    """Constraints alapján kiválasztja a ténylegesen elérhető változónevet / verziót / timespan-t.

    Két lépés: először a vízhozam-változó nevét választjuk ki, utána a változóhoz szűkített
    constraints-ből a többi mezőt (historicalnál pl. a vízhozam csak timespan=time_mean-nel létezik).
    """
    valasz: dict = {}
    c: dict = {}
    for lepes in range(2):
        try:
            c = dict(cl.apply_constraints(gyujtemeny, dict(alap, **valasz)))
        except Exception as e:
            hiba(f"GloFAS constraints {gyujtemeny}", e)
            break
        if lepes == 0:
            v = _lista(c.get("variable")) or ["river_discharge_in_the_last_24_hours"]
            valasz["variable"] = [next((x for x in v if "river_discharge" in x), v[0])]
    sv = _lista(c.get("system_version"))
    if sv:
        if verzio and verzio in sv:
            valasz["system_version"] = [verzio]
        else:
            valasz["system_version"] = ["operational"] if "operational" in sv else [sorted(sv)[-1]]
    if _lista(c.get("hydrological_model")):
        hm = _lista(c["hydrological_model"])
        valasz["hydrological_model"] = ["lisflood"] if "lisflood" in hm else [hm[0]]
    if _lista(c.get("product_type")):
        pt = _lista(c["product_type"])
        valasz["product_type"] = ["consolidated"] if "consolidated" in pt else [pt[0]]
    if _lista(c.get("timespan")):
        ts = _lista(c["timespan"])
        valasz["timespan"] = ["time_mean"] if "time_mean" in ts else [ts[0]]
    if "variable" not in valasz:
        valasz["variable"] = ["average_river_discharge_in_the_last_24_hours" if "historical" in gyujtemeny
                              else "river_discharge_in_the_last_24_hours"]
    return valasz


def legnagyobb_folyo_cella(da_mean: xr.DataArray, lat: float, lon: float, latn: str, lonn: str):
    ablak = da_mean.sel({latn: slice(lat + 0.12, lat - 0.12) if da_mean[latn][0] > da_mean[latn][-1]
                         else slice(lat - 0.12, lat + 0.12),
                         lonn: slice(lon - 0.12, lon + 0.12)})
    if ablak.size == 0 or not np.isfinite(ablak.values).any():
        raise ValueError("üres cella-ablak")
    idx = np.unravel_index(np.nanargmax(ablak.values), ablak.shape)
    return float(ablak[latn].values[idx[ablak.dims.index(latn)]]), float(ablak[lonn].values[idx[ablak.dims.index(lonn)]])


def _folyo_ablak(da: xr.DataArray, lat: float, lon: float, latn: str, lonn: str) -> xr.DataArray:
    return da.sel({latn: slice(lat + 0.12, lat - 0.12) if da[latn][0] > da[latn][-1] else slice(lat - 0.12, lat + 0.12),
                   lonn: slice(lon - 0.12, lon + 0.12)})


def _glofas_csomag_osszegzes(da: xr.DataArray) -> dict:
    """Egy letöltött csomagból folyónként a cella-ablak havi összegei és darabszámai (kis JSON, commitolható)."""
    latn, lonn = lat_lon(da)
    tdim = dim_nev(da, ("valid_time", "time"))
    ki = {}
    for f, s, la, lo in FOLYOK:
        w = _folyo_ablak(da, la, lo, latn, lonn)
        honap = w[tdim].dt.month.rename("honap")
        osszeg = w.fillna(0).groupby(honap).sum(tdim).reindex(honap=range(1, 13), fill_value=0)
        db = w.notnull().groupby(honap).sum(tdim).reindex(honap=range(1, 13), fill_value=0)
        ki[f"{f}|{s}"] = {"lat": [round(float(x), 4) for x in w[latn].values], "lon": [round(float(x), 4) for x in w[lonn].values],
                          "osszeg": np.round(osszeg.transpose("honap", latn, lonn).values.astype(float), 3).tolist(),
                          "db": db.transpose("honap", latn, lonn).values.astype(int).tolist()}
    return ki


def glofas_klima(cl) -> dict:
    """GloFAS 1991–2020 havi klíma folyónként.

    Az EWDS kérésenként max. 500 mezőt enged, felhasználónként egyszerre egy kérést futtat, és a MARS-archívum
    sora néha órás. Ezért: minden GLOFAS_NAP_LEPES. nap (alapból 3.), több év/kérés; a kéréseket beküldjük, a
    job-azonosítókat és a kész csomagok összegzését az adat/cache/glofas_reszek/ mappába mentjük. Ha a türelmi idő
    lejár, kivételt dobunk (a vízhozam-rész klíma nélkül készül el), a következő futás pedig innen folytatja.
    """
    gy = CACHE / "glofas_klima_1991_2020.json"
    if gy.exists():
        return json.loads(gy.read_text(encoding="utf-8"))
    naplo("GloFAS klíma számítása (egyszeri, utána cache-ből)…")
    alap = glofas_valtozo(cl, "cems-glofas-historical", {"year": ["2000"]}, verzio=GLOFAS_KLIMA_VERZIO)
    verzio = alap.get("system_version", ["?"])[0]
    napok = [f"{d:02d}" for d in range(1, 32, GLOFAS_NAP_LEPES)]
    evek_kereskent = max(1, 480 // (len(napok) * 12))
    csomagok = [list(range(e, min(e + evek_kereskent, 2021))) for e in range(1991, 2021, evek_kereskent)]
    reszdir = CACHE / "glofas_reszek"
    reszdir.mkdir(parents=True, exist_ok=True)
    allapot_f = reszdir / "jobok.json"
    jobok = json.loads(allapot_f.read_text(encoding="utf-8")) if allapot_f.exists() else {}

    def kulcs(evek):
        return f"{verzio}_{GLOFAS_NAP_LEPES}_{evek[0]}_{evek[-1]}"

    def keres(evek):
        return dict(alap, year=[str(e) for e in evek], month=[f"{m:02d}" for m in range(1, 13)], day=napok,
                    area=FOLYO_AREA, data_format="netcdf", download_format="unarchived")

    def mentes():
        allapot_f.write_text(json.dumps(jobok, indent=1), encoding="utf-8")

    # Korábbi (pl. megszakadt) futás még sorban álló, azonos kéréseinek átvétele
    try:
        hianyzo_kulcsok = {kulcs(c): c for c in csomagok if kulcs(c) not in jobok}
        if hianyzo_kulcsok:
            for job in cl.get_jobs(limit=50, sortby="-created", status=["accepted", "running", "successful"]).json["jobs"]:
                if job.get("processID") != "cems-glofas-historical":
                    continue
                req = cl.get_remote(job["jobID"]).request
                for k, c in hianyzo_kulcsok.items():
                    if k not in jobok and _lista(req.get("year")) == [str(e) for e in c] \
                            and _lista(req.get("day")) == napok and _lista(req.get("system_version")) == [verzio] \
                            and [float(x) for x in req.get("area", [])] == [float(x) for x in FOLYO_AREA]:
                        jobok[k] = job["jobID"]
                        naplo(f"GloFAS historical {c[0]}–{c[-1]}: korábbi kérés átvéve ({job['status']})")
            mentes()
    except Exception as e:
        naplo(f"Korábbi GloFAS kérések lekérdezése nem sikerült: {e!r:.200}")

    hatarido = dt.datetime.now() + dt.timedelta(minutes=GLOFAS_KLIMA_TURELEM_PERC)
    probalkozas: dict[str, int] = {}
    while True:
        hianyzo = [c for c in csomagok if not (reszdir / f"{kulcs(c)}.json").exists()]
        if not hianyzo:
            break
        for c in hianyzo:
            k = kulcs(c)
            allapot = "nincs"
            if k in jobok:
                try:
                    remote = cl.get_remote(jobok[k])
                    allapot = remote.status
                except Exception:
                    allapot = "nincs"
            if allapot in ("accepted", "running"):
                continue
            if allapot == "successful":
                with tempfile.TemporaryDirectory() as tmp:
                    cel = pathlib.Path(tmp) / "gh.nc"
                    remote.download(str(cel))
                    if zipfile.is_zipfile(cel):
                        with zipfile.ZipFile(cel) as z:
                            nev = next(n for n in z.namelist() if n.endswith((".nc", ".grib", ".grib2")))
                            cel = cel.with_name(pathlib.Path(nev).name)
                            cel.write_bytes(z.read(nev))
                    da = adatvaltozo(megnyit(cel)).load()
                (reszdir / f"{k}.json").write_text(json.dumps(_glofas_csomag_osszegzes(da)), encoding="utf-8")
                naplo(f"GloFAS historical {c[0]}–{c[-1]} kész")
                continue
            # nincs még kérés, vagy elbukott / törölték: (újra)beküldés
            probalkozas[k] = probalkozas.get(k, 0) + 1
            if probalkozas[k] > 3:
                raise RuntimeError(f"GloFAS historical {c[0]}–{c[-1]}: a kérés többször is elbukott ({allapot})")
            if allapot != "nincs":
                naplo(f"GloFAS historical {c[0]}–{c[-1]}: állapot {allapot}, újraküldöm")
            jobok[k] = cl.submit("cems-glofas-historical", keres(c)).request_id
            mentes()
            naplo(f"GloFAS historical {c[0]}–{c[-1]} beküldve")
        if dt.datetime.now() > hatarido:
            kesz = len(csomagok) - len(hianyzo)
            raise RuntimeError(f"GloFAS klíma még nem készült el ({kesz}/{len(csomagok)} csomag kész, a többi kérés "
                               f"az EWDS sorában áll); a következő futás innen folytatja")
        time.sleep(60)

    # Összesítés: a cella a 30 éves átlag alapján a legnagyobb vízhozamú cella az ablakban
    reszek = [json.loads((reszdir / f"{kulcs(c)}.json").read_text(encoding="utf-8")) for c in csomagok]
    cellak, havi = {}, {}
    for f, s, _, _ in FOLYOK:
        k = f"{f}|{s}"
        osszeg = sum(np.array(r_[k]["osszeg"], dtype=float) for r_ in reszek)
        db = sum(np.array(r_[k]["db"], dtype=float) for r_ in reszek)
        with np.errstate(invalid="ignore", divide="ignore"):
            atlag = np.where(db.sum(0) > 0, osszeg.sum(0) / db.sum(0), np.nan)
            havi_atl = np.where(db > 0, osszeg / db, np.nan)
        i, j = np.unravel_index(np.nanargmax(atlag), atlag.shape)
        cellak[k] = [reszek[0][k]["lat"][i], reszek[0][k]["lon"][j]]
        havi[k] = [round(float(x), 1) for x in havi_atl[:, i, j]]
    napszam = int(sum(np.array(r_[f"{FOLYOK[0][0]}|{FOLYOK[0][1]}"]["db"]).max(axis=(1, 2)).sum() for r_ in reszek))
    klima = {"verzio": verzio, "cellak": cellak, "havi_atlag": havi,
             "mintavetel": f"minden {GLOFAS_NAP_LEPES}. nap, {napszam} nap 1991–2020"}
    gy.write_text(json.dumps(klima, ensure_ascii=False, indent=1), encoding="utf-8")
    for f in reszdir.iterdir():
        f.unlink()
    reszdir.rmdir()
    return klima


def glofas_szezonalis(ev: int, ho: int) -> list:
    cl = kliens(EWDS_URL, ["EWDS_KEY", "CDS_KEY", "CDSAPI_KEY"])
    # Előbb az előrejelzés (havonta kell), utána a klíma (egyszeri): az EWDS felhasználónként egyszerre
    # egy kérést futtat, így a klíma-kérések nem tartják fel az előrejelzést.
    alap = glofas_valtozo(cl, "cems-glofas-seasonal", {"year": [str(ev)], "month": [f"{ho:02d}"]})
    lepesek = list(range(24, 24 * 216, 24))  # 1–215. nap
    darab = 108  # az EWDS kérésenként max. 150 előretekintési lépést enged
    reszek = []
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(0, len(lepesek), darab):
            keres = dict(alap, year=[str(ev)], month=[f"{ho:02d}"],
                         leadtime_hour=[str(h) for h in lepesek[i:i + darab]], area=FOLYO_AREA,
                         data_format="netcdf", download_format="unarchived")
            reszek.append(adatvaltozo(megnyit(letolt(cl, "cems-glofas-seasonal", keres,
                                                     pathlib.Path(tmp) / f"gs_{i}.nc"))).load())
    elso_dim = dim_nev(reszek[0], ("forecast_period", "step", "valid_time", "time"))
    da = xr.concat(reszek, dim=elso_dim) if len(reszek) > 1 else reszek[0]
    try:
        klima = glofas_klima(cl)
    except Exception as e:  # a vízhozam-előrejelzés klíma nélkül is elkészül (klímához viszonyított mezők = null)
        hiba("GloFAS klíma", e)
        klima = {"cellak": {}, "havi_atlag": {}}
    latn, lonn = lat_lon(da)
    tag = dim_nev(da, ("number", "realization", "member"))
    tdim = dim_nev(da, ("valid_time", "time", "step", "forecast_period"))
    for d in ("forecast_reference_time",):
        if d in da.dims and da.sizes[d] == 1:
            da = da.isel({d: 0})
    if tdim == "step" or "valid_time" not in da.coords:
        ido = pd.Timestamp(dt.datetime(ev, ho, 1)) + pd.to_timedelta(da[tdim].values)
    else:
        ido = pd.to_datetime(da["valid_time"].values).ravel()
    sorok = []
    for f, s, la, lo in FOLYOK:
        k = f"{f}|{s}"
        try:
            clat, clon = klima["cellak"].get(k) or legnagyobb_folyo_cella(da.mean([tdim] + ([tag] if tag else [])), la, lo, latn, lonn)
            pont = da.sel({latn: clat, lonn: clon}, method="nearest")
            arr = np.asarray(pont.transpose(*( [tag] if tag else []), tdim).values, dtype=float)
            arr = arr.reshape(-1, len(ido))
            df = pd.DataFrame(arr.T, index=pd.DatetimeIndex(ido))
            elso_nap = float(np.nanmedian(arr[:, 0]))
            for (vev, vho), g in df.groupby([df.index.year, df.index.month]):
                if len(g) < calendar.monthrange(vev, vho)[1] - 1:
                    continue  # csak teljes hónap
                tagatlag = g.mean().values
                kl = (klima["havi_atlag"].get(k) or [None] * 12)[vho - 1]
                kl = kl if kl is not None and np.isfinite(kl) else None
                med = float(np.nanmedian(tagatlag))
                sorok.append({
                    "folyo": f, "szelveny": s, "honap": f"{vev}-{vho:02d}",
                    "median": r(med, 0), "p25": r(np.nanpercentile(tagatlag, 25), 0),
                    "p75": r(np.nanpercentile(tagatlag, 75), 0), "klima_atlag": r(kl, 0), "egyseg": "m³/s",
                    "elteres_szazalek": r((med / kl - 1) * 100, 1) if kl else None,
                    "p_klima_alatt": r(float(np.mean(tagatlag < kl))) if kl else None,
                    "elso_nap_median": r(elso_nap, 0), "cella": [r(clat, 3), r(clon, 3)],
                    "forras_id": "F-GLOFAS",
                })
        except Exception as e:
            hiba(f"GloFAS {k}", e)
    FORRASOK.append({"forras_id": "F-GLOFAS", "nev": "GloFAS szezonális vízhozam-előrejelzés (CEMS)",
                     "dataset": "cems-glofas-seasonal", "parameterek": alap, "inicializalas": f"{ev}-{ho:02d}",
                     "klima": (f"cems-glofas-historical {klima.get('verzio')} 1991–2020"
                               + (f" ({klima['mintavetel']})" if klima.get("mintavetel") else ""))
                              if klima.get("havi_atlag") else "nem elérhető (lásd meta.hibak)",
                     "licenc": "CEMS-FLOODS datasets licence",
                     "attribucio": "Copernicus Emergency Management Service – GloFAS"})
    return sorok


# --------------------------------------------------------------------------- 3. ENS heti (2 hét)
def era5_klima(cl) -> dict:
    gy = CACHE / "era5_klima_1991_2020.json"
    if gy.exists():
        return json.loads(gy.read_text(encoding="utf-8"))
    naplo("ERA5 klíma számítása (egyszeri, utána cache-ből)…")
    ki = {}
    with tempfile.TemporaryDirectory() as tmp:
        def pont_letolt(i: int) -> pathlib.Path:
            _, la, lo, _ = PONTOK[i]
            return letolt(cl, "reanalysis-era5-single-levels-timeseries", {
                "variable": ["2m_temperature", "total_precipitation"],
                "location": {"latitude": round(la * 4) / 4, "longitude": round(lo * 4) / 4},
                "date": ["1991-01-01/2020-12-31"], "data_format": "csv",
            }, pathlib.Path(tmp) / f"era5_{i}.csv")

        with ThreadPoolExecutor(PARHUZAMOS) as ex:
            fajlok = list(ex.map(pont_letolt, range(len(PONTOK))))
        for (nev, _, _, _), p in zip(PONTOK, fajlok):
            df = pd.read_csv(p)
            tcol = next(c for c in df.columns if "time" in c.lower() or "date" in c.lower())
            t2m = next(c for c in df.columns if c.lower() in ("t2m", "2m_temperature") or "t2m" in c.lower())
            tp = next(c for c in df.columns if c.lower() in ("tp", "total_precipitation"))
            df[tcol] = pd.to_datetime(df[tcol])
            df = df.set_index(tcol)
            t = df[t2m].astype(float)
            t = t[t.index.hour.isin([0, 12])] - 273.15  # ugyanazok az órák, mint az ENS-ből
            nap_t = t.groupby(t.index.dayofyear).mean()
            nap_p = (df[tp].astype(float).resample("D").sum() * 1000.0)
            nap_p = nap_p.groupby(nap_p.index.dayofyear).mean()
            # 15 napos simítás körkörösen
            def simit(s):
                s = s.reindex(range(1, 367)).interpolate(limit_direction="both")
                return pd.concat([s.iloc[-7:], s, s.iloc[:7]]).rolling(15, center=True).mean().iloc[7:-7]
            ki[nev] = {"t_napi": list(np.round(simit(nap_t).values, 2)),
                       "p_napi": list(np.round(simit(nap_p).values, 3))}
    CACHE.mkdir(parents=True, exist_ok=True)
    gy.write_text(json.dumps(ki, ensure_ascii=False), encoding="utf-8")
    return ki


def ens_letolt(cel: pathlib.Path) -> tuple[xr.Dataset, xr.Dataset, dt.datetime]:
    from ecmwf.opendata import Client

    # Forrás: az ECMWF saját szervere a leggyorsabb; az AWS tükör sok byte-range kérésnél 503 SlowDown-t ad.
    forrasok = [s for s in os.environ.get("ECMWF_OPENDATA_SOURCE", "ecmwf,azure,google,aws").split(",") if s]
    lepesek_t = list(range(12, 337, 12))
    kw = {"engine": "cfgrib", "backend_kwargs": {"indexpath": ""}}
    utolso_hiba: Exception | None = None
    for forras in forrasok:
        try:
            c = Client(source=forras)
            # A nyílt ENS-ben jelenleg csak a 50 perturbált tag (pf) van; a cf-et csak akkor vesszük, ha létezik.
            # (A type=["cf","pf"] listából a kliens csak a pf-et töltötte le.)
            res = c.retrieve(time=0, stream="enfo", type="pf", param="2t", step=lepesek_t, target=str(cel / "t.grib2"))
            c.retrieve(date=res.datetime, time=0, stream="enfo", type="pf", param="tp",
                       step=[168, 336], target=str(cel / "p.grib2"))
            van_cf = True
            try:
                c.retrieve(date=res.datetime, time=0, stream="enfo", type="cf", param="2t", step=lepesek_t,
                           target=str(cel / "t_cf.grib2"))
                c.retrieve(date=res.datetime, time=0, stream="enfo", type="cf", param="tp", step=[168, 336],
                           target=str(cel / "p_cf.grib2"))
            except Exception:
                van_cf = False
            break
        except Exception as e:
            utolso_hiba = e
            naplo(f"ENS letöltés ({forras}) sikertelen: {e!r:.300}")
    else:
        raise RuntimeError(f"ENS letöltés minden forrásból sikertelen: {utolso_hiba!r}")

    def tagok(nev):
        pf = xr.open_dataset(cel / f"{nev}.grib2", **kw)
        if "number" not in pf.dims:
            pf = pf.expand_dims("number")
        if not van_cf:
            return pf
        cf = xr.open_dataset(cel / f"{nev}_cf.grib2", **kw)
        if "number" not in cf.dims:
            cf = cf.expand_dims("number")
        return xr.concat([cf, pf], dim="number", coords="minimal", compat="override")

    t, p = tagok("t"), tagok("p")
    return t, p, res.datetime


def ens_heti(cl_cds, letolto=ens_letolt) -> list:
    klima = era5_klima(cl_cds)
    with tempfile.TemporaryDirectory() as tmp:
        t_ds, p_ds, indulas = letolto(pathlib.Path(tmp))

        def kivag(da: xr.DataArray) -> xr.DataArray:  # csak Magyarország környéke kerül memóriába
            la, lo = lat_lon(da)
            n, w, s_, e = HU_AREA
            lat_sl = slice(n, s_) if da[la][0] > da[la][-1] else slice(s_, n)
            return da.sel({la: lat_sl, lo: slice(w, e)})

        t = kivag(adatvaltozo(t_ds)).load() - 273.15
        p = kivag(adatvaltozo(p_ds)).load() * 1000.0
    latn, lonn = lat_lon(t)
    sorok = []
    for het in (1, 2):
        kezd = indulas + dt.timedelta(days=7 * (het - 1))
        veg = kezd + dt.timedelta(days=7)
        lepesek = [s for s in range(12, 337, 12) if 168 * (het - 1) < s <= 168 * het]
        napok = [(kezd + dt.timedelta(days=i)).timetuple().tm_yday for i in range(7)]
        for valtozo in ("homerseklet", "csapadek"):
            pont_anom, pont_p = {}, {}
            for nev, la, lo, reg in PONTOK:
                if valtozo == "homerseklet":
                    x = t.sel({latn: la, lonn: lo}, method="nearest").sel(step=[pd.Timedelta(hours=s) for s in lepesek])
                    tagok = x.mean("step").values.ravel()
                    kl = float(np.mean([klima[nev]["t_napi"][min(d, 366) - 1] for d in napok]))
                else:
                    x = p.sel({latn: la, lonn: lo}, method="nearest")
                    v = x.sel(step=pd.Timedelta(hours=168 * het)).values.ravel()
                    v0 = x.sel(step=pd.Timedelta(hours=168 * (het - 1))).values.ravel() if het > 1 else 0.0
                    tagok = v - v0
                    kl = float(np.sum([klima[nev]["p_napi"][min(d, 366) - 1] for d in napok]))
                tagok = tagok[np.isfinite(tagok)]
                pont_anom[nev] = float(tagok.mean() - kl)
                pont_p[nev] = float((tagok > kl).mean())
            for terulet in ["Magyarország"] + REGIOK:
                nevek = [p_[0] for p_ in PONTOK if terulet == "Magyarország" or p_[3] == terulet]
                pr = float(np.mean([pont_p[n] for n in nevek]))
                sorok.append({
                    "het": het, "het_kezdete": kezd.strftime("%Y-%m-%d"),
                    "het_vege": (veg - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
                    "terulet": terulet, "valtozo": valtozo,
                    "anomalia": r(np.mean([pont_anom[n] for n in nevek]), 1),
                    "egyseg": "°C" if valtozo == "homerseklet" else "mm/hét",
                    "p_atlag_felett": r(pr), "p_atlag_alatt": r(1 - pr), "forras_id": "F-ENS",
                })
    tagszam = int(t.sizes.get("number", 1))
    FORRASOK.append({"forras_id": "F-ENS", "nev": f"ECMWF nyílt ENS ({tagszam} tag, 15 nap)",
                     "inicializalas": indulas.strftime("%Y-%m-%d %H UTC"),
                     "modszer": "Nyers ensemble a ERA5 1991–2020 napi klímához (00/12 UTC) mérve; modellhiba nincs korrigálva",
                     "licenc": "CC BY 4.0 (ECMWF open data); ERA5: Copernicus licence",
                     "attribucio": "ECMWF open data; Copernicus Climate Change Service (ERA5)"})
    return sorok


# --------------------------------------------------------------------------- futtatás
def main() -> int:
    most = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    ev, ho = int(os.environ.get("INIT_EV", most.year)), int(os.environ.get("INIT_HO", most.month))
    ki = {"meta": {}, "havi": [], "havi_rendszerenkent": [], "heti": [], "vizhozam": []}
    reszek = {}

    # A három rész más-más szolgáltatást használ (CDS, EWDS, ECMWF open data), ezért párhuzamosan futnak.
    def havi_resz():
        ki["havi"], ki["havi_rendszerenkent"] = c3s_havi(ev, ho)
        return True

    def vizhozam_resz():
        ki["vizhozam"] = glofas_szezonalis(ev, ho)
        return bool(ki["vizhozam"])

    def heti_resz():
        ki["heti"] = ens_heti(kliens(CDS_URL, ["CDS_KEY", "CDSAPI_KEY"]))
        return True

    feladatok = {"havi": ("C3S havi", havi_resz), "vizhozam": ("GloFAS", vizhozam_resz)}
    if os.environ.get("HETI", "1") == "1":
        feladatok["heti"] = ("ENS heti", heti_resz)
    # Teljes futási határidő: ha egy rész (pl. az EWDS sora miatt) nem végez időben, a kész részekkel írjuk ki
    # az eredményt, mielőtt a GitHub Actions job-időkorlátja (300 perc) mindent leállítana.
    hatarido = dt.datetime.now() + dt.timedelta(minutes=float(os.environ.get("MAX_FUTAS_PERC", "270")))
    ex = ThreadPoolExecutor(len(feladatok))
    jovok = {nev: ex.submit(fv) for nev, (_, fv) in feladatok.items()}
    idotullepes = False
    for nev, jovo in jovok.items():
        try:
            reszek[nev] = jovo.result(timeout=max(1.0, (hatarido - dt.datetime.now()).total_seconds()))
        except FuturesTimeout:
            idotullepes = True
            hiba(feladatok[nev][0], "időtúllépés: nem végzett a futási határidőig (MAX_FUTAS_PERC); "
                                    "a következő futás folytatja"); reszek[nev] = False
        except Exception as e:
            hiba(feladatok[nev][0], e); reszek[nev] = False
    ex.shutdown(wait=not idotullepes, cancel_futures=True)
    if idotullepes:  # a félbemaradt rész eredménye ne kerüljön be félkészen
        for nev, ertek in reszek.items():
            if not ertek:
                ki[nev] = []
                if nev == "havi":
                    ki["havi_rendszerenkent"] = []
    sorrend = {"F-C3S": 0, "F-GLOFAS": 1, "F-ENS": 2}
    FORRASOK.sort(key=lambda f: sorrend.get(f.get("forras_id"), 9))
    ki["meta"] = {
        "riport": "Energofish szezonális numerikus adatréteg", "sema_verzio": "1.0",
        "inicializalas_honap": f"{ev}-{ho:02d}", "generalva_utc": most.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reszek_sikeresek": reszek, "pontok": [{"nev": n, "lat": la, "lon": lo, "regio": rg} for n, la, lo, rg in PONTOK],
        "megjegyzes": ("Az anomáliák a modellek saját hindcast-klímájához (C3S), a GloFAS 1991–2020 reanalízishez, "
                       "illetve az ERA5 1991–2020 klímához (ENS) viszonyítanak. Az elotav_honap 0 = az indítás hónapja."),
        "adatforrasok": FORRASOK, "hibak": HIBAK,
    }
    ADAT.mkdir(parents=True, exist_ok=True)
    szoveg = json.dumps(ki, ensure_ascii=False, indent=1)
    (ADAT / f"szezonalis_numerikus_{ev}-{ho:02d}.json").write_text(szoveg, encoding="utf-8")
    (ADAT / "latest.json").write_text(szoveg, encoding="utf-8")
    naplo(f"Kész. Részek: {reszek}; hibák: {len(HIBAK)}")
    kod = 0 if any(reszek.values()) else 1
    if idotullepes:  # a még futó letöltő szálak ne tartsák életben a folyamatot
        sys.stdout.flush(); sys.stderr.flush()
        os._exit(kod)
    return kod


if __name__ == "__main__":
    sys.exit(main())
