"""BestChange rate watcher -> Telegram.

Берёт официальную выгрузку BestChange (api.bestchange.ru/info.zip, обновляется ~раз в минуту),
проверяет правила из config.json и шлёт в Telegram, когда курс >= порога.

Запуск:
  python watcher.py                  # бесконечный цикл с интервалом из конфига
  python watcher.py --once           # одна проверка (для Планировщика задач)
  python watcher.py --find usdc      # найти точное название валюты
  python watcher.py --top "USDC BEP20 (USDC)" "Т-Банк RUB"   # топ обменников по паре
  python watcher.py --chat-id        # узнать свой chat_id (сначала напишите боту /start)
"""
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "config.json"
STATE = HERE / "state.json"
INFO_URL = "http://api.bestchange.ru/info.zip"
ENC = "cp1251"


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def fetch_zip():
    with urllib.request.urlopen(INFO_URL, timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read()))


def read_lines(zf, name):
    with zf.open(name) as f:
        for line in io.TextIOWrapper(f, encoding=ENC, errors="replace"):
            yield line.rstrip("\r\n").split(";")


def currencies(zf):
    # id;pos;полное имя;короткое имя;код валюты;...
    return {int(p[0]): (p[2], p[3]) for p in read_lines(zf, "bm_cy.dat")}


def exchangers(zf):
    # id;имя;...
    return {int(p[0]): p[1] for p in read_lines(zf, "bm_exch.dat")}


def resolve_currency(cy, query):
    if isinstance(query, int) or str(query).isdigit():
        return int(query)
    q = str(query).strip().lower()
    hits = [i for i, (full, short) in cy.items() if q in (full.lower(), short.lower())]
    if len(hits) != 1:
        raise SystemExit(f"Валюта '{query}': найдено {len(hits)}. Уточните через --find.")
    return hits[0]


def pair_rates(zf, from_id, to_id):
    """{exch_id: (курс = сколько получаю за 1 отдаваемую, резерв, min, max)}"""
    prefix = f"{from_id};{to_id};"
    out = {}
    with zf.open("bm_rates.dat") as f:
        for raw in f:
            line = raw.decode(ENC, "replace")
            if not line.startswith(prefix):
                continue
            p = line.strip().split(";")
            give, recv = float(p[3]), float(p[4])
            out[int(p[2])] = (recv / give, float(p[5]), float(p[8]), float(p[9]))
    return out


def tg_send(cfg, text):
    tg = cfg["telegram"]
    data = urllib.parse.urlencode({"chat_id": tg["chat_id"], "text": text}).encode()
    url = f"https://api.telegram.org/bot{tg['token']}/sendMessage"
    urllib.request.urlopen(url, data=data, timeout=30).read()


def check(cfg):
    zf = fetch_zip()
    cy, ex = currencies(zf), exchangers(zf)
    state = load_json(STATE, {})
    for rule in cfg["rules"]:
        fid = resolve_currency(cy, rule["from"])
        tid = resolve_currency(cy, rule["to"])
        wanted = {n.lower() for n in rule.get("exchangers", [])}
        rates = pair_rates(zf, fid, tid)
        for eid, (rate, reserve, mn, mx) in rates.items():
            name = ex.get(eid, str(eid))
            if wanted and name.lower() not in wanted:
                continue
            key = f"{fid}-{tid}-{eid}-{rule['min_rate']}"
            hit = rate >= rule["min_rate"]
            if hit and not state.get(key):
                tg_send(cfg, (
                    f"✅ {name}: {cy[fid][1]} → {cy[tid][1]}\n"
                    f"Курс {rate:.4f} (порог {rule['min_rate']})\n"
                    f"Резерв {reserve:,.0f} | лимит {mn:g}–{mx:g}\n"
                    f"https://www.bestchange.ru/click.php?id={eid}&from={fid}&to={tid}"
                ))
                print(f"[ALERT] {name} {rate:.4f} >= {rule['min_rate']}")
            state[key] = hit  # повторное уведомление только после падения ниже порога
        if wanted:
            missing = wanted - {ex.get(e, "").lower() for e in rates}
            if missing:
                print(f"[info] нет в паре сейчас: {', '.join(sorted(missing))}")
    STATE.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    print(time.strftime("%H:%M:%S"), "проверено")


def main():
    args = sys.argv[1:]
    # В облаке (GitHub Actions) весь конфиг лежит в секрете WATCHER_CONFIG
    env_cfg = os.environ.get("WATCHER_CONFIG")
    cfg = json.loads(env_cfg) if env_cfg else load_json(CONFIG, None)
    if args[:1] == ["--find"]:
        q = args[1].lower()
        for i, (full, short) in sorted(currencies(fetch_zip()).items()):
            if q in full.lower() or q in short.lower():
                print(f"{i}\t{full}\t|\t{short}")
        return
    if args[:1] == ["--top"]:
        zf = fetch_zip()
        cy, ex = currencies(zf), exchangers(zf)
        rates = pair_rates(zf, resolve_currency(cy, args[1]), resolve_currency(cy, args[2]))
        for eid, (rate, reserve, mn, mx) in sorted(rates.items(), key=lambda x: -x[1][0])[:20]:
            print(f"{rate:.4f}\t{ex.get(eid, eid)}\tрезерв {reserve:,.0f}\tлимит {mn:g}–{mx:g}")
        return
    if args[:1] == ["--chat-id"]:
        url = f"https://api.telegram.org/bot{cfg['telegram']['token']}/getUpdates"
        for u in json.loads(urllib.request.urlopen(url, timeout=30).read())["result"]:
            chat = (u.get("message") or {}).get("chat")
            if chat:
                print(chat["id"], chat.get("username") or chat.get("title"))
        return
    if cfg is None:
        raise SystemExit("Нет config.json — скопируйте config.example.json")
    if args[:1] == ["--once"]:
        check(cfg)
        return
    while True:
        try:
            check(cfg)
        except Exception as e:  # сеть/битый архив — не роняем цикл
            print("[error]", e)
        time.sleep(cfg.get("interval_sec", 60))


if __name__ == "__main__":
    main()
