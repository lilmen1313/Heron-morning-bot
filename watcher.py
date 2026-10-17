"""BestChange rate watcher -> Telegram.

Берёт официальную выгрузку BestChange (api.bestchange.ru/info.zip, обновляется ~раз в минуту),
проверяет правила из config.json и шлёт в Telegram, когда курс >= порога.
Раз в день присылает сводку: лучший курс, его место за 30 дней, наценка к курсу ЦБ, ближайшие события.

Правила берутся из опубликованной Google Таблицы (CSV-ссылка в RULES_URL или "rules_url"),
без неё — из "rules" в config.json.

Запуск:
  python watcher.py                  # бесконечный цикл с интервалом из конфига
  python watcher.py --once           # одна проверка (для Планировщика задач)
  python watcher.py --summary        # показать сводку по текущим данным (без отправки)
  python watcher.py --rules [ссылка] # проверить таблицу правил: что прочитано и что не так
  python watcher.py --find usdc      # найти точное название валюты
  python watcher.py --top "USDC BEP20 (USDC)" "Т-Банк RUB"   # топ обменников по паре
  python watcher.py --chat-id        # узнать свой chat_id (сначала напишите боту /start)
"""
import csv
import http.client
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "config.json"
STATE = HERE / "state.json"
INFO_URL = "http://api.bestchange.ru/info.zip"
ENC = "cp1251"
MSK = timezone(timedelta(hours=3))
FETCH_ATTEMPTS = 3       # разовый таймаут BestChange — не повод для тревоги
FETCH_PAUSE_SEC = 10
ERROR_REPEAT_SEC = 3600  # предупреждение о сбое — не чаще раза в час
HISTORY_DAYS = 60        # сколько дней истории курсов хранить
CBR_RETRY_SEC = 3600     # курс ЦБ не ответил — следующая попытка через час
CBR_JSON = "https://www.cbr-xml-daily.ru/daily_json.js"
CBR_XML = "https://www.cbr.ru/scripts/XML_daily.asp"
# Заседания ЦБ по ключевой ставке. Обновлять раз в год (график выходит в сентябре);
# переопределяется в конфиге: "summary": {"cbr_meetings": [...]}
CBR_MEETINGS = [
    "2026-10-23", "2026-12-18",
    "2027-02-12", "2027-03-19", "2027-04-30", "2027-06-11",
    "2027-07-30", "2027-09-10", "2027-10-22", "2027-12-17",
]
HOLIDAYS = {(1, d) for d in range(1, 9)} | {(2, 23), (3, 8), (5, 1), (5, 9), (6, 12), (11, 4)}


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def fetch_zip():
    last = None
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(INFO_URL, timeout=60) as r:
                return zipfile.ZipFile(io.BytesIO(r.read()))
        except (OSError, http.client.HTTPException, zipfile.BadZipFile) as e:
            last = e
            print(f"[warn] BestChange, попытка {attempt}: {e}")
            if attempt < FETCH_ATTEMPTS:
                time.sleep(FETCH_PAUSE_SEC)
    raise RuntimeError(f"BestChange недоступен ({FETCH_ATTEMPTS} попытки): {last}")


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
        raise ValueError(f"Валюта '{query}': найдено {len(hits)}. Уточните через --find.")
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
    try:
        urllib.request.urlopen(url, data=data, timeout=30).read()
    except urllib.error.HTTPError as e:
        # в ответе Telegram причина словами: «bot was blocked by the user» и т.п.
        try:
            reason = json.loads(e.read()).get("description") or e.reason
        except Exception:
            reason = e.reason
        raise RuntimeError(f"Telegram отказал: {e.code} {reason}") from None
    except OSError as e:
        raise RuntimeError(f"Telegram недоступен: {e}") from None


def describe(e):
    return str(e) if isinstance(e, (RuntimeError, ValueError)) else f"{type(e).__name__}: {e}"


RULES_OFF = {"нет", "false", "ложь", "0", "выкл", "-"}


def parse_rules_csv(text):
    """CSV таблицы правил → (правила, проблемы). Колонки ищутся по заголовку, порядок любой.
    Номера строк — как в таблице, чтобы по сообщению сразу найти строку."""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise ValueError("таблица правил пустая")
    head = [h.strip("﻿ \t").lower() for h in rows[0]]

    def col(prefix):
        return next((i for i, h in enumerate(head) if h.startswith(prefix)), None)

    c = {"on": col("вкл"), "from": col("отда"), "to": col("получа"), "ex": col("обменник"), "min": col("порог")}
    absent = [n for n, k in (("отдаю", "from"), ("получаю", "to"), ("порог", "min")) if c[k] is None]
    if absent:
        raise ValueError(f"в таблице правил нет колонок: {', '.join(absent)}")
    rules, problems = [], []
    for n, row in enumerate(rows[1:], 2):
        def get(k):
            return row[c[k]].strip() if c[k] is not None and c[k] < len(row) else ""
        if not any(x.strip() for x in row) or get("on").lower() in RULES_OFF:
            continue
        label = f"строка {n} таблицы"
        if not get("from") or not get("to"):
            problems.append(f"{label}: не заполнено «отдаю» или «получаю»")
            continue
        raw = get("min")
        try:
            rate = float(raw.replace(" ", "").replace(" ", "").replace(",", "."))
        except ValueError:
            problems.append(f"{label}: порог «{raw}» — не число")
            continue
        exch = [e.strip() for e in re.split(r"[,;\n]", get("ex")) if e.strip()]
        rules.append({"from": get("from"), "to": get("to"), "exchangers": exch,
                      "min_rate": rate, "_label": label})
    return rules, problems


def rules_url(cfg):
    return os.environ.get("RULES_URL") or cfg.get("rules_url")


def load_rules(cfg, state):
    """(правила, проблемы). Таблица не ответила — проверяем по последним полученным из неё
    правилам (state["_rules"]), а не стоим: сбой Google не должен выключать уведомления."""
    url = rules_url(cfg)
    if not url:
        return cfg.get("rules", []), []
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            text = r.read().decode("utf-8-sig")
        if text.lstrip().startswith("<"):
            raise RuntimeError("по ссылке веб-страница, а не CSV: таблица не опубликована "
                               "или выбран не тот формат")
        rules, problems = parse_rules_csv(text)
    except Exception as e:
        msg = f"таблица правил недоступна ({describe(e)})"
        if state.get("_rules") is not None:
            return state["_rules"], [msg + " — проверяю по последним полученным правилам"]
        if cfg.get("rules"):
            return cfg["rules"], [msg + " — проверяю по правилам из config.json"]
        raise RuntimeError(msg) from None
    state["_rules"] = rules
    return rules, problems


def summary_cfg(cfg, rules):
    """Настройки сводки с умолчаниями; None — сводка выключена ("summary": false)."""
    sm = cfg.get("summary", {})
    if sm is False:
        return None
    return {
        "at": sm.get("at", "10:00"),
        "from": sm.get("from", rules[0]["from"] if rules else None),
        "cbr_meetings": sm.get("cbr_meetings", CBR_MEETINGS),
    }


def check(cfg, rules, state, ctx):
    """Проверяет все правила. Возвращает список проблем (пустой — всё в порядке).
    В ctx складывает курсы для сводки: obs {(обменник, банк): курс}, missing, from_name."""
    zf = fetch_zip()
    cy, ex = currencies(zf), exchangers(zf)
    known = {n.lower() for n in ex.values()}
    sm = summary_cfg(cfg, rules)
    try:
        sum_fid = resolve_currency(cy, sm["from"]) if sm and sm["from"] is not None else None
    except ValueError:
        sum_fid = None  # битое имя всплывёт ниже как проблема правила
    obs, missing_all = {}, set()
    problems = []
    for i, rule in enumerate(rules, 1):
        label = rule.get("_label", f"правило {i}")
        try:
            fid = resolve_currency(cy, rule["from"])
            tid = resolve_currency(cy, rule["to"])
            wanted = {n.lower() for n in rule.get("exchangers", [])}
            unknown = wanted - known
            if unknown:  # опечатка или обменник ушёл с BestChange — правило молча не сработает
                problems.append(f"{label}: нет на BestChange обменника {', '.join(sorted(unknown))}")
            rates = pair_rates(zf, fid, tid)
            for eid, (rate, reserve, mn, mx) in rates.items():
                name = ex.get(eid, str(eid))
                if wanted and name.lower() not in wanted:
                    continue
                if fid == sum_fid:
                    obs[(name, cy[tid][0].removesuffix(" RUB"))] = rate
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
            missing = wanted - unknown - {ex.get(e, "").lower() for e in rates}
            if missing:
                print(f"[info] {label}, нет в паре сейчас: {', '.join(sorted(missing))}")
            if fid == sum_fid:
                names = {n.lower(): n for n in rule.get("exchangers", [])}
                missing_all |= {names[m] for m in missing}
        except Exception as e:  # одно битое правило не должно глушить остальные
            problems.append(f"{label}: {describe(e)}")
    if sum_fid is not None:
        ctx.update(obs=obs, from_name=cy[sum_fid][1],
                   missing=missing_all - {name for name, _ in obs})
    return problems


# ---------- история курсов и сводка ----------

def fetch_cbr_usd():
    """Курс доллара ЦБ на сегодня. Два источника: зеркало и сам cbr.ru."""
    errors = []
    try:
        with urllib.request.urlopen(CBR_JSON, timeout=20) as r:
            return float(json.loads(r.read())["Valute"]["USD"]["Value"])
    except Exception as e:
        errors.append(f"cbr-xml-daily.ru: {describe(e)}")
    try:
        req = urllib.request.Request(CBR_XML, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            v = ET.fromstring(r.read()).find("Valute[@ID='R01235']")
        return float(v.findtext("Value").replace(",", ".")) / float(v.findtext("Nominal"))
    except Exception as e:
        errors.append(f"cbr.ru: {describe(e)}")
    raise RuntimeError("; ".join(errors))


def record(state, day, obs, now):
    """Дневные итоги [макс, мин, сумма, число замеров]: по паре, по обменнику (@) и лучший (_best)."""
    hist = state.setdefault("_hist", {})
    d = hist.setdefault(day, {"r": {}})

    def add(k, v):
        a = d["r"].get(k)
        d["r"][k] = [v, v, round(v, 4), 1] if not a else \
            [max(a[0], v), min(a[1], v), round(a[2] + v, 4), a[3] + 1]

    per_ex = {}
    for (name, bank), rate in obs.items():
        add(f"{name}|{bank}", rate)
        per_ex[name] = max(per_ex.get(name, 0), rate)
    for name, rate in per_ex.items():
        add("@" + name, rate)
    if obs:
        add("_best", max(obs.values()))
    if "cbr" not in d and now - state.get("_cbr_try", 0) >= CBR_RETRY_SEC:
        state["_cbr_try"] = now
        try:
            d["cbr"] = round(fetch_cbr_usd(), 4)
            state.pop("_cbr_err", None)
        except Exception as e:
            state["_cbr_err"] = describe(e)[:300]
            print("[warn] курс ЦБ:", state["_cbr_err"])
    for old in sorted(hist)[:-HISTORY_DAYS]:
        del hist[old]


def day_avg(hist, day, key):
    a = hist.get(day, {}).get("r", {}).get(key)
    return a[2] / a[3] if a else None


def usual_range(vals):
    """«Обычный» разброс: без 10% самых низких и 10% самых высоких. Нужно ≥ 5 дней."""
    if len(vals) < 5:
        return None
    v = sorted(vals)
    k = len(v) // 10
    v = v[k:len(v) - k]
    return v[0], v[-1]


def pct(x):
    return f"{x:+.1f}%".replace(".", ",")


def upcoming_events(today, meetings, horizon=7):
    """События ближайших дней — не прогноз, а «в эти дни возможны движения»."""
    ev = [(date.fromisoformat(s), "заседание ЦБ по ключевой ставке") for s in meetings]
    for k in range(2):  # этот и следующий месяц
        y, m = today.year + (today.month + k - 1) // 12, (today.month + k - 1) % 12 + 1
        tax = date(y, m, 28)
        while tax.weekday() >= 5:
            tax += timedelta(days=1)
        ev.append((tax, "уплата налогов: экспортёры продают валюту"))
        d, n = date(y, m, 1), 0  # 3-й рабочий день; переносы праздников не учтены — отсюда «≈»
        while True:
            if d.weekday() < 5 and (d.month, d.day) not in HOLIDAYS:
                n += 1
                if n == 3:
                    break
            d += timedelta(days=1)
        ev.append((d, "≈ Минфин объявляет покупку/продажу валюты"))
    out = []
    for d, what in sorted(ev):
        days = (d - today).days
        if 0 <= days <= horizon:
            when = "сегодня" if days == 0 else "завтра" if days == 1 else f"через {days} дн."
            out.append(f"⚠️ {d:%d.%m} ({when}) — {what}")
    if not any(date.fromisoformat(s) >= today for s in meetings):
        out.append("⚙️ Даты заседаний ЦБ закончились — обновите список в настройках")
    return out


def build_summary(state, ctx, today, meetings):
    hist = state.get("_hist", {})
    past = [d for d in sorted(hist) if d < today.isoformat()][-30:]
    # курс ЦБ за сегодня; не получен — последний известный (за день он меняется мало)
    cbr_day = next((d for d in sorted(hist, reverse=True)
                    if d <= today.isoformat() and "cbr" in hist[d]), None)
    cbr = hist[cbr_day]["cbr"] if cbr_day else None
    cbr_label = "" if cbr_day == today.isoformat() else f" на {cbr_day[8:10]}.{cbr_day[5:7]}" if cbr_day else ""
    obs = ctx["obs"]
    lines = [f"📊 Сводка {today:%d.%m} · {ctx['from_name']}"]
    if not obs:
        lines.append("Сейчас ни одного вашего обменника в паре")
    else:
        (bname, bbank), best = max(obs.items(), key=lambda x: x[1])
        lines.append(f"Лучший курс: {best:.2f} ({bname}, {bbank})")
        vals = [v for v in (day_avg(hist, d, "_best") for d in past) if v is not None]
        if len(vals) >= 3:
            label = "За 30 дней" if len(vals) >= 30 else f"За {len(vals)} дн."
            lines.append(f"{label}: выше, чем в {round(100 * sum(v < best for v in vals) / len(vals))}% дней")
        else:
            lines.append(f"Сравнение с прошлыми днями — после 3 дней истории (сейчас {len(vals)})")

        def markups(key):
            return [day_avg(hist, d, key) / hist[d]["cbr"] * 100 - 100
                    for d in past if "cbr" in hist[d] and day_avg(hist, d, key) is not None]

        def usual(key):
            r = usual_range(markups(key))
            return f" (обычно {pct(r[0]).rstrip('%')}…{pct(r[1])})" if r else ""

        if cbr:
            lines.append(f"К курсу ЦБ{cbr_label} ({cbr:.2f}): {pct(best / cbr * 100 - 100)}{usual('_best')}")
        else:
            lines.append(f"Курс ЦБ: нет данных ({state.get('_cbr_err', 'ещё не получен')})")
        per_ex = {}
        for (name, bank), rate in obs.items():
            if rate > per_ex.get(name, (0, ""))[0]:
                per_ex[name] = (rate, bank)
        lines.append("")
        for name, (rate, bank) in sorted(per_ex.items(), key=lambda x: -x[1][0])[:8]:
            m = f" · {pct(rate / cbr * 100 - 100)}{usual('@' + name)}" if cbr else ""
            lines.append(f"{name}: {rate:.2f} ({bank}){m}")
    for name in sorted(ctx.get("missing", ())):
        lines.append(f"{name}: нет в паре")
    events = upcoming_events(today, meetings)
    if events:
        lines.append("")
        lines.extend(events)
    if len(past) < 30:
        lines.append(f"\nИстория курсов: {len(past)} из 30 дней")
    return "\n".join(lines)


# ---------- запуск ----------

def fmt_msk(ts):
    return datetime.fromtimestamp(ts, MSK).strftime("%d.%m %H:%M")


def report_health(cfg, state, problems, now):
    """Сбой → предупреждение в Telegram (не чаще раза в час); восстановление → одно сообщение."""
    err = state.get("_error")
    if problems:
        err = err or {"since": now, "alerted": 0}
        if now - err["alerted"] >= ERROR_REPEAT_SEC:
            text = "\n".join(f"• {p}" for p in problems)[:3000]
            try:
                tg_send(cfg, f"⚠️ Проверка не прошла (сбой с {fmt_msk(err['since'])} МСК):\n{text}")
                err["alerted"] = now
            except Exception as e:
                print("[error] предупреждение не отправлено:", describe(e))
        state["_error"] = err
    elif err:
        if err["alerted"]:
            try:
                tg_send(cfg, f"🟢 Проверки снова идут. Сбой длился с {fmt_msk(err['since'])} до {fmt_msk(now)} МСК")
            except Exception as e:
                print("[error] сообщение о восстановлении не отправлено:", describe(e))
                return  # попробуем при следующей проверке
        del state["_error"]


def load_state():
    """(состояние, восстановилось ли оно). Нет файла или он битый — не восстановилось."""
    try:
        return json.loads(STATE.read_text(encoding="utf-8")), True
    except (FileNotFoundError, ValueError):
        return {}, False


def run_once(cfg, now=None):
    """Одна проверка с отчётом о сбоях и сводкой. Возвращает True, если всё прошло."""
    now = time.time() if now is None else now
    state, restored = load_state()
    problems = []
    # В GitHub состояние приезжает из кэша. Не приехало — не затираем историю пустым файлом:
    # без state.json workflow не сохранит кэш, и следующий запуск возьмёт прежний.
    persist = restored or os.environ.get("GITHUB_ACTIONS") != "true" \
        or os.environ.get("RESET_STATE") == "true"
    if not persist:
        problems.append("состояние бота не восстановилось из кэша GitHub — история курсов в этот раз "
                        "не сохраняется. Разовый сбой пройдёт сам; если повторяется больше часа — "
                        "запустите workflow вручную с галочкой reset_state")
    ctx, rules = {}, []
    try:
        rules, rule_problems = load_rules(cfg, state)
        problems += rule_problems
        if not rules:
            print("[info] нет включённых правил — проверять нечего, сводки не будет")
        problems += check(cfg, rules, state, ctx)
    except Exception as e:
        problems.append(describe(e))
    if persist and "obs" in ctx:
        msk = datetime.fromtimestamp(now, MSK)
        record(state, msk.date().isoformat(), ctx["obs"], now)
        sm = summary_cfg(cfg, rules)
        force = os.environ.get("FORCE_SUMMARY") == "true"
        if sm and (force or (state.get("_summary_day") != msk.date().isoformat()
                             and msk.strftime("%H:%M") >= sm["at"])):
            try:
                tg_send(cfg, build_summary(state, ctx, msk.date(), sm["cbr_meetings"]))
                state["_summary_day"] = msk.date().isoformat()
                print("[summary] отправлена")
            except Exception as e:
                problems.append(f"сводка: {describe(e)}")
    token = str(cfg.get("telegram", {}).get("token") or "")
    if token:
        problems = [p.replace(token, "***") for p in problems]
    for p in problems:
        print("[error]", p)
    report_health(cfg, state, problems, now)
    if persist:
        STATE.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(time.strftime("%H:%M:%S"), "проверено" if not problems else "проверка с ошибками")
    return not problems


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
        try:
            rates = pair_rates(zf, resolve_currency(cy, args[1]), resolve_currency(cy, args[2]))
        except ValueError as e:
            raise SystemExit(str(e))
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
    if args[:1] == ["--rules"]:
        if len(args) > 1:
            cfg["rules_url"] = args[1]
        if not rules_url(cfg):
            raise SystemExit("Нет ссылки на таблицу: python watcher.py --rules \"<ссылка на CSV>\"")
        rules, problems = load_rules(cfg, {})
        zf = fetch_zip()
        cy, ex = currencies(zf), exchangers(zf)
        known = {n.lower() for n in ex.values()}
        print(f"Прочитано включённых правил: {len(rules)}")
        for r in rules:
            try:
                fid, tid = resolve_currency(cy, r["from"]), resolve_currency(cy, r["to"])
            except ValueError as e:
                problems.append(f"{r['_label']}: {e}")
                continue
            wanted = {n.lower() for n in r["exchangers"]}
            if wanted - known:
                problems.append(f"{r['_label']}: нет на BestChange обменника {', '.join(sorted(wanted - known))}")
            in_pair = sorted(ex[e] for e in pair_rates(zf, fid, tid)
                             if e in ex and (not wanted or ex[e].lower() in wanted))
            print(f"  {r['_label']}: {r['from']} → {r['to']}, порог {r['min_rate']:g}, "
                  f"обменники: {', '.join(r['exchangers']) or 'все'}; сейчас в паре: {', '.join(in_pair) or 'никого'}")
        print("\n".join(["Ошибки:"] + [f"  • {p}" for p in problems]) if problems else "Ошибок нет")
        return
    if args[:1] == ["--summary"]:
        global tg_send
        tg_send = lambda c, t: None  # только показать: уведомления о курсе не слать
        state, _ = load_state()
        rules, _ = load_rules(cfg, {})
        ctx = {}
        check(cfg, rules, json.loads(json.dumps(state)), ctx)  # антиспам не трогаем: проверяем на копии
        msk = datetime.now(MSK)
        record(state, msk.date().isoformat(), ctx.get("obs", {}), time.time())
        print(build_summary(state, ctx, msk.date(), summary_cfg(cfg, rules)["cbr_meetings"]))
        return
    if args[:1] == ["--once"]:
        # красный запуск в GitHub = запасной канал, если не дошло и предупреждение в Telegram
        sys.exit(0 if run_once(cfg) else 1)
    while True:
        run_once(cfg)
        time.sleep(cfg.get("interval_sec", 60))


if __name__ == "__main__":
    main()
