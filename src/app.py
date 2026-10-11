"""SaafHawa: a personal clean-air planner.

In cities like Delhi the air is "Poor" or worse for weeks at a time, so a
"the AQI is bad" alert says nothing new. What does change is *when* it is
least bad: PM2.5 often swings 2 to 3x within a single day, peaking at night
and in the early morning and dipping in the afternoon when the sun lifts the
mixing layer. SaafHawa forecasts those swings hour by hour and tells each
person when to do the things they cannot skip (exercise, commute, kids'
play, airing out the home) to breathe in the least pollution.

One Lambda function handles:
  * HTTP requests from API Gateway (the web app and its JSON API)
  * the 6:00 AM IST EventBridge schedule that emails each subscriber their
    personal plan for the day through SNS (one topic, filtered per user)

Data: Open-Meteo air quality API (Copernicus CAMS) for hourly PM2.5 and
PM10, Open-Meteo weather API for mixing-layer height and wind. Responses are
cached in DynamoDB for 30 minutes per location.
"""

import base64
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

TABLE_NAME = os.environ.get("TABLE_NAME", "")
TOPIC_ARN = os.environ.get("TOPIC_ARN", "")

IST = timezone(timedelta(hours=5, minutes=30))
HERE = os.path.dirname(os.path.abspath(__file__))
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
UID_RE = re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")

CACHE_SECONDS = 30 * 60
TEST_BRIEF_GAP_SEC = 120
PAST_DAYS = 5
FORECAST_DAYS = 3

# Berkeley Earth: breathing 22 ug/m3 of PM2.5 for a day is roughly as harmful as one cigarette.
UG_PER_CIGARETTE_DAY = 22.0

# Approximate breathing rates for adults (m3 per hour), from the US EPA Exposure Factors Handbook.
ACTIVITIES = {
    "run": {"name": "Run or workout", "rate": 3.0, "duration": 1, "earliest": 5, "latest": 21, "hour": 6, "kind": "outdoor"},
    "walk": {"name": "Walk or outdoor yoga", "rate": 1.5, "duration": 1, "earliest": 5, "latest": 21, "hour": 7, "kind": "outdoor"},
    "commute_am": {"name": "Morning commute", "rate": 1.3, "duration": 1, "earliest": 6, "latest": 11, "hour": 8, "kind": "outdoor"},
    "commute_pm": {"name": "Evening commute", "rate": 1.3, "duration": 1, "earliest": 15, "latest": 22, "hour": 18, "kind": "outdoor"},
    "kids": {"name": "Kids' outdoor play", "rate": 1.6, "duration": 2, "earliest": 7, "latest": 18, "hour": 17, "kind": "outdoor"},
    "errands": {"name": "Market or errands", "rate": 1.2, "duration": 1, "earliest": 8, "latest": 21, "hour": 11, "kind": "outdoor"},
    "ventilate": {"name": "Air out the home", "rate": 0.0, "duration": 1, "earliest": 6, "latest": 23, "hour": 7, "kind": "ventilation"},
}

DEFAULT_ROUTINE = [
    {"activity": "run", "hour": 6, "duration": 1},
    {"activity": "commute_am", "hour": 8, "duration": 1},
    {"activity": "ventilate", "hour": 7, "duration": 1},
]

# CPCB NAQI breakpoints: (conc_low, conc_high, index_low, index_high)
PM25_BP = [(0, 30, 0, 50), (30, 60, 50, 100), (60, 90, 100, 200), (90, 120, 200, 300), (120, 250, 300, 400), (250, 500, 400, 500)]
PM10_BP = [(0, 50, 0, 50), (50, 100, 50, 100), (100, 250, 100, 200), (250, 350, 200, 300), (350, 430, 300, 400), (430, 600, 400, 500)]
CATEGORIES = [
    (50, "Good", "#3a9a5b"),
    (100, "Satisfactory", "#8bbf3f"),
    (200, "Moderate", "#e3b21b"),
    (300, "Poor", "#ea7a1f"),
    (400, "Very Poor", "#d23b32"),
    (500, "Severe", "#7b2d8e"),
]


class UserError(Exception):
    """An error caused by user input, shown to the user as-is."""


# ------------------------------------------------------------------ AWS clients

_table = None
_sns = None
_html = None


def table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(TABLE_NAME)
    return _table


def sns():
    global _sns
    if _sns is None:
        _sns = boto3.client("sns")
    return _sns


# ------------------------------------------------------------------ helpers

def now_ist():
    return datetime.now(IST)


def hour_key(dt):
    return dt.strftime("%Y-%m-%dT%H:00")


def parse_key(key):
    return datetime.strptime(key, "%Y-%m-%dT%H:%M")


def fmt_hour(h):
    h = h % 24
    if h == 0:
        return "12 AM"
    if h == 12:
        return "12 PM"
    return f"{h} AM" if h < 12 else f"{h - 12} PM"


def fmt_range(start_hour, duration):
    return f"{fmt_hour(start_hour)} to {fmt_hour(start_hour + duration)}"


def mean(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def r1(x):
    return None if x is None else round(x, 1)


def sub_index(conc, breakpoints):
    if conc is None:
        return None
    c = max(0.0, float(conc))
    for c_lo, c_hi, i_lo, i_hi in breakpoints:
        if c <= c_hi:
            return i_lo + (i_hi - i_lo) * (c - c_lo) / (c_hi - c_lo)
    return 500.0


def category(naqi):
    for upper, name, colour in CATEGORIES:
        if naqi <= upper:
            return name, colour
    return CATEGORIES[-1][1], CATEGORIES[-1][2]


def naqi_from(pm25_avg, pm10_avg):
    s25 = sub_index(pm25_avg, PM25_BP)
    s10 = sub_index(pm10_avg, PM10_BP)
    if s25 is None and s10 is None:
        return None, None
    if s10 is None or (s25 is not None and s25 >= s10):
        return int(round(min(s25, 500))), "PM2.5"
    return int(round(min(s10, 500))), "PM10"


def slug(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# ------------------------------------------------------------------ external data

def get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "SaafHawa/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def geocode(name):
    name = (name or "").strip()
    if len(name) < 2:
        raise UserError("Enter a city or town name.")
    query = urllib.parse.urlencode({"name": name, "count": 1, "language": "en", "format": "json", "countryCode": "IN"})
    data = get_json("https://geocoding-api.open-meteo.com/v1/search?" + query)
    results = data.get("results") or []
    if not results:
        raise UserError(f"Couldn't find '{name}' in India. Try a nearby bigger city.")
    r = results[0]
    admin = r.get("admin1") or ""
    label = r["name"] if not admin or admin == r["name"] else f"{r['name']}, {admin}"
    return {"label": label, "lat": round(float(r["latitude"]), 3), "lon": round(float(r["longitude"]), 3)}


def place_from_params(params):
    if params.get("lat") not in (None, "") and params.get("lon") not in (None, ""):
        try:
            lat, lon = float(params["lat"]), float(params["lon"])
        except (TypeError, ValueError):
            raise UserError("Location coordinates are not valid.")
        if not (6 <= lat <= 38 and 68 <= lon <= 98):
            raise UserError("SaafHawa currently covers India only.")
        label = str(params.get("label") or "Your location")[:60]
        return {"label": label, "lat": round(lat, 3), "lon": round(lon, 3)}
    return geocode(params.get("city", ""))


def fetch_series(lat, lon):
    """Hourly PM2.5, PM10, mixing height and wind for past 5 days and next 3, cached in DynamoDB."""
    key = f"cache#{lat:.2f},{lon:.2f}"
    now = int(time.time())
    try:
        item = table().get_item(Key={"pk": key}).get("Item")
        if item and int(item.get("fetched", 0)) > now - CACHE_SECONDS:
            return json.loads(item["data"])
    except Exception as exc:
        print(f"cache read failed: {exc!r}")

    q = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon, "hourly": "pm2_5,pm10",
        "past_days": PAST_DAYS, "forecast_days": FORECAST_DAYS, "timezone": "Asia/Kolkata",
    })
    air = get_json("https://air-quality-api.open-meteo.com/v1/air-quality?" + q)["hourly"]
    series = {
        "time": air["time"],
        "pm25": air["pm2_5"],
        "pm10": air["pm10"],
        "blh": [None] * len(air["time"]),
        "wind": [None] * len(air["time"]),
    }
    try:
        q = urllib.parse.urlencode({
            "latitude": lat, "longitude": lon, "hourly": "boundary_layer_height,wind_speed_10m",
            "past_days": PAST_DAYS, "forecast_days": FORECAST_DAYS, "timezone": "Asia/Kolkata",
        })
        wx = get_json("https://api.open-meteo.com/v1/forecast?" + q, timeout=6)["hourly"]
        lookup = {t: i for i, t in enumerate(wx["time"])}
        for i, t in enumerate(series["time"]):
            j = lookup.get(t)
            if j is not None:
                series["blh"][i] = (wx.get("boundary_layer_height") or [None] * len(wx["time"]))[j]
                series["wind"][i] = (wx.get("wind_speed_10m") or [None] * len(wx["time"]))[j]
    except Exception as exc:
        print(f"weather fetch skipped: {exc!r}")

    try:
        table().put_item(Item={"pk": key, "fetched": now, "ttl": now + 6 * 3600, "data": json.dumps(series)})
    except Exception as exc:
        print(f"cache write failed: {exc!r}")
    return series


# ------------------------------------------------------------------ analysis

class Series:
    def __init__(self, raw):
        self.time = raw["time"]
        self.pm25 = raw["pm25"]
        self.pm10 = raw["pm10"]
        self.blh = raw.get("blh") or [None] * len(self.time)
        self.wind = raw.get("wind") or [None] * len(self.time)
        self.index = {t: i for i, t in enumerate(self.time)}

    def at(self, key):
        i = self.index.get(key)
        return None if i is None else self.pm25[i]

    def window(self, date, start_hour, duration):
        """Mean PM2.5 for a window starting at date+start_hour, or None if data is missing."""
        vals = []
        for h in range(start_hour, start_hour + duration):
            day = date + timedelta(days=h // 24)
            v = self.at(f"{day.isoformat()}T{h % 24:02d}:00")
            if v is None:
                return None
            vals.append(v)
        return sum(vals) / len(vals)

    def naqi_at(self, i):
        lo = max(0, i - 23)
        p25 = [v for v in self.pm25[lo:i + 1] if v is not None]
        p10 = [v for v in self.pm10[lo:i + 1] if v is not None]
        if len(p25) < 16 and len(p10) < 16:
            return None, None
        return naqi_from(mean(p25) if len(p25) >= 16 else None, mean(p10) if len(p10) >= 16 else None)

    def day_avg(self, date, field="pm25"):
        arr = self.pm25 if field == "pm25" else self.pm10
        prefix = date.isoformat()
        vals = [arr[i] for i, t in enumerate(self.time) if t.startswith(prefix) and arr[i] is not None]
        return (sum(vals) / len(vals), len(vals)) if vals else (None, 0)


def plan_item(series, date, item, now):
    act = ACTIVITIES[item["activity"]]
    duration = int(item.get("duration") or act["duration"])
    usual = int(item.get("hour", act["hour"]))
    first_allowed = act["earliest"]
    if date == now.date():
        first_allowed = max(first_allowed, now.hour + 1 if now.minute > 15 else now.hour)

    candidates = []
    for start in range(first_allowed, act["latest"] + 1):
        c = series.window(date, start, duration)
        if c is not None:
            candidates.append((c, start))

    result = {
        "activity": item["activity"],
        "name": act["name"],
        "kind": act["kind"],
        "duration": duration,
        "usual_hour": usual,
        "usual_label": fmt_range(usual, duration),
    }
    if not candidates:
        result["status"] = "none"
        result["message"] = "No time left in the usual range today. See tomorrow's plan."
        return result

    best_c, best_h = min(candidates)
    worst_c, worst_h = max(candidates)
    usual_c = series.window(date, usual, duration)
    usual_passed = date == now.date() and usual < first_allowed

    result.update({
        "status": "ok",
        "best_hour": best_h,
        "best_label": fmt_range(best_h, duration),
        "best_pm25": r1(best_c),
        "worst_hour": worst_h,
        "worst_label": fmt_range(worst_h, duration),
        "worst_pm25": r1(worst_c),
        "usual_pm25": None if usual_passed else r1(usual_c),
        "usual_passed": usual_passed,
        "options": [{"hour": h, "pm25": r1(c)} for c, h in sorted(candidates, key=lambda x: x[1])],
    })

    compare = usual_c if (usual_c is not None and not usual_passed) else worst_c
    compare_label = "your usual time" if (usual_c is not None and not usual_passed) else "the worst time"
    saving = 0 if compare <= 0 else max(0.0, (compare - best_c) / compare * 100)
    result["saving_pct"] = int(round(saving))
    result["compare_label"] = compare_label

    if act["kind"] == "outdoor":
        dose_best = best_c * act["rate"] * duration
        dose_cmp = compare * act["rate"] * duration
        result["dose_best_ug"] = int(round(dose_best))
        result["dose_compare_ug"] = int(round(dose_cmp))
        result["dose_saved_ug"] = int(round(dose_cmp - dose_best))

    if best_h == usual and not usual_passed:
        result["message"] = f"Your usual time, {fmt_range(usual, duration)}, is already the cleanest option. Keep it."
    elif saving < 8:
        result["message"] = f"The air barely changes in this range (under 8%). Any time works; {result['best_label']} is marginally best."
    elif act["kind"] == "ventilation":
        result["message"] = (
            f"Open windows {result['best_label']} (PM2.5 about {int(best_c)}). "
            f"Keep them shut around {fmt_hour(worst_h)}, when it peaks near {int(worst_c)}."
        )
    else:
        result["message"] = (
            f"Move it to {result['best_label']}: about {result['saving_pct']}% less PM2.5 than {compare_label}"
            + (f" ({result['dose_saved_ug']} ug less inhaled)." if result.get("dose_saved_ug") else ".")
        )
    return result


def stretches(series, date, now, hours=3, first=6, last=23):
    """Cleanest and dirtiest `hours`-long stretch between first and last o'clock on date."""
    start_from = first
    if date == now.date():
        start_from = max(first, now.hour)
    opts = []
    for s in range(start_from, last - hours + 2):
        c = series.window(date, s, hours)
        if c is not None:
            opts.append((c, s))
    if not opts:
        return None
    best = min(opts)
    worst = max(opts)
    return {
        "clean": {"hour": best[1], "label": fmt_range(best[1], hours), "pm25": r1(best[0])},
        "dirty": {"hour": worst[1], "label": fmt_range(worst[1], hours), "pm25": r1(worst[0])},
        "ratio": r1(worst[0] / best[0]) if best[0] > 0 else None,
    }


def why_text(series, date, clean_hour, dirty_hour):
    i_clean = series.index.get(f"{date.isoformat()}T{clean_hour:02d}:00")
    i_dirty = series.index.get(f"{date.isoformat()}T{dirty_hour:02d}:00")
    if i_clean is None or i_dirty is None:
        return None
    b_clean, b_dirty = series.blh[i_clean], series.blh[i_dirty]
    if b_clean is None or b_dirty is None or b_clean <= b_dirty:
        return None
    return (
        f"Why: around {fmt_hour(clean_hour)} the sun lifts the mixing layer to about {int(b_clean)} m, "
        f"diluting the smoke and dust. Around {fmt_hour(dirty_hour)} it sinks to about {int(b_dirty)} m "
        "and traps pollution near the ground."
    )


def build_overview(place, routine=None):
    raw = fetch_series(place["lat"], place["lon"])
    s = Series(raw)
    now = now_ist()
    today = now.date()
    tomorrow = today + timedelta(days=1)
    now_k = hour_key(now)

    past_idx = [i for i, t in enumerate(s.time) if t <= now_k]
    if not past_idx:
        raise UserError("No air quality data for this location right now.")
    i_now = past_idx[-1]
    naqi, driver = s.naqi_at(i_now)
    if naqi is None:
        raise UserError("Not enough recent data for this location.")
    cat, colour = category(naqi)

    # 48-hour chart: today 00:00 to tomorrow 23:00
    chart = []
    for d in (today, tomorrow):
        for h in range(24):
            k = f"{d.isoformat()}T{h:02d}:00"
            i = s.index.get(k)
            chart.append({
                "t": k,
                "pm25": r1(s.pm25[i]) if i is not None else None,
                "blh": s.blh[i] if i is not None else None,
                "past": k <= now_k,
            })

    # Daily history and outlook
    days = []
    for offset in range(-PAST_DAYS, 2):
        d = today + timedelta(days=offset)
        avg25, n = s.day_avg(d, "pm25")
        avg10, _ = s.day_avg(d, "pm10")
        if avg25 is None or n < 16:
            continue
        dn, _ = naqi_from(avg25, avg10)
        dc, dcol = category(dn)
        days.append({
            "date": d.isoformat(),
            "label": "Today" if offset == 0 else ("Tomorrow" if offset == 1 else d.strftime("%a %d")),
            "kind": "forecast" if offset > 0 else ("today" if offset == 0 else "past"),
            "pm25": r1(avg25),
            "naqi": dn,
            "category": dc,
            "color": dcol,
            "cigarettes": r1(avg25 / UG_PER_CIGARETTE_DAY),
        })

    today_stretch = stretches(s, today, now)
    tomorrow_stretch = stretches(s, tomorrow, now)
    focus_day, focus = (today, today_stretch) if today_stretch else (tomorrow, tomorrow_stretch)
    why = why_text(s, focus_day, focus["clean"]["hour"], focus["dirty"]["hour"]) if focus else None

    routine = routine or DEFAULT_ROUTINE
    plan_today = [plan_item(s, today, it, now) for it in routine]
    plan_tomorrow = [plan_item(s, tomorrow, it, now) for it in routine]

    today_avg = next((d for d in days if d["kind"] == "today"), None)
    return {
        "place": place,
        "generated": now.strftime("%Y-%m-%dT%H:%M"),
        "now": {
            "naqi": naqi,
            "driver": driver,
            "category": cat,
            "color": colour,
            "pm25": r1(s.pm25[i_now]),
            "pm10": r1(s.pm10[i_now]),
            "blh": s.blh[i_now],
            "wind": s.wind[i_now],
            "hour": s.time[i_now],
        },
        "today_cigarettes": today_avg["cigarettes"] if today_avg else None,
        "chart": chart,
        "days": days,
        "stretch": {"today": today_stretch, "tomorrow": tomorrow_stretch},
        "why": why,
        "plan": {"today": plan_today, "tomorrow": plan_tomorrow},
        "activities": {k: {kk: v[kk] for kk in ("name", "duration", "earliest", "latest", "hour", "kind")} for k, v in ACTIVITIES.items()},
    }


# ------------------------------------------------------------------ profiles + morning brief

def clean_routine(raw):
    if not isinstance(raw, list) or not raw:
        raise UserError("Add at least one activity to your routine.")
    if len(raw) > 8:
        raise UserError("Up to 8 activities.")
    out = []
    for it in raw:
        if not isinstance(it, dict) or it.get("activity") not in ACTIVITIES:
            raise UserError("Unknown activity in routine.")
        act = ACTIVITIES[it["activity"]]
        try:
            hour = int(it.get("hour", act["hour"]))
            duration = int(it.get("duration", act["duration"]))
        except (TypeError, ValueError):
            raise UserError("Routine times must be whole hours.")
        if not 0 <= hour <= 23 or not 1 <= duration <= 3:
            raise UserError("Pick an hour between 0 and 23 and a duration of 1 to 3 hours.")
        out.append({"activity": it["activity"], "hour": hour, "duration": duration})
    return out


def get_profile(uid):
    item = table().get_item(Key={"pk": f"user#{uid}"}).get("Item")
    if not item:
        return None
    return {
        "uid": uid,
        "email": item.get("email", ""),
        "brief": bool(item.get("brief", False)),
        "place": json.loads(item["place"]) if item.get("place") else None,
        "routine": json.loads(item["routine"]) if item.get("routine") else DEFAULT_ROUTINE,
        "sub_arn": item.get("sub_arn", ""),
        "last_test_ts": int(item.get("last_test_ts", 0)),
    }


def save_profile(body):
    uid = str(body.get("uid", ""))
    if not UID_RE.match(uid):
        raise UserError("Missing device id. Reload the page and try again.")
    place = body.get("place")
    if not isinstance(place, dict):
        raise UserError("Pick a location first.")
    place = place_from_params({"lat": place.get("lat"), "lon": place.get("lon"), "label": place.get("label")})
    routine = clean_routine(body.get("routine"))
    email = str(body.get("email") or "").strip()
    brief = bool(body.get("brief"))
    if brief and not EMAIL_RE.match(email):
        raise UserError("Enter a valid email for the morning brief.")

    old = get_profile(uid) or {}
    sub_arn = old.get("sub_arn", "")
    message = "Saved. Your plan updates automatically."

    email_changed = email.lower() != (old.get("email") or "").lower()
    if (not brief or email_changed) and sub_arn:
        try:
            sns().unsubscribe(SubscriptionArn=sub_arn)
        except Exception as exc:
            print(f"unsubscribe skipped: {exc!r}")
        sub_arn = ""
    if brief and (email_changed or not sub_arn):
        resp = sns().subscribe(
            TopicArn=TOPIC_ARN,
            Protocol="email",
            Endpoint=email,
            Attributes={"FilterPolicy": json.dumps({"uid": [uid]})},
            ReturnSubscriptionArn=True,
        )
        sub_arn = resp.get("SubscriptionArn", "")
        message = (
            f"Saved. Check {email} for an email from AWS Notifications and click 'Confirm subscription'. "
            "Your brief arrives every day at 6:00 AM."
        )
    elif brief:
        message = "Saved. Your next brief arrives at 6:00 AM."

    table().put_item(Item={
        "pk": f"user#{uid}",
        "email": email,
        "brief": brief,
        "place": json.dumps(place),
        "routine": json.dumps(routine),
        "sub_arn": sub_arn,
        "last_test_ts": old.get("last_test_ts", 0),
        "updated": int(time.time()),
    })
    return {"ok": True, "message": message}


def brief_text(profile):
    place = profile["place"]
    data = build_overview(place, profile["routine"])
    now = data["now"]
    st = data["stretch"]["today"]
    plan = data["plan"]["today"]
    day = "today"
    if not st or not all(p["status"] == "ok" for p in plan):
        st, plan, day = data["stretch"]["tomorrow"], data["plan"]["tomorrow"], "tomorrow"

    lines = [f"Your clean-air plan for {place['label']} {day}:", ""]
    if st:
        lines.append(f"Cleanest stretch: {st['clean']['label']} (PM2.5 about {int(st['clean']['pm25'])}).")
        lines.append(f"Worst stretch: {st['dirty']['label']} (PM2.5 about {int(st['dirty']['pm25'])}). Stay in and keep windows shut.")
        if st.get("ratio"):
            lines.append(f"The worst hours are {st['ratio']}x dirtier than the cleanest, so timing matters {day}.")
    lines.append("")
    lines.append("Your routine:")
    for p in plan:
        lines.append(f"  - {p['name']}: {p.get('message', '')}")
    lines.append("")
    if data.get("today_cigarettes") is not None:
        lines.append(
            f"Breathing today's outdoor air all day is roughly like smoking {data['today_cigarettes']} cigarettes "
            "(Berkeley Earth estimate). An air purifier indoors cuts most of it."
        )
    lines.append(f"Right now: estimated NAQI {now['naqi']} ({now['category']}), mostly {now['driver']}.")
    lines.append("")
    lines.append("Forecast from Open-Meteo (Copernicus CAMS). Model estimate, not a CPCB station reading.")
    lines.append("To change your routine, open SaafHawa. To stop these emails, use the unsubscribe link below.")
    subject = f"Clean-air hours {day} for {place['label']}"
    if st:
        subject = f"Cleanest {day}: {st['clean']['label']} in {place['label']}"
    return subject.encode("ascii", "ignore").decode()[:99], "\n".join(lines)


def send_brief(uid, profile):
    subject, message = brief_text(profile)
    sns().publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=message,
        MessageAttributes={"uid": {"DataType": "String", "StringValue": uid}},
    )


def run_morning_briefs():
    sent, failed = 0, 0
    scan_kwargs = {
        "FilterExpression": "begins_with(pk, :p) AND brief = :b",
        "ExpressionAttributeValues": {":p": "user#", ":b": True},
    }
    while True:
        page_ = table().scan(**scan_kwargs)
        for item in page_.get("Items", []):
            uid = item["pk"].split("#", 1)[1]
            try:
                send_brief(uid, get_profile(uid))
                sent += 1
            except Exception as exc:
                failed += 1
                print(f"brief failed for {uid}: {exc!r}")
        if "LastEvaluatedKey" not in page_:
            break
        scan_kwargs["ExclusiveStartKey"] = page_["LastEvaluatedKey"]
    print(json.dumps({"briefs_sent": sent, "failed": failed}))
    return {"sent": sent, "failed": failed}


# ------------------------------------------------------------------ HTTP routes

def route_overview(params):
    place = place_from_params(params)
    return build_overview(place)


def route_plan(body):
    place = body.get("place")
    if not isinstance(place, dict):
        raise UserError("Pick a location first.")
    place = place_from_params({"lat": place.get("lat"), "lon": place.get("lon"), "label": place.get("label")})
    routine = clean_routine(body.get("routine"))
    data = build_overview(place, routine)
    return {"place": data["place"], "plan": data["plan"], "stretch": data["stretch"]}


def route_get_profile(params):
    uid = str(params.get("uid", ""))
    if not UID_RE.match(uid):
        raise UserError("Missing device id.")
    p = get_profile(uid)
    if not p:
        return {"exists": False}
    return {"exists": True, "email": p["email"], "brief": p["brief"], "place": p["place"], "routine": p["routine"]}


def route_test_brief(body):
    uid = str(body.get("uid", ""))
    if not UID_RE.match(uid):
        raise UserError("Missing device id.")
    p = get_profile(uid)
    if not p or not p["brief"] or not p["email"]:
        raise UserError("Turn on the morning brief and save first.")
    now = int(time.time())
    if now - p["last_test_ts"] < TEST_BRIEF_GAP_SEC:
        raise UserError("A test brief was just sent. Wait two minutes.")
    table().update_item(
        Key={"pk": f"user#{uid}"},
        UpdateExpression="SET last_test_ts = :t",
        ExpressionAttributeValues={":t": now},
    )
    send_brief(uid, p)
    return {"ok": True, "message": f"Brief sent to {p['email']}. If you confirmed the subscription, it arrives within a minute."}


def respond(status, payload, content_type="application/json"):
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return {"statusCode": status, "headers": {"content-type": content_type, "cache-control": "no-store"}, "body": body}


def page():
    global _html
    if _html is None:
        with open(os.path.join(HERE, "index.html"), encoding="utf-8") as f:
            _html = f.read()
    return respond(200, _html, "text/html; charset=utf-8")


def parse_body(event):
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise UserError("Request body must be JSON.")
    if not isinstance(data, dict):
        raise UserError("Request body must be a JSON object.")
    return data


def handler(event, context):
    if event.get("source") == "aws.events" or event.get("detail-type") == "Scheduled Event":
        return run_morning_briefs()

    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "/")
    params = event.get("queryStringParameters") or {}
    try:
        if method == "GET" and path == "/":
            return page()
        if method == "GET" and path == "/api/overview":
            return respond(200, route_overview(params))
        if method == "POST" and path == "/api/plan":
            return respond(200, route_plan(parse_body(event)))
        if method == "GET" and path == "/api/profile":
            return respond(200, route_get_profile(params))
        if method == "POST" and path == "/api/profile":
            return respond(200, save_profile(parse_body(event)))
        if method == "POST" and path == "/api/brief/test":
            return respond(200, route_test_brief(parse_body(event)))
        return respond(404, {"error": "Not found"})
    except UserError as exc:
        return respond(400, {"error": str(exc)})
    except Exception as exc:
        print(f"unhandled error on {method} {path}: {exc!r}")
        return respond(502, {"error": "Couldn't reach the air quality service. Try again in a minute."})
