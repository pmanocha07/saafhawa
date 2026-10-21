# SaafHawa: a personal clean-air planner

**Live demo: https://nho1xxv73m.execute-api.ap-south-1.amazonaws.com/** · [Demo video](https://youtu.be/3eTWmP0NtAU?si=yu8hPancDLZs2cGw)

**Track: Air** · Environmental Hacks, Bharat Builds Tour (WeMakeDevs x AWS)

## The problem

From October to January, Delhi's AQI sits at "Poor" or worse almost every hour. Every AQI app shows a red number and says "avoid outdoor activity." People still have to run, commute, send kids out to play and air out their homes. An alert that says "bad air" every single day tells them nothing.

What *does* change is **when**. PM2.5 routinely swings two to three times within one day. At night the ground cools, the atmosphere's mixing layer sinks to a couple of hundred metres and traps smoke near the ground. In the afternoon the sun lifts it past a kilometre and dilutes it. Most people run at 6 AM and air out their homes at 7 AM, which is close to the worst time of day.

## What SaafHawa does

It forecasts that daily swing for your exact location and re-times the things you can't skip.

| Tab | What it gives you |
| --- | --- |
| **Today** | Live estimated NAQI, a 48-hour hourly PM2.5 chart, the cleanest and worst 3-hour stretches, how many times dirtier the worst hours are, and *why* (mixing-layer height) |
| **Planner** | Your routine (run, walk, commute, kids' play, errands, airing out the home) with the best start time for each, the % less PM2.5, and micrograms less inhaled, based on breathing rates |
| **Indoor** | Estimated PM2.5 inside your room with and without a purifier, the purifier CADR your room actually needs, and the best hour to air it out |
| **Exposure** | Daily averages for the past 5 days and next 2, turned into cigarette-equivalents, plus your personal daily exposure based on hours outdoors and indoor filtering |
| **Daily brief** | An email every morning at 6:00 AM with *your* plan for the day. It's a plan, not an alarm |
| **How it works** | Method, data sources, limits and architecture |

## AWS architecture

```
Browser ──HTTPS──> API Gateway (HTTP API) ──> Lambda (Python 3.12) ──> Open-Meteo (CAMS PM2.5/PM10, mixing layer, wind)
                                                 │
EventBridge cron (6:00 AM IST) ─────────────────>│──> DynamoDB  (user routines + 30-min forecast cache with TTL)
                                                 └──> SNS topic ──(filter policy: uid)──> each user's email
```

| Service | Role |
| --- | --- |
| **AWS Lambda** | Serves the app, runs the planner and exposure maths, builds and sends the daily briefs |
| **Amazon API Gateway** | Public HTTPS endpoint for the app and `/api/*` |
| **Amazon DynamoDB** | Stores each user's location and routine, and caches forecasts per location for 30 minutes (TTL cleans up) |
| **Amazon EventBridge** | Daily 6:00 AM IST schedule for the briefs |
| **Amazon SNS** | Email delivery. One topic, one filter policy per user, so each person receives only their own plan |
| **AWS SAM** | The whole stack is a single `template.yaml`, deployed with one command |

## The maths

- **NAQI (estimate):** 24-hour average PM2.5 and PM10, converted with CPCB breakpoints; the higher sub-index wins.
- **Best window:** every allowed start hour is scored by mean forecast PM2.5 over the activity's duration; the lowest wins.
- **Inhaled dose:** concentration × breathing rate × time. Breathing rates are approximately 3.0 m³/h running, 1.5 walking, 1.3 commuting, 1.6 kids at play, 1.2 errands (US EPA Exposure Factors Handbook).
- **Cigarette equivalent:** Berkeley Earth's estimate that 22 µg/m³ of PM2.5 for a day ≈ 1 cigarette.
- **Indoor:** steady-state mass balance: indoor = 0.8 × leak rate × outdoor ÷ (leak rate + 0.2 deposition + CADR ÷ volume). The recommended CADR is the larger of 5 air changes an hour or what keeps the room near the WHO 15 µg/m³ daily guideline.

## Deploy (about 5 minutes, free tier)

Use **AWS CloudShell**, which already has the SAM CLI.

1. In the AWS console, switch the region to **Asia Pacific (Mumbai) ap-south-1** and open **CloudShell**.
2. Click **Actions > Upload file** and upload `saafhawa.zip`, then run:

```bash
unzip -o saafhawa.zip && cd saafhawa
sam deploy --stack-name saafhawa --resolve-s3 --capabilities CAPABILITY_IAM --region ap-south-1
```

3. Open the `AppUrl` shown under **Outputs**.

No `sam build` is needed: the function uses only the Python standard library and boto3, which is built into Lambda.

To update after changes, run the same `sam deploy` command again. To remove everything: `sam delete --stack-name saafhawa --region ap-south-1`.

## API

| Method | Path | Input | Returns |
| --- | --- | --- | --- |
| GET | `/` | | The web app |
| GET | `/api/overview` | `?city=` or `?lat=&lon=&label=` | Now, 48h chart, stretches, daily history, default plan |
| POST | `/api/plan` | `{place, routine}` | Plan for today and tomorrow |
| GET | `/api/profile` | `?uid=` | Saved email, brief setting, place, routine |
| POST | `/api/profile` | `{uid, email, brief, place, routine}` | Saves the profile; subscribes or unsubscribes the email on SNS |
| POST | `/api/brief/test` | `{uid}` | Sends the brief now (rate-limited) |

## Limits and next steps

- The CAMS model grid is about 40 km, so it can miss very local sources. Next: blend in CPCB station data and crowd reports.
- Next: WhatsApp and SMS briefs, Hindi and regional languages, school and RWA group plans, and indoor sensor integration.

## Credits

- Air quality, weather and geocoding: [Open-Meteo](https://open-meteo.com/) (CC BY 4.0), based on Copernicus CAMS.
- NAQI breakpoints: Central Pollution Control Board (CPCB), India. Cigarette equivalence: Berkeley Earth.

## License

MIT
