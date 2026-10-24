# Election Night 2026 — Senate results wall

A full-screen, broadcast-style dashboard that simulates live results for the 35
U.S. Senate races on the 2026 ballot. Built for background shots: everything
moves, nothing needs an operator, and a fresh night is rolled automatically
when the previous one finishes.

**All numbers are randomly generated.** Candidate names are real; vote counts,
margins, precincts and projections are not.

## Run

```bash
pip install -r requirements.txt
python election_dashboard.py              # fullscreen, 1920x1080 scaled to your display
python election_dashboard.py --windowed   # resizable window
```

Keys: `F`/`F11` fullscreen · `SPACE` pause · `N` next spotlight race · `R` new night · `ESC`/`Q` quit

## What's on screen

| Panel | Visual |
| --- | --- |
| Header | Rolling odometer of total votes counted, live vote-flow equalizer, pulsing LIVE badge, simulated Eastern clock |
| Balance of Power | 100-seat hemicycle (not up / called / leading), tug-of-war control lever with a spring-loaded "if current leads hold" knob |
| Race Spotlight | Rotating featured race: swingometer dial with a physically damped needle, candidate cards, expected-vote ring, margin-over-time trace |
| Senate Map | Tile cartogram; tiles fill bottom-up with % reporting, flash on projection |
| Closest Races | Mixing-console faders with LED ladders for the five tightest uncalled races |
| All Races | Continuously scrolling results board for all 35 races |
| Margin Spectrum | Every reporting race as a live dot on a D+30 … R+30 axis |
| Ticker / banner | Crawl of every race; "PROJECTION" lower-third when races are called (batched at poll close) |

The simulation follows real poll-closing times (Georgia at 7:00 ET through
Alaska at 1:00 AM), lets early returns lean one way and drift back ("red
mirage / blue shift"), calls safe states at poll close and leaves razor-thin
races uncalled.

## Options

```
--cycle SECONDS      length of a full night (default 600)
--hold SECONDS       hold final results before restarting (default 45)
--spotlight SECONDS  time per spotlight race (default 10)
--start SECONDS      skip ahead into the night
--seed N             reproducible scenario
--title / --subtitle header text
--fps N              frame rate (default 30)
--no-sim-label       hide the small "SIMULATED RESULTS" tag in the header
--snapshot out.png   render one frame (after --start) headlessly and exit
```

Race data (candidates, baseline leans, turnout, poll-close times) is the
`RACES` table at the top of `election_dashboard.py`.

Fonts: Barlow Condensed, SIL Open Font License (`fonts/OFL.txt`).
