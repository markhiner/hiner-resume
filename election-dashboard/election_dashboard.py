#!/usr/bin/env python3
"""
Election Night 2026 - U.S. Senate results wall.

A broadcast-style, full-screen dashboard that simulates results arriving for
the 35 Senate races on the 2026 ballot. Every vote count, margin, precinct
total and projection is randomly generated; a new night is rolled each time
the previous one finishes, so it can loop unattended behind a set.

    pip install -r requirements.txt
    python election_dashboard.py                 # fullscreen
    python election_dashboard.py --windowed      # in a resizable window

Keys:  F / F11 fullscreen   SPACE pause   N next spotlight race
       R start a new night  ESC / Q quit
"""

import argparse
import math
import os
import random
import sys
from collections import deque

os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")
import pygame
import pygame.gfxdraw as gfx

W, H = 1920, 1080
HERE = os.path.dirname(os.path.abspath(__file__))
FONT_DIR = os.path.join(HERE, "fonts")

# ---------------------------------------------------------------- palette
TEXT = (232, 240, 255)
DIM = (128, 148, 186)
FAINT = (58, 74, 108)
ACCENT = (0, 198, 255)
GOLD = (255, 198, 72)
LIVE_RED = (232, 22, 44)
DEM = (38, 122, 255)
DEM_LT = (120, 176, 255)
DEM_DK = (16, 44, 108)
GOP = (236, 44, 60)
GOP_LT = (255, 128, 132)
GOP_DK = (96, 18, 28)
IND = (240, 172, 40)
IND_DK = (96, 66, 12)
NEUTRAL = (70, 84, 112)

PARTY = {
    "D": dict(color=DEM, light=DEM_LT, dark=DEM_DK, short="DEM", word="DEMOCRAT"),
    "R": dict(color=GOP, light=GOP_LT, dark=GOP_DK, short="GOP", word="REPUBLICAN"),
    "I": dict(color=IND, light=(255, 210, 120), dark=IND_DK, short="IND", word="INDEPENDENT"),
}

# ---------------------------------------------------------------- race data
# abbr, state, Democrat, Republican, baseline lean (R minus D, points),
# expected votes (thousands), poll close (ET, 24h+), seat held by, special.
# Leans and turnout are rough simulation inputs only; each night re-rolls them.
RACES = [
    ("AL", "Alabama", "Everett Wess", "Barry Moore", 25, 1400, 20.0, "R", False),
    ("AK", "Alaska", "Mary Peltola", "Dan Sullivan", 6, 260, 25.0, "R", False),
    ("AR", "Arkansas", "Hallie Shoffner", "Tom Cotton", 27, 900, 20.5, "R", False),
    ("CO", "Colorado", "John Hickenlooper", "Mark Baisley", -11, 2500, 21.0, "D", False),
    ("DE", "Delaware", "Chris Coons", "Michael Katz", -18, 360, 20.0, "D", False),
    ("FL", "Florida", "Angie Nixon", "Ashley Moody", 10, 7700, 20.0, "R", True),
    ("GA", "Georgia", "Jon Ossoff", "Mike Collins", -1, 3900, 19.0, "D", False),
    ("ID", "Idaho", "David Roth", "Jim Risch", 31, 600, 22.0, "R", False),
    ("IL", "Illinois", "Juliana Stratton", "Don Tracy", -14, 4100, 20.0, "D", False),
    ("IA", "Iowa", "Josh Turek", "Ashley Hinson", 6, 1200, 21.0, "R", False),
    ("KS", "Kansas", "Adam Hamilton", "Roger Marshall", 13, 1000, 21.0, "R", False),
    ("KY", "Kentucky", "Charles Booker", "Andy Barr", 21, 1500, 19.0, "R", False),
    ("LA", "Louisiana", "Jamie Davis", "Julia Letlow", 19, 1400, 21.0, "R", False),
    ("ME", "Maine", "Troy Jackson", "Susan Collins", 1, 680, 20.0, "R", False),
    ("MA", "Massachusetts", "Ed Markey", "John Deaton", -24, 2500, 20.0, "D", False),
    ("MI", "Michigan", "Abdul El-Sayed", "Mike Rogers", -1, 4500, 21.0, "D", False),
    ("MN", "Minnesota", "Peggy Flanagan", "Michele Tafoya", -6, 2500, 21.0, "D", False),
    ("MS", "Mississippi", "Scott Colom", "Cindy Hyde-Smith", 14, 700, 20.0, "R", False),
    ("MT", "Montana", "Alani Bankhead", "Kurt Alme", 13, 480, 22.0, "R", False),
    ("NE", "Nebraska", None, "Pete Ricketts", 14, 680, 21.0, "R", False),
    ("NH", "New Hampshire", "Chris Pappas", "John E. Sununu", -2, 620, 20.0, "D", False),
    ("NJ", "New Jersey", "Cory Booker", "Justin Murphy", -12, 2600, 20.0, "D", False),
    ("NM", "New Mexico", "Ben Ray Luján", "Larry Marker", -9, 720, 21.0, "D", False),
    ("NC", "North Carolina", "Roy Cooper", "Michael Whatley", -1, 3800, 19.5, "R", False),
    ("OH", "Ohio", "Sherrod Brown", "Jon Husted", 3, 4200, 19.5, "R", True),
    ("OK", "Oklahoma", "N'Kiyla Thomas", "Kevin Hern", 30, 1150, 20.0, "R", False),
    ("OR", "Oregon", "Jeff Merkley", "David Brock Smith", -15, 1950, 23.0, "D", False),
    ("RI", "Rhode Island", "Jack Reed", "Raymond McKay", -23, 360, 20.0, "D", False),
    ("SC", "South Carolina", "Annie Andrews", "Darline Graham", 16, 1700, 19.0, "R", False),
    ("SD", "South Dakota", None, "Mike Rounds", 34, 350, 21.0, "R", False),
    ("TN", "Tennessee", "Marquita Bradshaw", "Bill Hagerty", 22, 1750, 20.0, "R", False),
    ("TX", "Texas", "James Talarico", "Ken Paxton", 5, 8100, 21.0, "R", False),
    ("VA", "Virginia", "Mark Warner", "Bert Mizusawa", -9, 3000, 19.0, "D", False),
    ("WV", "West Virginia", "Rachel Fetty Anderson", "Shelley Moore Capito", 36, 500, 19.5, "R", False),
    ("WY", "Wyoming", "James Byrd", "Harriet Hageman", 40, 200, 21.0, "R", False),
]

INCUMBENTS = {
    "Dan Sullivan", "Tom Cotton", "John Hickenlooper", "Chris Coons", "Ashley Moody",
    "Jon Ossoff", "Jim Risch", "Roger Marshall", "Susan Collins", "Ed Markey",
    "Cindy Hyde-Smith", "Pete Ricketts", "Cory Booker", "Ben Ray Luján", "Jon Husted",
    "Jeff Merkley", "Jack Reed", "Mike Rounds", "Bill Hagerty", "Mark Warner",
    "Shelley Moore Capito",
}
SURNAMES = {
    "David Brock Smith": "Brock Smith",
    "Rachel Fetty Anderson": "Fetty Anderson",
    "Shelley Moore Capito": "Capito",
    "John E. Sununu": "Sununu",
}

# Seats not on the ballot (Democrats include the two caucusing independents).
NOT_UP = {"D": 34, "R": 31}
# The Vice President breaks ties for Republicans: 51 for Dem control, 50 for GOP.
DEM_CONTROL = 51

# NPR-style tile grid cartogram (col, row)
TILES = {
    "AK": (0, 0), "ME": (10, 0), "VT": (9, 1), "NH": (10, 1),
    "WA": (0, 2), "ID": (1, 2), "MT": (2, 2), "ND": (3, 2), "MN": (4, 2), "IL": (5, 2),
    "WI": (6, 2), "MI": (7, 2), "NY": (8, 2), "RI": (9, 2), "MA": (10, 2),
    "OR": (0, 3), "NV": (1, 3), "WY": (2, 3), "SD": (3, 3), "IA": (4, 3), "IN": (5, 3),
    "OH": (6, 3), "PA": (7, 3), "NJ": (8, 3), "CT": (9, 3),
    "CA": (0, 4), "UT": (1, 4), "CO": (2, 4), "NE": (3, 4), "MO": (4, 4), "KY": (5, 4),
    "WV": (6, 4), "VA": (7, 4), "MD": (8, 4), "DE": (9, 4),
    "AZ": (1, 5), "NM": (2, 5), "KS": (3, 5), "AR": (4, 5), "TN": (5, 5), "NC": (6, 5),
    "SC": (7, 5),
    "OK": (3, 6), "LA": (4, 6), "MS": (5, 6), "AL": (6, 6), "GA": (7, 6),
    "HI": (0, 7), "TX": (3, 7), "FL": (8, 7),
}

NIGHT_START, NIGHT_END = 18.85, 27.0  # 6:51 PM -> 3:00 AM Eastern


# ---------------------------------------------------------------- helpers
def clamp(x, a, b):
    return a if x < a else b if x > b else x


def lerp(a, b, t):
    return a + (b - a) * t


def mix(c1, c2, t):
    t = clamp(t, 0.0, 1.0)
    return tuple(int(a + (b - a) * t) for a, b in zip(c1, c2))


def scale(c, k):
    return tuple(int(clamp(v * k, 0, 255)) for v in c[:3])


def fmt_int(n):
    return f"{int(round(n)):,}"


def fmt_hour(h):
    hh = int(h) % 24
    mm = int((h - int(h)) * 60)
    return f"{hh % 12 or 12}:{mm:02d} {'PM' if hh >= 12 else 'AM'}"


def fmt_rate(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}K"
    return f"{v:.0f}"


def surname(name):
    return SURNAMES.get(name, name.split()[-1]).upper()


def initials(name):
    parts = [p for p in name.replace(".", "").split() if len(p) > 1]
    return (parts[0][0] + parts[-1][0]).upper() if len(parts) > 1 else name[:2].upper()


def margin_label(party, m):
    return f"{party} +{abs(m):.1f}"


class Spring:
    """Under-damped spring so needles and faders swing like hardware."""

    def __init__(self, v=0.0, k=55.0, c=8.0):
        self.v, self.vel, self.target, self.k, self.c = v, 0.0, v, k, c

    def update(self, dt):
        for _ in range(2):
            h = dt / 2
            a = self.k * (self.target - self.v) - self.c * self.vel
            self.vel += a * h
            self.v += self.vel * h


# ---------------------------------------------------------------- fonts / text
class Text:
    WEIGHTS = {"m": "BarlowCondensed-Medium.ttf", "s": "BarlowCondensed-SemiBold.ttf",
               "b": "BarlowCondensed-Bold.ttf"}
    FALLBACK = "bahnschrift,robotocondensed,arialnarrow,helveticaneue,arial,dejavusans"

    def __init__(self):
        self.fonts = {}
        self.cache = {}

    def font(self, size, w="s"):
        key = (size, w)
        f = self.fonts.get(key)
        if f is None:
            path = os.path.join(FONT_DIR, self.WEIGHTS[w])
            if os.path.exists(path):
                f = pygame.font.Font(path, size)
            else:
                f = pygame.font.SysFont(self.FALLBACK, int(size * 0.82), bold=(w != "m"))
            self.fonts[key] = f
        return f

    def render(self, s, size, w="s", color=TEXT, spacing=0):
        key = (s, size, w, color, spacing)
        surf = self.cache.get(key)
        if surf is None:
            f = self.font(size, w)
            if not spacing:
                surf = f.render(s, True, color)
            else:
                glyphs = [f.render(ch, True, color) for ch in s]
                width = sum(g.get_width() for g in glyphs) + spacing * max(0, len(s) - 1)
                surf = pygame.Surface((max(1, width), f.get_height()), pygame.SRCALPHA)
                x = 0
                for g in glyphs:
                    surf.blit(g, (x, 0))
                    x += g.get_width() + spacing
            if len(self.cache) > 6000:
                self.cache.clear()
            self.cache[key] = surf
        return surf

    def draw(self, dst, s, size, w, color, pos, anchor="topleft", spacing=0, alpha=255):
        if not s:
            return pygame.Rect(pos, (0, 0))
        surf = self.render(s, size, w, color, spacing)
        r = surf.get_rect(**{anchor: (int(pos[0]), int(pos[1]))})
        if alpha < 255:
            surf = surf.copy()
            surf.set_alpha(int(alpha))
        dst.blit(surf, r)
        return r

    def fit(self, s, max_w, size, w="b", min_size=12):
        while size > min_size and self.font(size, w).size(s)[0] > max_w:
            size -= 1
        return size


T = None  # set in main()


# ---------------------------------------------------------------- drawing
def aa_circle(s, color, c, r):
    r = int(r)
    if r < 1:
        return
    x, y = int(c[0]), int(c[1])
    gfx.aacircle(s, x, y, r, color)
    gfx.filled_circle(s, x, y, r, color)


def aa_ring(s, color, c, r):
    gfx.aacircle(s, int(c[0]), int(c[1]), int(r), color)


def aa_poly(s, color, pts):
    pts = [(int(round(x)), int(round(y))) for x, y in pts]
    if len(pts) < 3:
        return
    gfx.aapolygon(s, pts, color)
    gfx.filled_polygon(s, pts, color)


def arc_band(s, color, c, r0, r1, a0, a1):
    """Filled ring segment between radii r0..r1, angles a0..a1 (radians, CCW, y up)."""
    n = max(2, int(abs(a1 - a0) * r1 / 5) + 1)
    cx, cy = c
    outer = [(cx + r1 * math.cos(a0 + (a1 - a0) * i / (n - 1)),
              cy - r1 * math.sin(a0 + (a1 - a0) * i / (n - 1))) for i in range(n)]
    inner = [(cx + r0 * math.cos(a1 + (a0 - a1) * i / (n - 1)),
              cy - r0 * math.sin(a1 + (a0 - a1) * i / (n - 1))) for i in range(n)]
    aa_poly(s, color, outer + inner)


def thick_line(s, color, p0, p1, w):
    dx, dy = p1[0] - p0[0], p1[1] - p0[1]
    ln = math.hypot(dx, dy) or 1
    nx, ny = -dy / ln * w / 2, dx / ln * w / 2
    aa_poly(s, color, [(p0[0] + nx, p0[1] + ny), (p1[0] + nx, p1[1] + ny),
                       (p1[0] - nx, p1[1] - ny), (p0[0] - nx, p0[1] - ny)])


def box(s, rect, color):
    gfx.box(s, pygame.Rect(rect), color)


def vgradient(size, top, bottom):
    surf = pygame.Surface(size, pygame.SRCALPHA)
    w, h = size
    for y in range(h):
        pygame.draw.line(surf, mix(top, bottom, y / max(1, h - 1)), (0, y), (w, y))
    return surf


class Glow:
    """Quarter-resolution additive light layer, blurred and added over the frame."""
    K = 4

    def __init__(self):
        self.surf = pygame.Surface((W // self.K, H // self.K))

    def clear(self):
        self.surf.fill((0, 0, 0))

    def circle(self, color, pos, r):
        pygame.draw.circle(self.surf, color, (pos[0] / self.K, pos[1] / self.K), max(1, r / self.K))

    def rect(self, color, rect):
        r = pygame.Rect(rect)
        pygame.draw.rect(self.surf, color, (r.x // self.K, r.y // self.K,
                                            max(1, r.w // self.K), max(1, r.h // self.K)))

    def line(self, color, p0, p1, w):
        k = self.K
        pygame.draw.line(self.surf, color, (p0[0] / k, p0[1] / k), (p1[0] / k, p1[1] / k), max(1, int(w / k)))

    def apply(self, screen):
        small = pygame.transform.smoothscale(self.surf, (W // 16, H // 16))
        soft = pygame.transform.smoothscale(small, (W // self.K, H // self.K))
        soft.blit(self.surf, (0, 0), special_flags=pygame.BLEND_ADD)
        screen.blit(pygame.transform.smoothscale(soft, (W, H)), (0, 0), special_flags=pygame.BLEND_ADD)


# ---------------------------------------------------------------- simulation
class Race:
    def __init__(self, spec, rng):
        (self.abbr, self.state, dem, gop, lean, votes_k, close, held, special) = spec
        self.names = [dem or "Independent", gop]
        self.parties = ["D" if dem else "I", "R"]
        self.held, self.special, self.close = held, special, close
        self.true_margin = lean + rng.gauss(0, 4.2)
        self.expected = votes_k * 1000 * rng.uniform(0.9, 1.08)
        self.precincts = int(votes_k * 1.15 * rng.uniform(0.85, 1.15)) + 40
        self.start = close + rng.uniform(0.03, 0.22)
        span = rng.uniform(2.2, 4.4) * (0.75 + 0.25 * min(1.0, votes_k / 3000))
        self.end = min(self.start + span, NIGHT_END - 0.4)
        self.final = 1.0 if rng.random() < 0.7 else rng.uniform(0.975, 0.995)
        self.bias = rng.gauss(0, 6.5)  # early-vote lean that reverses as the count goes on
        m = abs(self.true_margin)
        if m >= 13:
            self.call_at = -1.0  # projected at poll close
        elif m >= 0.35:
            self.call_at = min(clamp(0.97 - m * 0.055, 0.22, 0.97), self.final)
        else:
            self.call_at = None  # never called tonight
        self.votes = [0.0, 0.0]
        self.shown = [0.0, 0.0]
        self.frac = 0.0
        self.shown_frac = 0.0
        self.next_dump = self.start
        self.history = []
        self.called = None
        self.flash = 0.0

    # -- simulation
    def target(self, h):
        x = clamp((h - self.start) / (self.end - self.start), 0.0, 1.0)
        return (1 - (1 - x) ** 2.3) * self.final

    def update(self, h, rng):
        if h < self.close:
            return False
        if h >= self.next_dump and h >= self.start:
            tgt = self.target(h)
            if tgt > self.frac + 1e-5:
                p, d = self.frac, tgt - self.frac
                m = self.true_margin + self.bias * (1 - 2 * (p + d / 2)) + rng.gauss(0, 3.0)
                r_share = clamp(0.5 + m / 200, 0.03, 0.97)
                n = d * self.expected
                self.votes[1] += n * r_share
                self.votes[0] += n * (1 - r_share)
                self.frac = tgt
                self.history.append((self.frac, self.margin(self.votes)))
            self.next_dump = h + rng.uniform(0.012, 0.09)
        if self.called is None and self.call_at is not None and self.frac >= self.call_at - 1e-9:
            self.called = 1 if self.true_margin > 0 else 0
            self.flash = 1.0
            return True
        return False

    def smooth(self, dt):
        k = 1 - math.exp(-dt * 2.6)
        for i in range(2):
            self.shown[i] += (self.votes[i] - self.shown[i]) * k
        self.shown_frac += (self.frac - self.shown_frac) * k
        self.flash = max(0.0, self.flash - dt * 0.6)

    # -- views
    @staticmethod
    def margin(v):
        t = v[0] + v[1]
        return (v[1] - v[0]) / t * 100 if t > 0 else 0.0

    @property
    def total(self):
        return self.shown[0] + self.shown[1]

    @property
    def m(self):
        """Displayed margin, R minus D (or minus independent) in points."""
        return self.margin(self.shown)

    def pct(self, i):
        t = self.total
        return self.shown[i] / t * 100 if t > 0 else 0.0

    @property
    def reporting(self):
        return self.total > 1

    @property
    def leader(self):
        if not self.reporting:
            return None
        return 0 if self.shown[0] >= self.shown[1] else 1

    def party_of(self, i):
        return self.parties[i]

    @property
    def lead_party(self):
        return None if self.leader is None else self.parties[self.leader]

    @property
    def pct_in(self):
        return self.shown_frac * 100

    @property
    def precincts_in(self):
        return min(self.precincts, int(self.precincts * (self.shown_frac / self.final) ** 0.85 + 0.5)) \
            if self.shown_frac > 0 else 0

    def status(self, h):
        if h < self.close:
            return f"POLLS CLOSE {fmt_hour(self.close)}"
        if self.called is not None:
            return "PROJECTED WINNER"
        if not self.reporting:
            return "AWAITING RESULTS"
        if h >= self.end and self.frac >= self.final - 1e-6:
            return "TOO CLOSE TO CALL"
        return "TOO EARLY TO CALL" if self.frac < 0.4 else "TOO CLOSE TO CALL"

    def outcome(self):
        if self.called is None:
            return ""
        p = self.parties[self.called]
        return f"{PARTY[p]['short']} {'HOLD' if p == self.held else 'GAIN'}"


class Night:
    def __init__(self, seed, cycle):
        self.rng = random.Random(seed)
        self.races = [Race(s, self.rng) for s in RACES]
        self.cycle = cycle
        self.t = 0.0

    @property
    def hour(self):
        return NIGHT_START + (NIGHT_END - NIGHT_START) * min(1.0, self.t / self.cycle)

    @property
    def done(self):
        return self.t >= self.cycle

    def update(self, dt):
        self.t += dt
        h = self.hour
        calls = [r for r in self.races if r.update(h, self.rng)]
        for r in self.races:
            r.smooth(dt)
        return calls

    def seats(self):
        s = {"D": NOT_UP["D"], "R": NOT_UP["R"], "I": 0,
             "Dc": 0, "Rc": 0, "Ic": 0, "Dl": 0, "Rl": 0, "Il": 0, "open": 0}
        for r in self.races:
            if r.called is not None:
                p = r.parties[r.called]
                s[p] += 1
                s[p + "c"] += 1
            elif r.lead_party:
                s[r.lead_party + "l"] += 1
            else:
                s["open"] += 1
        return s


# ---------------------------------------------------------------- layout
HEADER_H = 96
TICKER_Y = 1028
P_BAL = pygame.Rect(24, 112, 640, 410)
P_SPOT = pygame.Rect(24, 538, 640, 476)
P_MAP = pygame.Rect(682, 112, 582, 456)
P_FAD = pygame.Rect(682, 584, 582, 430)
P_BOARD = pygame.Rect(1282, 112, 614, 600)
P_SPEC = pygame.Rect(1282, 728, 614, 286)


class Dashboard:
    def __init__(self, args):
        self.args = args
        self.seed = args.seed
        self.glow = Glow()
        self.time = 0.0
        self.paused = False
        self.bg = self.build_background()
        self.scan = self.build_scanlines()
        self.sweep = self.build_sweep()
        self.fader_cap = self.build_fader_cap()
        self.board_fade = self.build_board_fade()
        self.spot_surf = pygame.Surface((P_SPOT.w, P_SPOT.h - 40), pygame.SRCALPHA)
        self.seats_xy = self.hemicycle_layout()
        self.new_night()

    # ------------------------------------------------------------ state
    def new_night(self):
        seed = None if self.seed is None else self.seed
        self.night = Night(seed, self.args.cycle)
        if self.seed is not None:
            self.seed += 1
        self.hold = 0.0
        self.fade = 1.0
        self.fade_dir = -1
        self.seat_cols = [NEUTRAL] * 100
        self.ctrl_spring = Spring(50, k=30, c=7)
        self.spot = self.night.races[6]  # Georgia: first big poll close
        self.spot_timer = 0.0
        self.spot_alpha = 1.0
        self.spot_next = None
        self.needle = Spring(math.pi / 2, k=40, c=6)
        self.fader_races = []
        self.fader_springs = [Spring(0, k=50, c=7) for _ in range(5)]
        self.fader_timer = 0.0
        self.board_off = 0.0
        self.spec_y = {}
        self.pending = []
        self.banner = None
        self.banner_cool = 0.0
        self.ticker_items = deque()
        self.ticker_x = 300.0
        self.ticker_idx = 0
        self.flow = deque([0.0] * 56, maxlen=56)
        self.flow_acc = 0.0
        self.flow_prev = 0.0
        self.flow_rate = 0.0
        self.pick_faders(force=True)

    def update(self, dt):
        self.time += dt
        if self.paused:
            return
        n = self.night
        if self.fade_dir != 0:
            self.fade = clamp(self.fade + self.fade_dir * dt / 1.2, 0, 1)
            if self.fade_dir > 0 and self.fade >= 1:
                self.new_night()
                return
            if self.fade_dir < 0 and self.fade <= 0:
                self.fade_dir = 0
        calls = n.update(dt)
        self.pending.extend(calls)
        if n.done:
            self.hold += dt
            if self.hold > self.args.hold and self.fade_dir == 0:
                self.fade_dir = 1

        # spotlight rotation
        self.spot_timer += dt
        if self.spot_next is None and self.spot_timer > self.args.spotlight:
            self.spot_next = self.pick_spotlight()
        if self.spot_next is not None:
            self.spot_alpha -= dt / 0.35
            if self.spot_alpha <= 0:
                self.spot, self.spot_next = self.spot_next, None
                self.spot_timer = 0.0
                self.spot_alpha = 0.0
        elif self.spot_alpha < 1:
            self.spot_alpha = min(1.0, self.spot_alpha + dt / 0.35)
        m = clamp(self.spot.m if self.spot.reporting else 0, -20, 20)
        self.needle.target = math.pi / 2 - (m / 20) * (math.pi / 2 - 0.05)
        self.needle.update(dt)

        # faders
        self.fader_timer += dt
        if self.fader_timer > 7:
            self.pick_faders()
        for sp, r in zip(self.fader_springs, self.fader_races):
            sp.target = clamp(r.m if r.reporting else 0, -10.5, 10.5)
            sp.update(dt)

        # balance of power
        s = n.seats()
        proj = s["D"] + s["Dl"] + s["I"] + s["Il"]
        self.ctrl_spring.target = proj
        self.ctrl_spring.update(dt)
        targets = self.seat_targets(s)
        k = 1 - math.exp(-dt * 5)
        self.seat_cols = [mix(c, t, k) for c, t in zip(self.seat_cols, targets)]

        # board scroll
        self.board_off += dt * 26

        # vote-flow meter
        self.flow_acc += dt
        if self.flow_acc >= 0.25:
            tot = sum(r.total for r in n.races)
            rate = max(0.0, tot - self.flow_prev) / self.flow_acc * 60
            self.flow_prev = tot
            self.flow_acc = 0.0
            self.flow.append(rate)
            self.flow_rate = lerp(self.flow_rate, rate, 0.3)

        # projection banner
        self.banner_cool = max(0.0, self.banner_cool - dt)
        if self.banner is not None:
            self.banner["t"] += dt
            if self.banner["t"] > self.banner["dur"]:
                self.banner = None
                self.banner_cool = 1.2
        elif self.pending and self.banner_cool <= 0:
            races, self.pending = self.pending, []
            self.banner = {"races": races, "t": 0.0, "dur": 5.5 if len(races) == 1 else 6.5}

        # ticker
        self.ticker_x -= dt * 120
        while self.ticker_items and self.ticker_x + self.ticker_items[0].get_width() < 290:
            self.ticker_x += self.ticker_items.popleft().get_width()
        end = self.ticker_x + sum(s.get_width() for s in self.ticker_items)
        while end < W + 200:
            item = self.ticker_item(n.races[self.ticker_idx % len(n.races)])
            self.ticker_idx += 1
            self.ticker_items.append(item)
            end += item.get_width()

    def pick_spotlight(self):
        h = self.night.hour
        pool = [r for r in self.night.races if h >= r.close and r is not self.spot]
        if not pool:
            pool = sorted((r for r in self.night.races if r is not self.spot), key=lambda r: r.close)[:3]
        weights = []
        for r in pool:
            if r.called is None and r.reporting:
                w = 4 + 14 / (1 + abs(r.m))
            elif r.called is None:
                w = 2
            else:
                w = 1 + (3 if r.flash > 0 else 0)
            weights.append(w)
        return random.choices(pool, weights)[0]

    def pick_faders(self, force=False):
        self.fader_timer = 0.0
        rs = self.night.races
        live = sorted((r for r in rs if r.reporting and r.called is None), key=lambda r: abs(r.m))
        rest = sorted((r for r in rs if r not in live),
                      key=lambda r: (r.called is not None, abs(r.true_margin if not r.reporting else r.m)))
        new = (live + rest)[:5]
        if force or not self.fader_races:
            self.fader_races = new
            return
        # keep columns stable: only replace races that dropped out
        keep = [r if r in new else None for r in self.fader_races]
        incoming = [r for r in new if r not in keep]
        self.fader_races = [r if r is not None else incoming.pop(0) for r in keep]

    # ------------------------------------------------------------ prerender
    def build_background(self):
        bg = pygame.Surface((W, H))
        g = vgradient((W, H), (6, 11, 26), (3, 6, 16))
        bg.blit(g, (0, 0))
        grid = pygame.Surface((W, H), pygame.SRCALPHA)
        for x in range(0, W, 48):
            pygame.draw.line(grid, (60, 110, 200, 14), (x, 0), (x, H))
        for y in range(0, H, 48):
            pygame.draw.line(grid, (60, 110, 200, 14), (0, y), (W, y))
        bg.blit(grid, (0, 0))
        # radial vignette / centre bloom
        v = pygame.Surface((64, 36), pygame.SRCALPHA)
        for y in range(36):
            for x in range(64):
                d = math.hypot((x - 31.5) / 32, (y - 17.5) / 18)
                v.set_at((x, y), (0, 0, 0, int(clamp((d - 0.55) * 230, 0, 170))))
        bg.blit(pygame.transform.smoothscale(v, (W, H)), (0, 0))
        bloom = pygame.Surface((64, 36))
        for y in range(36):
            for x in range(64):
                d = math.hypot((x - 31.5) / 32, (y - 14) / 18)
                k = clamp(1 - d, 0, 1) ** 2
                bloom.set_at((x, y), (int(8 * k), int(20 * k), int(44 * k)))
        bg.blit(pygame.transform.smoothscale(bloom, (W, H)), (0, 0), special_flags=pygame.BLEND_ADD)

        # header band
        hb = vgradient((W, HEADER_H), (14, 26, 58, 245), (6, 12, 30, 245))
        bg.blit(hb, (0, 0))
        stripe = pygame.Surface((W, 3))
        for x in range(W):
            t = x / W
            c = mix(GOP, (230, 230, 255), t * 2) if t < 0.5 else mix((230, 230, 255), DEM, (t - 0.5) * 2)
            pygame.draw.line(stripe, c, (x, 0), (x, 2))
        bg.blit(stripe, (0, HEADER_H - 3))

        # ticker band
        tb = vgradient((W, H - TICKER_Y), (12, 22, 48, 255), (4, 8, 20, 255))
        bg.blit(tb, (0, TICKER_Y))
        pygame.draw.line(bg, ACCENT, (0, TICKER_Y), (W, TICKER_Y), 2)

        panels = [(P_BAL, "BALANCE OF POWER", "U.S. SENATE · 100 SEATS"),
                  (P_SPOT, "RACE SPOTLIGHT", "LIVE TRACKER"),
                  (P_MAP, "SENATE MAP", "35 SEATS UP · 2 SPECIAL"),
                  (P_FAD, "CLOSEST RACES", "MARGIN FADERS · ±10 PTS"),
                  (P_BOARD, "ALL RACES", "RESULTS BOARD"),
                  (P_SPEC, "MARGIN SPECTRUM", "EVERY RACE, LIVE · RING = CALLED")]
        for rect, title, sub in panels:
            self.panel_frame(bg, rect, title, sub)
        return bg.convert()

    def panel_frame(self, bg, rect, title, sub):
        p = vgradient(rect.size, (16, 30, 62, 215), (7, 13, 30, 230))
        pygame.draw.rect(p, (70, 120, 200, 70), p.get_rect(), 1, border_radius=6)
        strip = vgradient((rect.w - 2, 38), (30, 60, 120, 150), (14, 28, 60, 60))
        p.blit(strip, (1, 1))
        pygame.draw.line(p, (60, 140, 230, 140), (1, 39), (rect.w - 2, 39))
        bg.blit(p, rect)
        L = 16
        for (x, y, dx, dy) in [(rect.left, rect.top, 1, 1), (rect.right - 1, rect.top, -1, 1),
                               (rect.left, rect.bottom - 1, 1, -1), (rect.right - 1, rect.bottom - 1, -1, -1)]:
            pygame.draw.line(bg, ACCENT, (x, y), (x + dx * L, y), 2)
            pygame.draw.line(bg, ACCENT, (x, y), (x, y + dy * L), 2)
        pygame.draw.rect(bg, ACCENT, (rect.x + 14, rect.y + 13, 4, 14))
        r = T.draw(bg, title, 20, "b", TEXT, (rect.x + 26, rect.y + 9), spacing=3)
        T.draw(bg, sub, 14, "m", DIM, (r.right + 14, rect.y + 14), spacing=2)
        for i in range(4):
            pygame.draw.rect(bg, (40, 90, 160) if i < 3 else ACCENT, (rect.right - 54 + i * 10, rect.y + 16, 6, 8))

    def build_scanlines(self):
        s = pygame.Surface((W, H), pygame.SRCALPHA)
        for y in range(0, H, 3):
            pygame.draw.line(s, (0, 0, 0, 34), (0, y), (W, y))
        return s.convert_alpha()

    def build_sweep(self):
        bw, skew = 360, 0.35
        s = pygame.Surface((bw + int(H * skew), H))
        row = pygame.Surface((bw, 1))
        for x in range(bw):
            k = math.sin(math.pi * x / bw) ** 2
            row.set_at((x, 0), (int(6 * k), int(12 * k), int(22 * k)))
        for y in range(H):
            s.blit(row, (int((H - y) * skew), y))
        return s.convert()

    def build_fader_cap(self):
        w, h = 58, 30
        s = pygame.Surface((w, h), pygame.SRCALPHA)
        g = vgradient((w, h), (215, 222, 236, 255), (92, 102, 122, 255))
        mask = pygame.Surface((w, h), pygame.SRCALPHA)
        pygame.draw.rect(mask, (255, 255, 255, 255), (0, 0, w, h), border_radius=5)
        g.blit(mask, (0, 0), special_flags=pygame.BLEND_RGBA_MIN)
        s.blit(g, (0, 0))
        for i in range(-2, 3):
            if i == 0:
                continue
            y = h // 2 + i * 5
            pygame.draw.line(s, (60, 66, 80), (8, y), (w - 9, y))
            pygame.draw.line(s, (240, 244, 252), (8, y + 1), (w - 9, y + 1))
        pygame.draw.rect(s, (40, 44, 56), (0, 0, w, h), 1, border_radius=5)
        return s

    def build_board_fade(self):
        h = 36
        top = vgradient((P_BOARD.w - 4, h), (10, 18, 40, 255), (10, 18, 40, 0))
        bot = vgradient((P_BOARD.w - 4, h), (8, 14, 32, 0), (8, 14, 32, 255))
        return top, bot

    def hemicycle_layout(self, n=100, rows=6, r_in=118, r_out=252):
        radii = [r_in + (r_out - r_in) * i / (rows - 1) for i in range(rows)]
        tot = sum(radii)
        counts = [round(n * r / tot) for r in radii]
        counts[-1] += n - sum(counts)
        seats = []
        for r, c in zip(radii, counts):
            for j in range(c):
                a = math.pi * (1 - j / (c - 1))
                seats.append((a, r))
        seats.sort(key=lambda s: (-s[0], s[1]))
        cx, cy = P_BAL.x + 320, P_BAL.y + 312
        return [(cx + r * math.cos(a), cy - r * math.sin(a)) for a, r in seats]

    def seat_targets(self, s):
        order = ([scale(DEM, 0.55)] * NOT_UP["D"] + [DEM] * s["Dc"] + [IND] * (s["Ic"]) +
                 [("lead", DEM)] * s["Dl"] + [("lead", IND)] * s["Il"] + [NEUTRAL] * s["open"] +
                 [("lead", GOP)] * s["Rl"] + [GOP] * s["Rc"] + [scale(GOP, 0.55)] * NOT_UP["R"])
        out = []
        for c in order[:100]:
            out.append(mix(c[1], (24, 32, 52), 0.62) if isinstance(c[0], str) else c)
        return out

    # ------------------------------------------------------------ draw
    def draw(self, screen):
        self.glow.clear()
        screen.blit(self.bg, (0, 0))
        sx = (self.time * 160) % (W + 1400) - 900
        screen.blit(self.sweep, (int(sx), 0), special_flags=pygame.BLEND_ADD)
        self.draw_header(screen)
        self.draw_balance(screen)
        self.draw_spotlight(screen)
        self.draw_map(screen)
        self.draw_faders(screen)
        self.draw_board(screen)
        self.draw_spectrum(screen)
        self.draw_ticker(screen)
        self.draw_banner(screen)
        self.glow.apply(screen)
        screen.blit(self.scan, (0, 0))
        if self.fade > 0:
            f = pygame.Surface((W, H))
            f.set_alpha(int(255 * self.fade))
            screen.blit(f, (0, 0))
        if self.paused:
            T.draw(screen, "PAUSED", 18, "b", GOLD, (W - 30, HEADER_H + 2), "topright", spacing=3)

    # -- header
    def draw_header(self, s):
        n = self.night
        # emblem
        cx, cy = 62, 48
        aa_circle(s, (12, 24, 56), (cx, cy), 34)
        arc_band(s, GOP, (cx, cy), 27, 33, math.pi / 2, 3 * math.pi / 2)
        arc_band(s, DEM, (cx, cy), 27, 33, -math.pi / 2, math.pi / 2)
        star = []
        for i in range(10):
            a = math.pi / 2 + i * math.pi / 5
            r = 19 if i % 2 == 0 else 8
            star.append((cx + r * math.cos(a), cy - r * math.sin(a) + 1))
        aa_poly(s, TEXT, star)
        self.glow.circle((10, 30, 60), (cx, cy), 40)
        T.draw(s, self.args.title, 50, "b", TEXT, (110, 4))
        T.draw(s, self.args.subtitle, 18, "s", GOLD, (112, 62), spacing=4)

        # odometer
        total = sum(r.total for r in n.races)
        T.draw(s, "TOTAL VOTES COUNTED", 14, "s", DIM, (668, 12), spacing=3)
        self.draw_odometer(s, total, 668, 34)

        # vote flow equalizer
        x0, y0, bw = 1080, 82, 4
        T.draw(s, "VOTE FLOW", 14, "s", DIM, (x0, 12), spacing=3)
        T.draw(s, f"{fmt_rate(self.flow_rate)} / MIN", 14, "b", ACCENT, (x0 + 280, 12), "topright", spacing=1)
        peak = max(max(self.flow), 1.0)
        for i, v in enumerate(self.flow):
            hgt = max(2, int(44 * (v / peak) ** 0.7))
            for seg in range(0, hgt, 4):
                c = mix(ACCENT, (255, 255, 255), seg / 44)
                pygame.draw.rect(s, c, (x0 + i * 5, y0 - seg - 3, bw, 3))
        self.glow.rect((0, 40, 60), (x0, y0 - 44, 280, 44))

        # live badge + clock
        pulse = 0.5 + 0.5 * math.sin(self.time * 4)
        badge = pygame.Rect(1430, 22, 96, 38)
        pygame.draw.rect(s, LIVE_RED, badge, border_radius=4)
        aa_circle(s, mix((255, 160, 160), (255, 255, 255), pulse), (badge.x + 20, badge.centery), 7)
        T.draw(s, "LIVE", 28, "b", (255, 255, 255), (badge.x + 34, badge.centery), "midleft", spacing=2)
        self.glow.rect((int(90 + 80 * pulse), 4, 10), badge.inflate(10, 6))
        T.draw(s, f"{fmt_hour(n.hour)} ET", 48, "b", TEXT, (W - 24, 2), "topright")
        reported = sum(r.precincts_in for r in n.races)
        allp = sum(r.precincts for r in n.races)
        T.draw(s, f"PRECINCTS {fmt_int(reported)} / {fmt_int(allp)}", 15, "s", DIM, (1430, 68), spacing=2)
        if not self.args.no_sim_label:
            T.draw(s, "SIMULATED RESULTS", 13, "s", FAINT, (W - 24, 70), "topright", spacing=2)

    def draw_odometer(self, s, value, x, y):
        cw, ch, gap = 32, 50, 3
        digits = 9
        cx = x
        for k in range(digits - 1, -1, -1):
            cell = pygame.Rect(cx, y, cw, ch)
            pygame.draw.rect(s, (4, 8, 18), cell, border_radius=3)
            pygame.draw.rect(s, (50, 80, 130), cell, 1, border_radius=3)
            p = 10 ** k
            v = value / p
            d = int(v) % 10
            lower = value - int(v) * p
            roll = clamp(lower - (p - 1), 0, 1)
            col = TEXT if value >= p or k == 0 else FAINT
            prev_clip = s.get_clip()
            s.set_clip(cell.inflate(-2, -2))
            off = int(roll * ch)
            T.draw(s, str(d), 46, "b", col, (cell.centerx, cell.centery - off), "center")
            T.draw(s, str((d + 1) % 10), 46, "b", col, (cell.centerx, cell.centery - off + ch), "center")
            s.set_clip(prev_clip)
            # drum shading
            box(s, (cell.x + 1, cell.y + 1, cw - 2, 10), (0, 0, 0, 120))
            box(s, (cell.x + 1, cell.bottom - 11, cw - 2, 10), (0, 0, 0, 120))
            pygame.draw.line(s, (0, 0, 0), (cell.x + 1, cell.centery), (cell.right - 2, cell.centery))
            cx += cw + gap
            if k in (6, 3):
                T.draw(s, ",", 40, "b", DIM, (cx + 1, y + ch - 12), "bottomleft")
                cx += 12
        self.glow.rect((0, 26, 44), (x, y, cx - x, ch))

    # -- balance of power
    def draw_balance(self, s):
        n = self.night
        st = n.seats()
        p = P_BAL
        cx, cy = p.x + 320, p.y + 312
        for (x, y), c in zip(self.seats_xy, self.seat_cols):
            aa_circle(s, c, (x, y), 11)
            aa_ring(s, scale(c, 1.35), (x, y), 11)
        # recently called seats glow
        for i, ((x, y), c) in enumerate(zip(self.seats_xy, self.seat_cols)):
            if c in (DEM, GOP):
                self.glow.circle(scale(c, 0.12), (x, y), 14)
        # control line
        pygame.draw.line(s, GOLD, (cx, cy - 266), (cx, cy - 100), 2)
        d_tot = st["D"]
        r_tot = st["R"]
        T.draw(s, "DEM", 18, "b", DEM_LT, (cx - 52, cy - 78), "midtop", spacing=3)
        T.draw(s, "GOP", 18, "b", GOP_LT, (cx + 52, cy - 78), "midtop", spacing=3)
        T.draw(s, str(d_tot), 62, "b", TEXT, (cx - 52, cy + 4), "midbottom")
        T.draw(s, str(r_tot), 62, "b", TEXT, (cx + 52, cy + 4), "midbottom")
        pygame.draw.line(s, FAINT, (cx, cy - 70), (cx, cy - 6))
        self.glow.circle(scale(DEM, 0.18), (cx - 52, cy - 30), 40)
        self.glow.circle(scale(GOP, 0.18), (cx + 52, cy - 30), 40)
        if st["I"]:
            T.draw(s, f"IND {st['I']}", 16, "b", IND, (cx, cy + 6), "midtop")

        # side readouts
        T.draw(s, "NOT UP", 13, "s", DIM, (p.x + 22, p.y + 54), spacing=2)
        T.draw(s, f"D {NOT_UP['D']}  ·  R {NOT_UP['R']}", 18, "b", TEXT, (p.x + 22, p.y + 70))
        T.draw(s, "CALLED", 13, "s", DIM, (p.right - 22, p.y + 54), "topright", spacing=2)
        T.draw(s, f"D {st['Dc'] + st['Ic']}  ·  R {st['Rc']}", 18, "b", TEXT, (p.right - 22, p.y + 70), "topright")
        T.draw(s, "CONTROL", 13, "s", GOLD, (p.x + 22, p.y + 128), spacing=2)
        T.draw(s, f"D {DEM_CONTROL} · R 50 + VP", 18, "b", GOLD, (p.x + 22, p.y + 144))
        net = sum((1 if r.parties[r.called] != "R" else -1) for r in n.races
                  if r.called is not None and r.parties[r.called] != r.held)
        if net == 0:
            ntxt, nc = "NO NET CHANGE", DIM
        else:
            ntxt, nc = (f"DEM NET +{net}", DEM_LT) if net > 0 else (f"GOP NET +{-net}", GOP_LT)
        T.draw(s, ntxt, 18, "b", nc, (p.right - 22, p.y + 96), "topright")
        T.draw(s, f"UNDECIDED {st['open'] + st['Dl'] + st['Rl'] + st['Il']}", 18, "b", TEXT,
               (p.x + 22, p.y + 96))

        # tug-of-war lever
        tr = pygame.Rect(p.x + 40, p.y + 344, 560, 16)
        pygame.draw.rect(s, (14, 22, 42), tr.inflate(4, 4), border_radius=9)
        u = tr.w / 100
        x = tr.x
        segs = [(st["D"] + st["I"], DEM), (st["Dl"] + st["Il"], scale(DEM, 0.5)), (st["open"], (40, 50, 72)),
                (st["Rl"], scale(GOP, 0.5)), (st["R"], GOP)]
        for cnt, col in segs:
            w = cnt * u
            if w > 0:
                pygame.draw.rect(s, col, (int(x), tr.y, int(math.ceil(w)), tr.h))
            x += w
        for i in range(0, 101, 10):
            pygame.draw.line(s, (6, 10, 22), (tr.x + i * u, tr.y), (tr.x + i * u, tr.bottom - 1))
        lx = tr.x + 50.5 * u
        pygame.draw.line(s, GOLD, (lx, tr.y - 8), (lx, tr.bottom + 8), 3)
        # knob: projected Dem seats if every current leader holds
        kx = tr.x + clamp(self.ctrl_spring.v, 0, 100) * u
        aa_poly(s, TEXT, [(kx, tr.y - 3), (kx - 10, tr.y - 18), (kx + 10, tr.y - 18)])
        aa_circle(s, (220, 228, 244), (kx, tr.bottom + 13), 9)
        aa_circle(s, (90, 100, 124), (kx, tr.bottom + 13), 4)
        pygame.draw.line(s, TEXT, (kx, tr.y - 3), (kx, tr.bottom + 4), 2)
        self.glow.circle((50, 50, 60), (kx, tr.centery), 18)
        proj_d = st["D"] + st["Dl"] + st["I"] + st["Il"]
        proj_r = st["R"] + st["Rl"]
        if proj_d >= DEM_CONTROL:
            txt, col = f"D {proj_d} – R {proj_r} · DEM CONTROL", DEM_LT
        elif proj_r >= 50:
            txt, col = f"D {proj_d} – R {proj_r} · GOP CONTROL", GOP_LT
        else:
            txt, col = f"D {proj_d} – R {proj_r} · {st['open']} NOT REPORTING", TEXT
        T.draw(s, "IF CURRENT LEADS HOLD:", 13, "s", DIM, (tr.x, tr.bottom + 26), spacing=2)
        T.draw(s, txt, 15, "b", col, (tr.x + 172, tr.bottom + 24))
        lx = tr.right
        for lab, col in (("LEADING", scale(DEM, 0.5)), ("CALLED / NOT UP", DEM)):
            r = T.draw(s, lab, 13, "s", DIM, (lx, tr.bottom + 26), "topright", spacing=1)
            pygame.draw.rect(s, col, (r.x - 16, r.y + 3, 10, 10))
            lx = r.x - 28

    # -- spotlight
    def draw_spotlight(self, s):
        r = self.spot
        n = self.night
        surf = self.spot_surf
        surf.fill((0, 0, 0, 0))
        ox, oy = P_SPOT.x, P_SPOT.y + 40
        glow_ok = self.spot_alpha > 0.9
        sw = P_SPOT.w

        T.draw(surf, r.state.upper(), T.fit(r.state.upper(), 470, 52), "b", TEXT, (24, 2))
        sub = "U.S. SENATE" + (" · SPECIAL ELECTION" if r.special else "")
        sub += f" · PRECINCTS {fmt_int(r.precincts_in)} / {fmt_int(r.precincts)}"
        T.draw(surf, sub, 15, "s", DIM, (26, 64), spacing=2)

        # expected-vote ring
        rc = (sw - 52, 46)
        arc_band(surf, (30, 44, 74), rc, 26, 34, 0, 2 * math.pi)
        frac = r.shown_frac
        if frac > 0.002:
            arc_band(surf, ACCENT, rc, 26, 34, math.pi / 2 - 2 * math.pi * frac, math.pi / 2)
        T.draw(surf, f"{r.pct_in:.0f}%", 22, "b", TEXT, (rc[0], rc[1] - 2), "center")
        T.draw(surf, "EXP. VOTE IN", 12, "s", DIM, (rc[0], rc[1] + 38), "midtop", spacing=1)
        if glow_ok:
            self.glow.circle((0, 40, 60), (ox + rc[0], oy + rc[1]), 36)

        # swingometer dial
        c = (sw // 2, 262)
        R = 132
        segs = 40
        for i in range(segs):
            a0 = math.pi - math.pi * i / segs
            a1 = math.pi - math.pi * (i + 1) / segs
            t = (i + 0.5) / segs
            col = mix(DEM, (210, 216, 236), t * 2) if t < 0.5 else mix((210, 216, 236), GOP, (t - 0.5) * 2)
            col = scale(col, 0.35 + 0.65 * abs(t - 0.5) * 2) if abs(t - 0.5) > 0.03 else (120, 126, 150)
            arc_band(surf, col, c, R - 22, R, a1 + 0.012, a0 - 0.012)
        arc_band(surf, (30, 42, 70), c, R - 34, R - 28, 0, math.pi)
        a = self.needle.v
        # filled margin wedge from centre to needle
        if r.reporting:
            lc = PARTY[r.lead_party]["color"]
            lo, hi = (a, math.pi / 2) if a < math.pi / 2 else (math.pi / 2, a)
            if hi - lo > 0.01:
                arc_band(surf, lc, c, R - 34, R - 28, lo, hi)
        for k in range(-20, 21, 5):
            ta = math.pi / 2 - (k / 20) * (math.pi / 2 - 0.05)
            r0, r1 = (R + 4, R + 14) if k % 10 == 0 else (R + 4, R + 9)
            p0 = (c[0] + r0 * math.cos(ta), c[1] - r0 * math.sin(ta))
            p1 = (c[0] + r1 * math.cos(ta), c[1] - r1 * math.sin(ta))
            pygame.draw.aaline(surf, DIM, p0, p1)
            if k % 10 == 0 and abs(k) < 20:
                lab = "EVEN" if k == 0 else (f"D+{-k}" if k < 0 else f"R+{k}")
                lp = (c[0] + (R + 26) * math.cos(ta), c[1] - (R + 26) * math.sin(ta))
                T.draw(surf, lab, 14, "s", DIM, lp, "center")
        tip = (c[0] + (R - 6) * math.cos(a), c[1] - (R - 6) * math.sin(a))
        back = (c[0] - 22 * math.cos(a), c[1] + 22 * math.sin(a))
        nx, ny = -math.sin(a), -math.cos(a)
        aa_poly(surf, (250, 250, 255), [tip, (back[0] + nx * 5, back[1] + ny * 5), (back[0] - nx * 5, back[1] - ny * 5)])
        aa_circle(surf, (60, 70, 92), c, 18)
        aa_circle(surf, (200, 208, 226), c, 13)
        aa_circle(surf, (110, 120, 140), c, 6)
        if glow_ok:
            self.glow.line((70, 70, 80), (ox + c[0], oy + c[1]), (ox + tip[0], oy + tip[1]), 6)
            self.glow.circle((90, 90, 100), (ox + tip[0], oy + tip[1]), 10)

        # margin + status
        if r.reporting:
            lp = r.lead_party
            T.draw(surf, margin_label(lp, r.m), 34, "b", PARTY[lp]["light"], (c[0], c[1] + 20), "midtop")
        else:
            T.draw(surf, "— —", 34, "b", FAINT, (c[0], c[1] + 20), "midtop")
        status = r.status(n.hour)
        won = r.called is not None
        pc = PARTY[r.parties[r.called]]["color"] if won else (36, 50, 82)
        tw = T.font(16, "b").size(status)[0] + 34
        pill = pygame.Rect(0, 0, tw, 28)
        pill.midtop = (c[0], c[1] + 64)
        pygame.draw.rect(surf, pc, pill, border_radius=14)
        T.draw(surf, status, 16, "b", TEXT, pill.center, "center", spacing=2)

        # candidate cards
        for i in (0, 1):
            self.draw_candidate(surf, r, i, i == 0, ox, oy, glow_ok)

        # margin history sparkline
        gx, gy, gw, gh = 24, 380, sw - 48, 50
        pygame.draw.rect(surf, (8, 14, 30), (gx, gy, gw, gh))
        pygame.draw.line(surf, FAINT, (gx, gy + gh / 2), (gx + gw, gy + gh / 2))
        for q in range(1, 4):
            pygame.draw.line(surf, (20, 30, 54), (gx + gw * q / 4, gy), (gx + gw * q / 4, gy + gh))
        hist = r.history
        if len(hist) > 1:
            mx = max(5.0, max(abs(m) for _, m in hist))
            pts = [(gx + gw * clamp(f / max(r.final, 1e-3), 0, 1), gy + gh / 2 + clamp(m / mx, -1, 1) * gh / 2 * 0.9)
                   for f, m in hist]
            for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
                col = GOP if (y0 + y1) / 2 > gy + gh / 2 else DEM
                pygame.draw.line(surf, col, (x0, y0), (x1, y1), 3)
            aa_circle(surf, TEXT, pts[-1], 4)
        T.draw(surf, "MARGIN AS VOTE COMES IN", 12, "s", DIM, (gx + 4, gy + 2), spacing=2)
        T.draw(surf, "D", 12, "b", DEM_LT, (gx + gw - 4, gy + 2), "topright")
        T.draw(surf, "R", 12, "b", GOP_LT, (gx + gw - 4, gy + gh - 2), "bottomright")

        surf.set_alpha(int(255 * clamp(self.spot_alpha, 0, 1)))
        s.blit(surf, (ox + int((1 - self.spot_alpha) * 14), oy))

    def draw_candidate(self, surf, r, i, left, ox, oy, glow_ok):
        p = r.parties[i]
        P = PARTY[p]
        name = r.names[i]
        sw = surf.get_width()
        x = 22 if left else sw - 22
        anchor = "topleft" if left else "topright"
        ax = x + 30 if left else x - 30
        ay = 132
        winner = r.called == i
        aa_circle(surf, P["dark"], (ax, ay), 30)
        arc_band(surf, P["color"], (ax, ay), 26, 30, 0, 2 * math.pi)
        T.draw(surf, initials(name) if name != "Independent" else "IND", 24, "b", TEXT, (ax, ay), "center")
        if winner:
            bx, by = ax + (22 if left else -22), ay - 22
            aa_circle(surf, (255, 255, 255), (bx, by), 12)
            pygame.draw.lines(surf, P["color"], False, [(bx - 6, by), (bx - 1, by + 5), (bx + 7, by - 5)], 3)
            if glow_ok:
                self.glow.circle(scale(P["color"], 0.6), (ox + ax, oy + ay), 40)
        tx = x + 70 if left else x - 70
        T.draw(surf, P["short"], 15, "b", P["light"], (tx, 112), anchor, spacing=3)
        if name in INCUMBENTS:
            T.draw(surf, "INCUMBENT", 12, "s", DIM, (tx, 132), anchor, spacing=1)
        first = " ".join(name.split()[:-1]) if name not in SURNAMES else name[: -len(SURNAMES[name])].strip()
        last = surname(name)
        T.draw(surf, first.upper(), 16, "s", DIM, (x, 172), anchor, spacing=1)
        T.draw(surf, last, T.fit(last, 160, 30), "b", TEXT, (x, 190), anchor)
        pct = r.pct(i)
        col = P["light"] if r.leader == i else TEXT
        T.draw(surf, f"{pct:.1f}%" if r.reporting else "--", 52, "b", col, (x, 222), anchor)
        T.draw(surf, f"{fmt_int(r.shown[i])} VOTES", 16, "s", DIM, (x, 288), anchor, spacing=1)
        bw = 150
        bx = x if left else x - bw
        pygame.draw.rect(surf, (24, 34, 60), (bx, 312, bw, 6))
        fw = int(bw * pct / 100)
        pygame.draw.rect(surf, P["color"], (bx if left else bx + bw - fw, 312, fw, 6))

    # -- map
    def draw_map(self, s):
        n = self.night
        p = P_MAP
        size, step = 46, 50
        x0, y0 = p.x + 16, p.y + 50
        by_abbr = {r.abbr: r for r in n.races}
        h = n.hour
        for ab, (col, row) in TILES.items():
            rect = pygame.Rect(x0 + col * step, y0 + row * step, size, size)
            r = by_abbr.get(ab)
            if r is None:
                pygame.draw.rect(s, (16, 24, 44), rect, border_radius=4)
                T.draw(s, ab, 15, "s", (44, 58, 88), rect.center, "center")
                continue
            if r.called is not None:
                pc = PARTY[r.parties[r.called]]["color"]
                pygame.draw.rect(s, pc, rect, border_radius=4)
                pygame.draw.rect(s, scale(pc, 1.4), rect, 1, border_radius=4)
                T.draw(s, ab, 20, "b", (255, 255, 255), (rect.centerx, rect.centery - 5), "center")
                T.draw(s, "CALLED", 10, "b", (255, 255, 255), (rect.centerx, rect.bottom - 4),
                       "midbottom", spacing=1)
                if r.flash > 0:
                    k = r.flash
                    pygame.draw.rect(s, mix(pc, (255, 255, 255), 0.5 * k), rect, border_radius=4)
                    T.draw(s, ab, 20, "b", (255, 255, 255), (rect.centerx, rect.centery - 5), "center")
                    grow = int((1 - k) * 26)
                    pygame.draw.rect(s, mix((0, 0, 0), (255, 255, 255), k), rect.inflate(grow, grow), 2,
                                     border_radius=6)
                    self.glow.rect(scale(pc, k), rect.inflate(20, 20))
                continue
            pygame.draw.rect(s, (12, 20, 40), rect, border_radius=4)
            if h < r.close:
                pygame.draw.rect(s, (56, 70, 100), rect, 1, border_radius=4)
                T.draw(s, ab, 20, "b", (110, 124, 156), (rect.centerx, rect.centery - 5), "center")
                T.draw(s, fmt_hour(r.close).replace(":00", "").replace(" ", ""), 11, "s", (90, 104, 136),
                       (rect.centerx, rect.bottom - 4), "midbottom")
                continue
            if r.reporting:
                lc = PARTY[r.lead_party]["color"]
                strength = 0.35 + 0.65 * clamp(abs(r.m) / 15, 0, 1)
                fh = int(size * clamp(r.shown_frac, 0.04, 1))
                fill = pygame.Rect(rect.x, rect.bottom - fh, size, fh)
                pygame.draw.rect(s, scale(lc, 0.25 + 0.45 * strength), fill,
                                 border_bottom_left_radius=4, border_bottom_right_radius=4,
                                 border_top_left_radius=4 if fh >= size - 2 else 0,
                                 border_top_right_radius=4 if fh >= size - 2 else 0)
                pygame.draw.line(s, scale(lc, 1.3), (fill.x + 2, fill.y), (fill.right - 3, fill.y), 2)
                pygame.draw.rect(s, lc, rect, 2, border_radius=4)
                T.draw(s, ab, 20, "b", TEXT, (rect.centerx, rect.centery - 5), "center")
                T.draw(s, f"{r.pct_in:.0f}%", 11, "b", TEXT, (rect.centerx, rect.bottom - 4), "midbottom")
            else:
                pulse = 0.5 + 0.5 * math.sin(self.time * 3 + col)
                pygame.draw.rect(s, mix((60, 80, 120), (160, 190, 240), pulse), rect, 2, border_radius=4)
                T.draw(s, ab, 20, "b", TEXT, (rect.centerx, rect.centery - 5), "center")
                T.draw(s, "0%", 11, "s", DIM, (rect.centerx, rect.bottom - 4), "midbottom")

        # summary block in the empty top-left of the grid
        called = [r for r in n.races if r.called is not None]
        dc = sum(1 for r in called if r.parties[r.called] != "R")
        rcnt = len(called) - dc
        flips = sum(1 for r in called if r.parties[r.called] != r.held)
        bx, by = x0 + step + 6, y0 + 2
        T.draw(s, "RACES CALLED", 13, "s", DIM, (bx, by), spacing=2)
        T.draw(s, f"{len(called)}/35", 34, "b", TEXT, (bx, by + 16))
        T.draw(s, "DEM", 13, "s", DIM, (bx + 110, by), spacing=2)
        T.draw(s, str(dc), 34, "b", DEM_LT, (bx + 110, by + 16))
        T.draw(s, "GOP", 13, "s", DIM, (bx + 170, by), spacing=2)
        T.draw(s, str(rcnt), 34, "b", GOP_LT, (bx + 170, by + 16))
        T.draw(s, "FLIPS", 13, "s", DIM, (bx + 230, by), spacing=2)
        T.draw(s, str(flips), 34, "b", GOLD, (bx + 230, by + 16))
        reporting = sum(1 for r in n.races if r.reporting and r.called is None)
        T.draw(s, f"COUNTING {reporting}", 13, "s", DIM, (bx + 290, by), spacing=2)
        bar = pygame.Rect(bx + 290, by + 30, 100, 10)
        pygame.draw.rect(s, (24, 34, 60), bar)
        avg = sum(r.shown_frac * r.expected for r in n.races) / sum(r.expected for r in n.races)
        pygame.draw.rect(s, ACCENT, (bar.x, bar.y, int(bar.w * avg), bar.h))
        T.draw(s, f"{avg * 100:.0f}% OF EXPECTED VOTE", 12, "s", DIM, (bar.x, bar.bottom + 4), spacing=1)

        # legend in the empty bottom row
        lx, ly = x0 + 4 * step + 4, y0 + 7 * step + 6
        items = [((DEM, None), "CALLED D"), ((GOP, None), "CALLED R"),
                 ((scale(DEM, 0.55), DEM), "LEADING"), ((None, (110, 124, 156)), "POLLS OPEN")]
        for i, ((fillc, edge), lab) in enumerate(items):
            ix = lx + (i % 2) * 96
            iy = ly + (i // 2) * 18
            sw = pygame.Rect(ix, iy + 2, 11, 11)
            if fillc:
                pygame.draw.rect(s, fillc, sw)
            if edge:
                pygame.draw.rect(s, edge, sw, 1)
            T.draw(s, lab, 12, "s", DIM, (ix + 16, iy), spacing=1)

    # -- faders
    def draw_faders(self, s):
        p = P_FAD
        n = self.night
        colw = (p.w - 30) / 5
        top, bot = p.y + 128, p.y + 344
        mid = (top + bot) / 2
        half = (bot - top) / 2
        for i, (r, sp) in enumerate(zip(self.fader_races, self.fader_springs)):
            cx = int(p.x + 15 + colw * (i + 0.5))
            if i:
                xx = int(p.x + 15 + colw * i)
                pygame.draw.line(s, (26, 40, 70), (xx, p.y + 54), (xx, p.bottom - 14))
            T.draw(s, r.abbr, 36, "b", TEXT, (cx, p.y + 46), "midtop")
            T.draw(s, r.state.upper(), T.fit(r.state.upper(), colw - 10, 14, "s"), "s", DIM,
                   (cx, p.y + 88), "midtop", spacing=1)
            # LED ladders
            nseg = 11
            m = sp.v
            for side in (-1, 1):
                for k in range(nseg):
                    yy = mid + side * (6 + k * (half - 6) / nseg)
                    lit = r.reporting and ((m < 0 and side < 0) or (m > 0 and side > 0)) and \
                        abs(m) / 10 * nseg > k
                    base = DEM if side < 0 else GOP
                    if r.reporting and r.leader is not None and r.parties[0] == "I" and side < 0:
                        base = IND
                    col = base if lit else scale(base, 0.16)
                    for lx in (cx - 32, cx + 20):
                        pygame.draw.rect(s, col, (lx, int(yy) - 3, 12, 6), border_radius=1)
                    if lit and k == int(abs(m) / 10 * nseg - 1e-6):
                        self.glow.circle(scale(base, 0.5), (cx - 26, yy), 8)
                        self.glow.circle(scale(base, 0.5), (cx + 26, yy), 8)
            # track
            pygame.draw.rect(s, (2, 4, 10), (cx - 4, top - 6, 8, bot - top + 12), border_radius=4)
            pygame.draw.rect(s, (50, 64, 96), (cx - 4, top - 6, 8, bot - top + 12), 1, border_radius=4)
            pygame.draw.line(s, GOLD, (cx - 14, mid), (cx + 14, mid), 1)
            # cap
            yy = mid + clamp(sp.v, -10.5, 10.5) / 10 * half
            cap = self.fader_cap
            s.blit(cap, (cx - cap.get_width() // 2, int(yy) - cap.get_height() // 2))
            lc = PARTY[r.lead_party]["color"] if r.reporting else (120, 130, 150)
            pygame.draw.line(s, lc, (cx - 24, int(yy)), (cx + 23, int(yy)), 3)
            self.glow.line(scale(lc, 0.7), (cx - 24, yy), (cx + 24, yy), 8)
            # readouts
            if r.reporting:
                lp = r.lead_party
                T.draw(s, margin_label(lp, r.m), 26, "b", PARTY[lp]["light"], (cx, p.y + 368), "midtop")
            else:
                T.draw(s, r.status(n.hour).replace("POLLS CLOSE ", "CLOSES "), 13, "s", DIM,
                       (cx, p.y + 376), "midtop")
            bar = pygame.Rect(cx - 40, p.y + 404, 80, 5)
            pygame.draw.rect(s, (24, 34, 60), bar)
            pygame.draw.rect(s, ACCENT, (bar.x, bar.y, int(bar.w * r.shown_frac), bar.h))
            if r.called is not None:
                T.draw(s, f"CALLED · {r.pct_in:.0f}% IN", 12, "b", GOLD, (cx, bar.bottom + 3), "midtop", spacing=1)
            else:
                T.draw(s, f"{r.pct_in:.0f}% IN", 12, "s", DIM, (cx, bar.bottom + 3), "midtop", spacing=1)

    # -- board
    def draw_board(self, s):
        p = P_BOARD
        n = self.night
        hdr_y = p.y + 46
        T.draw(s, "STATE", 12, "s", DIM, (p.x + 16, hdr_y), spacing=2)
        T.draw(s, "DEMOCRAT", 12, "s", DEM_LT, (p.x + 74, hdr_y), spacing=2)
        T.draw(s, "MARGIN", 12, "s", DIM, (p.x + 306, hdr_y), "midtop", spacing=2)
        T.draw(s, "REPUBLICAN", 12, "s", GOP_LT, (p.x + 538, hdr_y), "topright", spacing=2)
        T.draw(s, "IN", 12, "s", DIM, (p.right - 16, hdr_y), "topright", spacing=2)
        pygame.draw.line(s, (40, 70, 120), (p.x + 10, hdr_y + 20), (p.right - 10, hdr_y + 20))
        area = pygame.Rect(p.x + 2, hdr_y + 22, p.w - 4, p.bottom - hdr_y - 26)
        rh = 58
        races = n.races
        off = self.board_off % (rh * len(races))
        first = int(off // rh)
        prev = s.get_clip()
        s.set_clip(area)
        y = area.y - (off - first * rh)
        idx = first
        while y < area.bottom:
            self.draw_board_row(s, races[idx % len(races)], p.x, int(y), p.w, rh, idx % 2 == 0)
            y += rh
            idx += 1
        top, bot = self.board_fade
        s.blit(top, (area.x, area.y))
        s.blit(bot, (area.x, area.bottom - bot.get_height()))
        s.set_clip(prev)

    def draw_board_row(self, s, r, x, y, w, h, alt):
        n = self.night
        if alt:
            box(s, (x + 8, y + 2, w - 16, h - 4), (255, 255, 255, 7))
        sb = pygame.Rect(x + 12, y + 6, 48, h - 12)
        if r.called is not None:
            pc = PARTY[r.parties[r.called]]["color"]
            pygame.draw.rect(s, pc, sb, border_radius=4)
        else:
            pygame.draw.rect(s, (14, 22, 44), sb, border_radius=4)
            ec = PARTY[r.lead_party]["color"] if r.reporting else (60, 74, 104)
            pygame.draw.rect(s, ec, sb, 2, border_radius=4)
        T.draw(s, r.abbr, 22, "b", TEXT, sb.center, "center")

        dn, rn = surname(r.names[0]), surname(r.names[1])
        dp, rp = r.pct(0), r.pct(1)
        dcol = PARTY[r.parties[0]]
        T.draw(s, dn, T.fit(dn, 130, 16, "s"), "s", DIM if r.called != 0 else dcol["light"], (x + 74, y + 7))
        T.draw(s, rn, T.fit(rn, 130, 16, "s"), "s", DIM if r.called != 1 else GOP_LT, (x + 538, y + 7), "topright")
        if r.reporting:
            T.draw(s, f"{dp:.1f}", 28, "b", TEXT if r.leader != 0 else dcol["light"], (x + 74, y + 23))
            T.draw(s, f"{rp:.1f}", 28, "b", TEXT if r.leader != 1 else GOP_LT, (x + 538, y + 23), "topright")
        else:
            T.draw(s, "—", 28, "b", FAINT, (x + 74, y + 23))
            T.draw(s, "—", 28, "b", FAINT, (x + 538, y + 23), "topright")
        # split bar
        bx, bw, byy = x + 222, 168, y + 20
        pygame.draw.rect(s, (22, 32, 56), (bx, byy, bw, 9))
        if r.reporting:
            dw = int(bw * dp / 100)
            pygame.draw.rect(s, dcol["color"], (bx, byy, dw, 9))
            pygame.draw.rect(s, GOP, (bx + dw, byy, bw - dw, 9))
        pygame.draw.line(s, (255, 255, 255), (bx + bw // 2, byy - 3), (bx + bw // 2, byy + 11), 1)
        if r.called is not None:
            st = r.outcome()
            T.draw(s, st, 13, "b", GOLD if "GAIN" in st else TEXT, (bx + bw // 2, byy + 15), "midtop", spacing=2)
        else:
            st = r.status(n.hour)
            T.draw(s, st, 12, "s", DIM, (bx + bw // 2, byy + 16), "midtop", spacing=1)
        # % in
        T.draw(s, f"{r.pct_in:.0f}%", 22, "b", TEXT if r.reporting else FAINT, (x + w - 16, y + 9), "topright")
        if r.called is not None:
            cx, cy = x + w - 30, y + 42
            aa_circle(s, GOLD, (cx, cy), 8)
            pygame.draw.lines(s, (20, 20, 30), False, [(cx - 4, cy), (cx - 1, cy + 3), (cx + 4, cy - 3)], 2)

    # -- spectrum
    def draw_spectrum(self, s):
        p = P_SPEC
        n = self.night
        ax0, ax1 = p.x + 34, p.right - 34
        ay = p.y + 166
        rng = 30
        mid = (ax0 + ax1) / 2
        box(s, (ax0, p.y + 52, int(mid - ax0), 214), (*DEM, 18))
        box(s, (int(mid), p.y + 52, int(ax1 - mid), 214), (*GOP, 18))
        for k in range(-rng, rng + 1, 10):
            x = mid + k / rng * (ax1 - mid)
            pygame.draw.line(s, (40, 56, 90) if k else GOLD, (x, p.y + 52), (x, p.y + 264), 1)
            lab = "EVEN" if k == 0 else (f"D+{-k}" if k < 0 else f"R+{k}")
            T.draw(s, lab, 12, "s", DIM, (x, p.y + 268), "midtop", spacing=1)
        pygame.draw.line(s, (60, 80, 120), (ax0, ay), (ax1, ay), 1)
        live = [r for r in n.races if r.reporting]
        live.sort(key=lambda r: r.m)
        lanes = [0, -1, 1, -2, 2, -3, 3, -4, 4]
        placed = {}
        for r in live:
            x = mid + clamp(r.m, -rng - 2, rng + 2) / rng * (ax1 - mid)
            for ln in lanes:
                if all(abs(x - px) > 28 for px in placed.get(ln, [])):
                    placed.setdefault(ln, []).append(x)
                    break
            else:
                ln = 0
            ty = ay + ln * 27
            cy = self.spec_y.get(r.abbr, ty)
            cy += (ty - cy) * 0.15
            self.spec_y[r.abbr] = cy
            lc = PARTY[r.lead_party]["color"]
            if r.called is not None:
                aa_circle(s, lc, (x, cy), 13)
                aa_ring(s, (255, 255, 255), (x, cy), 13)
                aa_ring(s, (255, 255, 255), (x, cy), 14)
            else:
                k = 0.35 + 0.5 * r.shown_frac
                aa_circle(s, scale(lc, k), (x, cy), 13)
                aa_ring(s, scale(lc, 1.4), (x, cy), 13)
            T.draw(s, r.abbr, 14, "b", TEXT, (x, cy), "center")
            if r.flash > 0:
                self.glow.circle(scale(lc, r.flash), (x, cy), 30)
        T.draw(s, "DEMOCRAT", 13, "b", DEM_LT, (ax0 + 4, p.y + 50), spacing=2)
        T.draw(s, "REPUBLICAN", 13, "b", GOP_LT, (ax1 - 4, p.y + 50), "topright", spacing=2)

    # -- ticker
    def ticker_item(self, r):
        h = self.night.hour
        parts = [(f"  {r.state.upper()}  ", 26, "b", TEXT)]
        if r.reporting:
            for i in (0, 1):
                p = r.parties[i]
                lead = r.leader == i
                parts.append((f"{surname(r.names[i])} ", 26, "s", PARTY[p]["light"]))
                parts.append((f"{r.pct(i):.1f}%   ", 26, "b",
                              TEXT if lead else DIM))
            parts.append((f"{r.pct_in:.0f}% IN", 20, "s", ACCENT))
        else:
            parts.append((f"{surname(r.names[0])} vs {surname(r.names[1])}   ", 26, "s", DIM))
            parts.append((r.status(h), 20, "s", GOLD))
        if r.called is not None:
            parts.append((f"   {r.outcome()}", 20, "b", GOLD))
        parts.append(("      •    ", 26, "b", (60, 90, 140)))
        surfs = [T.render(t, sz, w, c) for t, sz, w, c in parts]
        hh = max(x.get_height() for x in surfs)
        out = pygame.Surface((sum(x.get_width() for x in surfs), hh), pygame.SRCALPHA)
        x = 0
        for x_s in surfs:
            out.blit(x_s, (x, hh - x_s.get_height() - (4 if x_s.get_height() < hh else 0)))
            x += x_s.get_width()
        return out

    def draw_ticker(self, s):
        y = TICKER_Y + 2
        hgt = H - y
        prev = s.get_clip()
        s.set_clip(pygame.Rect(280, y, W - 280, hgt))
        x = self.ticker_x
        for item in self.ticker_items:
            s.blit(item, (int(x), y + (hgt - item.get_height()) // 2))
            x += item.get_width()
        s.set_clip(prev)
        alert = self.banner is not None
        blink = alert and int(self.time * 3) % 2 == 0
        lab = pygame.Rect(0, y, 280, hgt)
        pygame.draw.rect(s, LIVE_RED if alert else (0, 110, 170), lab)
        pygame.draw.polygon(s, LIVE_RED if alert else (0, 110, 170),
                            [(280, y), (300, y), (280, y + hgt)])
        T.draw(s, "PROJECTION" if alert else "SENATE RESULTS", 26, "b",
               (255, 255, 255) if not blink else (255, 220, 220), lab.center, "center", spacing=3)

    # -- projection banner
    def draw_banner(self, s):
        b = self.banner
        if b is None:
            return
        t, dur = b["t"], b["dur"]
        k = clamp(min(t / 0.45, (dur - t) / 0.4), 0, 1)
        k = 1 - (1 - k) ** 3
        rect = pygame.Rect(24, 936, 1872, 82)
        rect.x = int(24 - (1 - k) * (rect.w + 40))
        box(s, rect, (6, 10, 24, 252))
        pygame.draw.rect(s, GOLD, (rect.x, rect.y, rect.w, 3))
        tag = pygame.Rect(rect.x, rect.y + 3, 300, rect.h - 3)
        pygame.draw.rect(s, LIVE_RED, tag)
        T.draw(s, "PROJECTION", 20, "b", (255, 230, 230), (tag.x + 22, tag.y + 10), spacing=4)
        T.draw(s, fmt_hour(self.night.hour) + " ET", 36, "b", TEXT, (tag.x + 22, tag.y + 32))
        self.glow.rect((90, 10, 16), tag)
        races = b["races"]
        x = tag.right + 26
        if len(races) == 1:
            r = races[0]
            wi = r.called
            P = PARTY[r.parties[wi]]
            pygame.draw.rect(s, P["color"], (x, rect.y + 16, 10, rect.h - 30))
            T.draw(s, r.state.upper(), 48, "b", TEXT, (x + 24, rect.centery + 2), "midleft")
            nx = x + 24 + T.font(48, "b").size(r.state.upper())[0] + 30
            T.draw(s, f"{r.names[wi].upper()} ({P['short']})", 40, "b", P["light"], (nx, rect.centery + 2), "midleft")
            T.draw(s, "WINS U.S. SENATE SEAT" + (" · SPECIAL" if r.special else ""), 18, "s", DIM,
                   (rect.right - 24, rect.y + 16), "topright", spacing=3)
            oc = r.outcome()
            T.draw(s, oc, 34, "b", GOLD if "GAIN" in oc else TEXT, (rect.right - 24, rect.y + 38), "topright", spacing=2)
            self.glow.rect(scale(P["color"], 0.12 * k), (x, rect.y + 20, 14, rect.h - 40))
        else:
            T.draw(s, "PROJECTED WINNERS", 18, "s", DIM, (x, rect.y + 12), spacing=3)
            cx = x
            for r in races[:14]:
                P = PARTY[r.parties[r.called]]
                chip = pygame.Rect(cx, rect.y + 36, 90, 36)
                if chip.right > rect.right - 20:
                    break
                pygame.draw.rect(s, P["color"], chip, border_radius=4)
                T.draw(s, r.abbr, 24, "b", (255, 255, 255), (chip.x + 10, chip.centery), "midleft")
                T.draw(s, surname(r.names[r.called]), T.fit(surname(r.names[r.called]), 46, 13, "s"), "s",
                       (255, 255, 255), (chip.right - 6, chip.centery), "midright")
                cx += 98
            if len(races) > 14:
                T.draw(s, f"+{len(races) - 14}", 28, "b", TEXT, (cx + 6, rect.y + 54), "midleft")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Simulated election-night Senate results wall.")
    ap.add_argument("--windowed", action="store_true", help="run in a resizable window instead of fullscreen")
    ap.add_argument("--cycle", type=float, default=600, help="seconds for a full night of results (default 600)")
    ap.add_argument("--hold", type=float, default=45, help="seconds to hold final results before restarting")
    ap.add_argument("--spotlight", type=float, default=10, help="seconds per spotlight race")
    ap.add_argument("--start", type=float, default=0, help="skip ahead this many seconds into the night")
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--seed", type=int, default=None, help="fix the random scenario")
    ap.add_argument("--title", default="ELECTION NIGHT 2026")
    ap.add_argument("--subtitle", default="BATTLE FOR THE SENATE")
    ap.add_argument("--no-sim-label", action="store_true", help="hide the small 'SIMULATED RESULTS' tag")
    ap.add_argument("--snapshot", metavar="PNG", help="render one frame after --start seconds and exit")
    args = ap.parse_args()

    if args.snapshot:
        os.environ["SDL_VIDEODRIVER"] = "dummy"
    pygame.display.init()
    pygame.font.init()
    pygame.display.set_caption(args.title)
    if args.snapshot:
        screen = pygame.display.set_mode((W, H))
    else:
        flags = pygame.SCALED | (pygame.RESIZABLE if args.windowed else pygame.FULLSCREEN)
        try:
            screen = pygame.display.set_mode((W, H), flags, vsync=1)
        except pygame.error:
            screen = pygame.display.set_mode((W, H), flags)
        pygame.mouse.set_visible(args.windowed)

    global T
    T = Text()
    dash = Dashboard(args)
    dash.fade, dash.fade_dir = 1.0, -1

    step = 1 / 30
    t = 0.0
    while t < args.start:
        dash.update(step)
        t += step

    if args.snapshot:
        for _ in range(int(2 / step)):  # let animations settle
            dash.update(step)
        dash.fade = 0
        dash.draw(screen)
        pygame.image.save(screen, args.snapshot)
        return

    clock = pygame.time.Clock()
    while True:
        for e in pygame.event.get():
            if e.type == pygame.QUIT:
                return
            if e.type == pygame.KEYDOWN:
                if e.key in (pygame.K_ESCAPE, pygame.K_q):
                    return
                if e.key in (pygame.K_f, pygame.K_F11):
                    pygame.display.toggle_fullscreen()
                elif e.key == pygame.K_SPACE:
                    dash.paused = not dash.paused
                elif e.key == pygame.K_n:
                    dash.spot_next = dash.pick_spotlight()
                elif e.key == pygame.K_r:
                    dash.fade_dir = 1
        dt = min(clock.tick(args.fps) / 1000, 0.1)
        dash.update(dt)
        dash.draw(screen)
        pygame.display.flip()


if __name__ == "__main__":
    try:
        main()
    finally:
        pygame.quit()
    sys.exit(0)
