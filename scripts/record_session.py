"""
Records a running exchange to a file the website can replay when no exchange is up.

It polls the HTTP gateway the same way the demo page does and stores one frame per tick. Run it while the stack is
trading (for example on the EKS demo day), then copy the output into the website's data/maroof-street/ folder.

    python3 scripts/record_session.py --minutes 10 --out session.json.gz
"""
import argparse
import gzip
import json
import time
import urllib.request

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]


def get(base: str, path: str):
    with urllib.request.urlopen(f"{base}/{path}", timeout=4) as res:
        return json.load(res)


def frame(base: str) -> dict:
    return {
        "t": int(time.time() * 1000),
        "market": get(base, "market"),
        "tape": get(base, "tape?limit=30"),
        "traders": get(base, "traders"),
        "books": {s: get(base, f"book/{s}?levels=10") for s in SYMBOLS},
        "trades": {s: get(base, f"trades/{s}")[:10] for s in SYMBOLS},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8080", help="HTTP gateway address")
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--every", type=float, default=2, help="seconds between frames")
    ap.add_argument("--out", default="session.json.gz")
    args = ap.parse_args()

    frames = []
    deadline = time.time() + args.minutes * 60
    while time.time() < deadline:
        started = time.time()
        frames.append(frame(args.base))
        print(f"\r{len(frames)} frames", end="", flush=True)
        time.sleep(max(0, args.every - (time.time() - started)))

    recording = {"recorded_at": frames[0]["t"], "every_ms": int(args.every * 1000), "frames": frames}
    with gzip.open(args.out, "wt", encoding="utf-8") as f:
        json.dump(recording, f, separators=(",", ":"))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
