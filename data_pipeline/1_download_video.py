import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import cv2
import exchange_calendars as xcals
import numpy as np
import pandas as pd

from config import WIDTH, HEIGHT, NUM_PIXELS, DATA_DIR as OUTPUT_DIR

parser = argparse.ArgumentParser()
parser.add_argument("--start-date", required=True)
parser.add_argument("--first-period", type=int, default=1)
args = parser.parse_args()

print(f"Using W = {WIDTH}, H = {HEIGHT}")
print(f"Start date = {args.start_date}, first period = {args.first_period}")

with tempfile.TemporaryDirectory() as tmpdir:
    subprocess.run([
        sys.executable, "-m", "yt_dlp", "-f", "bestvideo[vcodec^=avc1]/best",
        "-o", f"{tmpdir}/video.%(ext)s", "--no-playlist", "-q", "--no-warnings",
        "https://www.youtube.com/watch?v=FtutLA63Cp8"
    ], check=True)

    cap = cv2.VideoCapture(str(next(Path(tmpdir).glob("video.*"))))
    frames = []
    while (ret := cap.read())[0]:
        gray = cv2.cvtColor(ret[1], cv2.COLOR_BGR2GRAY)
        resized = cv2.resize(gray, (WIDTH, HEIGHT), interpolation=cv2.INTER_AREA)
        frames.append((resized.astype(np.float32) / 255.0).flatten())
    cap.release()

print(f"Extracted {len(frames)} frames from video")

xnas = xcals.get_calendar("XNAS")
schedule = xnas.schedule.loc[args.start_date:"2025-12-31"]

timestamps = []
first_day = True
for _, row in schedule.iterrows():
    t = row["open"] + pd.Timedelta(minutes=15)
    end = row["close"] - pd.Timedelta(minutes=15)
    period_num = 1
    while t <= end:
        if first_day and period_num < args.first_period:
            period_num += 1
            t += pd.Timedelta(minutes=15)
            continue
        timestamps.append(t)
        period_num += 1
        t += pd.Timedelta(minutes=15)
    first_day = False

min_len = min(len(frames), len(timestamps))
timestamps = timestamps[:min_len]
frames = frames[:min_len]
print(f"Aligned to {min_len} frames/timestamps")

data = np.stack(frames)
df = pd.DataFrame({
    "timestamp": timestamps,
    **{f"p{i}": data[:, i] for i in range(NUM_PIXELS)}
})

output_file = OUTPUT_DIR / "bad_apple_frames.parquet"
df.to_parquet(output_file)

print(f"Saved {len(df)} frames to {output_file}")
print(f"First timestamp: {df['timestamp'].iloc[0]}")
print(f"Last timestamp:  {df['timestamp'].iloc[-1]}")
