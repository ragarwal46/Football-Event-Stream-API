import os
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import asyncio
import time
import requests
import pandas as pd

app = FastAPI()

# Store plays in memory (simulating "game state")
play_buffer: List[Dict[str, Any]] = []
last_sent_index = 0


# --- Helper: fetch play-by-play from various providers ---
def fetch_play_by_play(home_team, away_team):
    """
    Try multiple sources for play-by-play for the given game_id.
    Returns: list of play dicts (ordered as they occurred).
    Raises: Exception if no data was found.
    """
    global play_buffer, last_sent_index
   
    df = pd.read_csv('pbp-2024.csv')
    game_df = df[((df['OffenseTeam'] == home_team) & (df['DefenseTeam'] == away_team)) | ((df['OffenseTeam'] == away_team) & (df['DefenseTeam'] == home_team))].copy()
    game_df["TotalSecondsRemaining"] = (
    (4 - game_df["Quarter"]) * 15 * 60  # full quarters after current one
    + game_df["Minute"] * 60
    + game_df["Second"])

    # Now sort by descending "time remaining" to get chronological order
    df_sorted = game_df.sort_values("TotalSecondsRemaining", ascending=False)

    for index, row in df_sorted.iterrows():
        play = {
            "quarter": row["Quarter"],
            "time": row["TotalSecondsRemaining"],
            "description": row["Description"],
            "yards_gained": row["Yards"],
            "offense_team": row["OffenseTeam"],
            "defense_team": row["DefenseTeam"],
            "down": row["Down"],
            "yards_to_go": row["ToGo"],
            "yard_line": row["YardLine"]
        }
        play_buffer.append(play)

        

fetch_play_by_play("WAS", "ATL")


async def play_streamer():
    """
    Generator that yields a new play every 15 seconds.
    """
    global last_sent_index
    print("Here")
    while last_sent_index < len(play_buffer):
        play_data = play_buffer[last_sent_index]
        yield f"data: {json.dumps(play_data)}\n\n"
        last_sent_index += 1
        await asyncio.sleep(15)

def play_streamer_2():
    """
    Generator that yields a new play every 15 seconds.
    """
    global last_sent_index
    print("Here")
    while last_sent_index < len(play_buffer):
        play_data = play_buffer[last_sent_index]
        print(f"data: {json.dumps(play_data)}\n\n")
        last_sent_index += 1



@app.get("/stream-plays")
async def stream_plays():
    """
    SSE endpoint to stream plays to the client.
    React Native can subscribe to this endpoint.
    """
    return StreamingResponse(play_streamer(), media_type="text/event-stream")