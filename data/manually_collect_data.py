#!/usr/bin/env python3

from dataclasses import dataclass, asdict
import pandas as pd
import requests
import json
import time
import sys

@dataclass
class RawArtist:
    mbid: str;
    name: str;

@dataclass
class RawImage:
    size: str;
    text: str;
    
@dataclass
class RawAlbum:
    mbid: str;
    text: str;
    
@dataclass
class RawTrack:
    artist: RawArtist;
    streamable: str;
    image: RawImage;
    mbid: string;
    album: Album;
    name: str;
    url: str;
    attr: bool;

@dataclass
class RawTrack:
    artist_mbid: str;
    artist_name: str;
    mbid: str;
    name: str;
    album_title: str;
    album_mbid: str;

@dataclass
class Album:
    mbid: str;
    title: str;

@dataclass
class Track:
    mbid: str; # Musicbrainz ID
    title: str;
    album_title: str;
    album_mbid: str;    


def createRawTrack(raw_track_data: any) -> RawTrack:
    return RawTrack(
        raw_track_data["artist"]["mbid"],
        raw_track_data["artist"]["#name"],
        raw_track_data["mbid"],
        raw_track_data["name"],
        raw_track_data["album"]["#text"],
        raw_track_data["album"]["mbid"],
    )

def translateRawTrack(raw_track: RawTrack) -> Track:
    return Track(raw_track["mbid"],
                 raw_track["name"],
                 raw_track["album"]["#text"],
                 raw_track["album"]["mbid"])
    
    
def getRecentTracksPageCount() -> str:
    API_KEY = "STAND-IN"
    API_URL = f"http://ws.audioscrobbler.com/2.0/"
    params = {
        "method":"user.getrecenttracks",
        "user":"kreitmire",
        "api_key":API_KEY,
        "format":"json",
        "limit":"200"
    }
    
    r = requests.get(API_URL, params=params);

    if r.status_code == 200:
        data = r.json()
        return data['recenttracks']['@attr']['totalPages']
    else:
        print(f"An error happened while retrieving page count of getRecentTracks. Status code {r.status_code}")
        sys.exit()

def getAllListenedToTracks(total_pages: int) -> [Track]:
    # Total pages is based on a limit size of 200 results per query.
    all_tracks = [];
    API_KEY = "4f0cf49a581dc145e00681c7d6f5fdab"
    API_URL = f"http://ws.audioscrobbler.com/2.0/"
    params = {
        "method":"user.getrecenttracks",
        "user":"kreitmire",
        "api_key":API_KEY,
        "format":"json",
        "limit":"200",
        "page":"0",
    }
    query_counter = 0;
    failure_count = 0;
    for page in range(1, total_pages+1):
        params["page"] = str(page);
        r = requests.get(API_URL, params=params);
        print(f"fetching page {page} of {total_pages}")
        if r.status_code == 200:
            failure_count = 0;
            data = r.json();
            tracks = data["recenttracks"]["track"];
            all_tracks += [translateRawTrack(rt) for rt in tracks];
            query_counter += 1
            if query_counter == 3:
                time.sleep(1);
                query_counter = 0;
        else:
            print(f"An error happened while retrieving page count of getRecentTracks. Status code {r.status_code}")
            time.sleep(5);
            failure_count += 1;
            if failure_count == 5:
                print("5 consecutive failures over 25 seconds, bump up the sleep between queries and try again later.");
                sys.exit();
    return(all_tracks)
 
def constructTrackDataframe(tracks: [Track]):
    df = pd.DataFrame([asdict(t) for t in tracks])
    df.to_csv("kreitmire_all_recent_tracks.csv", index = False)
    print(df)
