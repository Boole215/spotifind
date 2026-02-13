#!/usr/bin/env python3

import pandas as pd;
import duckdb;

track_df = pd.read_csv("kreitmire.csv", names=["artist_name", "album_title", "track_title", "listened_datetime"]);
track_df = track_df.drop('listened_datetime', axis=1)

# TODO: Do this transform with regex?
track_df['track_title'] = track_df['track_title'].apply(lambda title: title.replace("’", "'").replace("‘", "'"))
# TODO: Find some method of normalizing arist names. E.g., 'カニエ・ウェスト' should be Kanye West
track_df = track_df.groupby(['artist_name', 'album_title', 'track_title']).size().reset_index(name='count').sort_values(by='count', ascending=False);

album_df = track_df.drop('count', axis=1);
album_df = album_df[['artist_name', 'album_title']].groupby(['artist_name', 'album_title']).size().reset_index(name='count').sort_values(by='count', ascending=False);

track_df_sum_counts = track_df.groupby(['artist_name', 'album_title'])["count"].sum().reset_index(name="sum_track_count").sort_values("sum_track_count", ascending=False);
album_df = album_df.merge(track_df_sum_counts.drop('artist_name', axis=1), left_on="album_title", right_on="album_title").sort_values(by='sum_track_count', ascending=False)

# Filter out albums that have been mostly listened to -- if not fully.
album_df = album_df.query("count <= 10");

revisit_albums_df = album_df.query("count >= 5");
fully_listen_to_albums_df = album_df.query("count < 5");

revisit_albums_df['combined_counts'] = revisit_albums_df['count'] * revisit_albums_df['sum_track_count']
fully_listen_to_albums_df['combined_counts'] = fully_listen_to_albums_df['count'] * fully_listen_to_albums_df['sum_track_count']

feature_colums = ['artist_name', 'album_title']
dependent_column = ['combined_counts']


# debug = duckdb.query("SELECT * FROM track_df_sum_counts WHERE album_title like '%Umurangi%'").df();

# print(track_df_sum_counts)
# print(album_df["sum_track_count"].mean())
# print(album_df["sum_track_count"].median())
# print(album_df["count"].mean())
# print(album_df["count"].median())
