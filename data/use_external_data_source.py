#!/usr/bin/env python3

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
import pandas as pd;
import numpy as np;
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

independent_columns = ["artist_name", "album_title"]
feature_columns = ['count', 'sum_track_count', 'combined_counts']


revisit_albums_df["match"] = np.nan
fully_listen_to_albums_df["match"] = np.nan


TRAINING_SET_SIZE = 30
revisit_albums_df_size = len(revisit_albums_df)
fully_listen_to_albums_df_size = len(fully_listen_to_albums_df)

revisit_training_df = revisit_albums_df.sample(30)
fully_listen_training_df = fully_listen_to_albums_df.sample(30)

revisit_training_df.to_csv('revisit_training_manual.csv')
fully_listen_training_df.to_csv('fully_listen_training_manual.csv')

revisit_training_train_df = pd.read_csv("revisit_training_manual_done.csv")
fully_listen_training_train_df = pd.read_csv("fully_listen_training_manual_done.csv")

logreg_revisit = LogisticRegression(solver='liblinear')
logreg_revisit.fit(revisit_training_train_df[feature_columns], revisit_training_train_df['match'])
revisit_albums_df["match"] = logreg_revisit.predict(revisit_albums_df[feature_columns])

logreg_fully_listen = LogisticRegression(solver='liblinear')
logreg_revisit.fit(fully_listen_training_train_df[feature_columns], fully_listen_training_train_df['match'])
fully_listen_to_albums_df["match"] = logreg_revisit.predict(fully_listen_to_albums_df[feature_columns])


print("You should revisit the following albums:")
print(len(revisit_albums_df))
revisit_albums_df = revisit_albums_df.query("match == 1").sort_values("combined_counts", ascending=False)[independent_columns];
print(revisit_albums_df)

print("You should fully listen to the following albums:")
print(len(fully_listen_to_albums_df))
fully_listen_to_albums_df = fully_listen_to_albums_df.query("match == 1").sort_values("combined_counts", ascending=False)[independent_columns];
print(fully_listen_to_albums_df)

