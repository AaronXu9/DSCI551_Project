import os
import pandas as pd
import numpy as np
import requests
import json
import ast
from datetime import datetime

rootURL = 'https://dsci551final-52aac-default-rtdb.firebaseio.com/'


def preprocess_partition(partition):
    """
    change str to list for genres and production_countries
    Args: partitino: list
    Returns: partition with str converted to list
    """
    for film in partition:
        film['genres'] = [genre['name'] for genre in ast.literal_eval(film['genres'])]
        film['production_countries'] = [coutnry['iso_3166_1'] for coutnry in ast.literal_eval(film['production_countries'])]

    partition = pd.DataFrame(partition)
    partition.release_date = pd.to_datetime(partition.release_date)
    return partition


def preprocess_partition_str(partition):
    """
    change str to list for genres and production_countries
    Args: partitino: list
    Returns: partition with str converted to list
    """
    for film in partition:
        if 'genres' in film.keys() and film['genres']:
            film['genres'] = ','.join([genre['name'] for genre in ast.literal_eval(film['genres'])])
        
        if 'production_countries' in film.keys() and film['production_countries']:
            countries = ast.literal_eval(film['production_countries'])
            if isinstance(countries, list):
                film['production_countries'] = ','.join([coutnry['iso_3166_1'] for coutnry in countries])
    
    partition = pd.DataFrame(partition)
    partition.release_date = pd.to_datetime(partition.release_date, errors='coerce')
    return partition


def filter_by_language(df: pd.DataFrame, language: str):
    """
    filter by language
    """
    return df[df.original_language == language]


def filter_by_rating(df: pd.DataFrame, rating: float):
    """
    filter by rating
    """
    return df[df.vote_average > rating]
    

def filter_by_genre(df: pd.DataFrame, genre: str):
    """
    filter by genre
    """
    return df.loc[df['genres'].str.contains(genre, case=False)]


def filter_by_date(df: pd.DataFrame, s_date=None, e_date=None):
    """
    filter by genre
    """
    if not s_date:
        s_date = df['release_date'].min()
    if not e_date:
        e_date = df['release_date'].max()
    
    return df.loc[(df['release_date'] >= s_date) &  (df['release_date'] <= e_date)]


def pmr(rootURL=rootURL, language=None, rating=None, genre=None, s_date=None, e_date=None):
    """
    get the partitions of the movies data and perform the map reduce function on the partitions 
    """
    try:
        res = requests.get(f'{rootURL}/metadata/root/movies_metadata/partitions.json')
        metadata = json.loads(res.text)
        reduced_df = pd.DataFrame()

        for part_key in metadata.keys():
            """map and reduce on each partition"""
            partition = requests.get(metadata[part_key])
            partition = json.loads(partition.text)
            partition = preprocess_partition_str(partition)

            """filter by the keyword"""
            if language:
                partition = filter_by_language(partition, language=language)
            if rating: 
                partition = filter_by_rating(partition, rating=rating)
            if genre:
                partition = filter_by_genre(partition, genre=genre)
            if s_date or e_date:
                partition = filter_by_date(partition, s_date=s_date, e_date=e_date)
        
            reduced_df = pd.concat([reduced_df, partition],ignore_index=True)

    except:
        print(res.text)
        return None
    
    return reduced_df


def analyze(film_df: pd.DataFrame):
    """
    Analyze the search results from pmr to find the average rating, max-rating film and mimimum rating film
    Args: 
        film_df
    Return: 
        average_rating: float
        max-rating film: pd.Series
        mimimum-rating film: pd.Series 
    """
    max_film = film_df[film_df['vote_average'] == film_df['vote_average'].max()]
    min_film = film_df[film_df['vote_average'] == film_df['vote_average'].min()]

    return film_df['vote_average'].mean(), max_film, min_film


if __name__ == '__main__':
    language = 'en'
    rating= 6
    genre = 'Crime'
    s_date = datetime(2009, 1, 1)
    e_date = datetime(2010, 1, 1)
    df = pmr(rootURL=rootURL, language=language, rating=rating, genre=genre, s_date=s_date, e_date=e_date)
    avg, max_film, min_film = analyze(df)
    print(df)
    print(avg, max_film, max_film)
