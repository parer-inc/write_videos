"""This service allows to write new videos to db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()


def write_videos(data):
    """Write videos into database (table videos)
       data must be a 2d array - [n][13]"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    try:
        for vid in data:
            if vid is None or len(vid) != 13:
                return False
        q = '''INSERT INTO  videos
                (id, title, views, likes,
                dislikes, comments, description,
                channel_id, duration, published_at,
                tags, default_language, made_for_kids, time)
                VALUES
                (%s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, NOW() );'''
        cursor.executemany(q, data)
    except Exception as error:
        print(error)
        # LOG
        return "Duplicate entry" in error
        # sys.exit("Error:Failed writing new videos to db")
    db.commit()
    return True


if __name__ == '__main__':
    q = Queue('write_videos', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='write_videos')
        worker.work()
