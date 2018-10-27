# coding=utf-8


import ujson

import redis
from redis import Redis

from listenbrainz.listen import Listen
from listenbrainz.listenstore import ListenStore, MIN_ID
from listenbrainz.webserver.views.api_tools import top_ten_listens

class RedisListenStore(ListenStore):
    def __init__(self, log, conf):
        super(RedisListenStore, self).__init__(log)
        self.log.info('Connecting to redis: %s:%s', conf['REDIS_HOST'], conf['REDIS_PORT'])
        self.redis = Redis(host=conf['REDIS_HOST'], port=conf['REDIS_PORT'], decode_responses=True)

    def get_playing_now(self, user_id):
        """ Return the current playing song of the user

            Arguments:
                user_id (int): the id of the user in the db

            Returns:
                Listen object which is the currently playing song of the user

        """
        data = self.redis.get('playing_now:{}'.format(user_id))
        if not data:
            return None
        data = ujson.loads(data)
        data.update({'listened_at': MIN_ID+1})
        return Listen.from_json(data)

    def check_connection(self):
        """ Pings the redis server to check if the connection works or not """
        try:
            self.redis.ping()
        except redis.exceptions.ConnectionError as e:
            self.log.error("Redis ping didn't work: {}".format(str(e)))
            raise

    def store_listens(self):
        listens_data = []
        data=self.redis.get('listens:{}'.format(
            Listen.from_json(timestamp).first()))
        if not data:
            return None
        data = ujson.loads(data)
        data.update({'listens': listens_data})
        return Listen.from_json(data)


    def store_top_listens(self,top_listens_data):
        top_listens_shelf = []
        data=self.redis.get('latest_listens:{}'.format(top_listens_data)
        top_listens_shelf=data[:10]
        data = ujson.loads(top_listens_shelf)
        return Listen.from_json(data)
