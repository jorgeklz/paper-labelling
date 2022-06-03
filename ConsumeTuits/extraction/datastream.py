# TODO: Check all the TODOs in this file and complete them all

import threading
import tweepy
import os
import json
import re
import random
import pandas as pd
import sys
from datetime import date
from geopy.geocoders import Nominatim
from model.knn import KNNModel
from os.path import exists
from extraction.preproccesing import Preprocessing

# ----------------------------------------------------------------------------------------

# All the cities are in this csv
cities = pd.read_csv('./Listas/ciudades.csv')

geolocator = Nominatim(user_agent="geoapiExercises")


class StreamListener(tweepy.Stream):
    def __init__(self, words):
        self.id_tweet = ''
        self.NUMBERS_MATCH = int(os.getenv("NUMBER_WORD_MATCH"))
        self.cities = [str(x[0]) for x in cities.values]
        self.category = [f'Categoria_{i+1}'for i in range(8)]
        self.knn_model = KNNModel()
        self.encoding = 'utf-16'
        self.daemon = True

        self.word_list = words  # All the emergency keywords
        self.preprocessor = Preprocessing()
        self.lista_name_fields = ['user_id', 'status_id', 'created_at', 'screen_name',
                                  'text', 'status_url', 'lat', 'long', 'place_full_name']

        # defining routes to save CSV and JSON files
        self.define_routes()

        self.should_create_file_csv = False # I just forget what this variable does :'(
        self.should_create_file_tsv = False  # Well... I don't remember why this variavble is needed neither
        self.should_create_file = False  # if the json file should be created or not

        # API key credentials
        self.consumer_key = os.getenv( "TWITTER_CONSUMER_KEY")  # twitter app key
        self.consumer_secret = os.getenv( "TWITTER_CONSUMER_SECRET")  # twitter app secret
        self.access_token = os.getenv("TWITTER_ACCESS_TOKEN")  # twitter key
        self.access_token_secret = os.getenv( "TWITTER_ACCESS_TOKEN_SECRET")  # twitter secret

        super().__init__(
            self.consumer_key, self.consumer_secret,
            self.access_token, self.access_token_secret
        )
# ----------------------------------------------------------------------------------------

    def define_routes(self):
        # Defining tree explorer architecture
        current_date = date.today()
        month = f'0{current_date.month}' if current_date.month < 10  else str(current_date.month)
        self.target_path = f"./data/{current_date.year}/{month}/" # where to save data
        
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)

        # TODO: rename variables to a more representative name
        # ...
        # This is the full JSON with all the keys provided by Twitter. This file increases
        # its size very quick, so always check available RAM before opening this file without
        # an efficient reader such as Pandas.
        self.path = f'{self.target_path}tweets_streaming_{str(date.today())}.json'
        self.path_csv = f'{self.target_path}data_streaming_{str(current_date)}.csv'
        self.path_tsv = f'{self.target_path}data_streaming_preprocessing_{str(current_date)}.csv'

    # [period] needs to be in seconds
    def extract_tweets(self, sample_size=30, thread=None):

        self.define_routes()

        # Since this is an event loop, the main is not always called (only when there is no
        # memory and the OS frees up memory), we need to define routes again

        # TODO: document what this piece of code is doing
        if thread is not None:
            threading.Event().set()

        # Since tweeter doesn't allow you to exceute too many conditions (every single
        # word is a condition) it's necessary to just pick up a little sample from the
        # whole population.
        sample_words = random.sample(self.word_list, sample_size)

        # feedback
        print("Listening tweets with the next matches\n", sample_words)

        # The online box constraint coordinates generatior can be found
        # here:
        # https://boundingbox.klokantech.com/

        # This will filter tweets that match with the sample we just generated

        return self.filter(
            languages=["es"],  # you can search in more than one language
            track=sample_words,
            # [none, low, medium] if you increase it then less tweets will be extracted
            filter_level="low",
            locations=[  # box constraints
                -83.278924317, -6.0320552006, -74.5878673271, 2.6512810757,  # Ecuador, base
            ],
            threaded=True
        )
# ----------------------------------------------------------------------------------------
    def on_status(self, status):
        '''
        This is an automated process to extract tweets and handle errors like allowed API
        call exceed exception
        '''
        if self.id_tweet != status._json['id_str']:
            sys.stdout.flush() # cleaning the buffer
            self.id_tweet = status._json['id_str']

            # If tweet does not have a retweeted status then it is a new tweet
            if not hasattr(status, "retweeted_status"):
                try:
                    location = status.place.full_name \
                            if hasattr( status.place, 'full_name') \
                            else (\
                                status.user.location \
                                if hasattr(status.user, 'location') \
                                else "NA"\
                            )

                    # not from Ecuador. If the location has a comma then it should
                    # always have the word "Ecuador"
                    if len(str(location).split(',')) > 1:
                        if "ecuador" not in str(location).lower():
                            return

                    # avoiding other locations
                    from_ecuador = False
                    for _citie in self.cities:
                        if f"{_citie.lower()}," in str(location).lower():
                            from_ecuador = True
                            break

                    if not from_ecuador:
                        return

                    text = status._json["text"]
                    matches = 0 # number of matches to be considered as emergency 
                                # tweet is defined in the .env file

                    # TODO: analyze this problem and determine if it is a problem
                    # on our side or if it is a problem with tweepy.
                    # ...
                    # This is a second filter. For no reason Twitter is giving
                    # us tweets that don't match with keywords, so, just to be
                    # sure let's apply a second filter
                    for word in self.word_list:
                        if re.search(f"{word}", text, re.I):
                            matches += 1

                    # Every single tweet should have at least [NUMBERS_MATCH] matches
                    if matches < self.NUMBERS_MATCH:
                        return

                    # This is the raw data coming from Tweeter servers
                    self.save_raw_data(status._json)
                    # CSV with relevant fields
                    self.save_csv(status, self.path_csv)
                    # CSV with text preprocessed
                    fields = self.save_csv( status, self.path_tsv, shouldPreprocessing=True)
                    self.save_labelled_csv(
                        processed_fields=fields,
                        path=f"{self.target_path}data_etiquetada_{str(date.today())}.csv",
                        path2=f"{self.target_path}data_etiquetada2_{str(date.today())}.csv"
                    )
                except Exception as e:
                    print(e)

        #TODO: for some reason we need to clean the buffer twice, the first one is at the beginning
        # of this method. Analayze why this is happening.
        sys.stdout.flush()

# ----------------------------------------------------------------------------------------
    def save_raw_data(self, json_data):
        '''
        Saves the full raw JSON coming data from Twitter
        '''
        # If the file doesn't exist then I should write the
        # json structure.
        if not exists(self.path):
            self.should_create_file = True

        with open(self.path, "a+", encoding=self.encoding) as f:
            # [f] is a reference
            self.delete_last_line(f)

            # What you wanna write
            content = ('[\n' if self.should_create_file else ',') + \
                f"{json.dumps(json_data)}\n]"

            if self.should_create_file:
                self.should_create_file = False

            f.write(content)  # replacing the last line
            f.flush()  # ensure file will be full writed before being readed again
            f.close()  # closing the file
# ----------------------------------------------------------------------------------------

    def save_labelled_csv(self, processed_fields, path2='data_etiquetada.csv', path="data_etiquetada2.csv"):
        """
        `processed_fields` should be a formatted string with "|" character as separator
        """
        # Headers of the CSV. All labelled data should contain the class and the institution
        fields = self.lista_name_fields.copy() + ['class', 'institution']

        text_index = 4  # Text field's position in the CSV
        splitted_text = processed_fields.split('|')
        label = self.knn_model.predict_label( splitted_text[text_index].split(' '))

        should_create_file1 = False
        should_create_file2 = False

        if not exists(path):
            should_create_file1 = True

        f = open(path, "a+", encoding=self.encoding)
        if should_create_file1:
            f.write("|".join(fields)+"\n")
            f.flush()
            should_create_file1 = False

        splitted_text += [label, random.sample(self.category, 1)[0]]

        temp_df = pd.DataFrame(columns=fields)
        temp_df.loc[-1] = splitted_text
        row = temp_df.to_csv(sep="|").split("\n-1|")[1]

        f.write(row)
        f.flush()
        f.close()

        if not exists(path2):
            should_create_file2 = True

        del fields[text_index]
        del splitted_text[text_index]

        f2 = open(path2, "a+", encoding=self.encoding)
        if should_create_file2:
            f2.write("|".join(fields)+"\n")
            f2.flush()
            should_create_file2 = False

        temp_df = pd.DataFrame(columns=fields)
        temp_df.loc[-1] = splitted_text
        row = temp_df.to_csv(sep="|").split("\n-1|")[1]

        f2.write(row)
        f2.flush()
        f2.close()
# ----------------------------------------------------------------------------------------

    def save_csv(self, status, path, shouldPreprocessing=False) -> list:
        """
        `status` is a raw json coming from Twitter.
        `path` is where do you want to save the csv (this is the same for labelled data).
        `shouldPreprocessing` should be True if you wanna save a preprocessed CSV file.
        If `make_label` is True then a new CSV is generated with the label of
        emergency in the dataset (`processed_text` cannot be None and shouldPreprocessing 
        should be False).

        This will return the last row writted in the CSV.
        """

        json_data = status._json
        lat = None
        long = None

        if not exists(path):
            self.should_create_file_csv = True

        f = open(path, "a+", encoding=self.encoding)
        if self.should_create_file_csv:
            f.write("|".join(self.lista_name_fields)+"\n")
            f.flush()
            self.should_create_file_csv = False

        location_name = json_data['place']['full_name'] if hasattr(status.place, 'full_name') else (
            json_data['user']['location'] if hasattr(status.user, 'location') else 'NA')
        location_coor = geolocator.geocode(location_name)

        # if hasattr(status.coordinates, 'coordinates'):
        #     lat = status.coordinates.coordinates[0]
        #     long = status.coordinates.coordinates[1]
        # else:
        #     if hasattr(status.place, 'bounding_box'):
        #         lat = status.place.bounding_box.coordinates[0][0][1]
        #         long = status.place.bounding_box.coordinates[0][0][0]
        #     else:
        #         lat = '0'
        #         long = '0'

        text = json_data['extended_tweet']['full_text']if hasattr(
            status, 'extended_tweet') else json_data['text']
        if shouldPreprocessing:
            text = self.preprocessor.clean_data(text)

        fields = [
            f"{json_data['user']['id_str']}",
            f"{json_data['id_str']}",
            f"{json_data['created_at']}",
            f"{json_data['user']['screen_name']}",
            f"{text}",
            f"{str('https://twitter.com/'+json_data['user']['screen_name']+'/status/'+status.id_str)}",
            f"{str(location_coor.latitude)}",
            f"{str(location_coor.longitude)}",
            f"{json_data['place']['full_name'] if hasattr(status.place, 'full_name') else (json_data['user']['location'] if hasattr(status.user, 'location') else 'NA')}"
        ]

        temp_df = pd.DataFrame(columns=self.lista_name_fields)
        temp_df.loc[-1] = fields
        row = temp_df.to_csv(sep="|").split("\n-1|")[1]
        print(row)
        f.write(row)
        f.flush()
        f.close()

        return '|'.join(fields)

# ----------------------------------------------------------------------------------------

    def delete_last_line(self, file):
        # Move the pointer (similar to a cursor in a text editor) to the end of the file
        file.seek(0, os.SEEK_END)

        # This code means the following code skips the very last character in the file -
        # i.e. in the case the last line is null we delete the last line
        # and the penultimate one
        pos = file.tell() - 1

        # Read each character in the file one at a time from the penultimate
        # character going backwards, searching for a newline character
        # If we find a new line, exit the search
        while pos > 0 and file.read(1) != "\n":
            pos -= 1
            file.seek(pos, os.SEEK_SET)

        # So long as we're not at the start of the file, delete all the characters ahead
        # of this position
        if pos > 0:
            file.seek(pos, os.SEEK_SET)
            file.truncate()
# ----------------------------------------------------------------------------------------

    def on_error(self, status_code):
        # status code 420 means Twitter has blocked the API key due to api call limit reached
        # and if that happens then connection should be down for a while
        if status_code == 420:
            print("Api call limit reached, waiting for reconnection. This is automatic.")
            return False
