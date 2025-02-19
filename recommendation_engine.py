##############################################################################################

# This file contain NLP based recommendation engine
# Class Recommendation_Engine can easily separable for future use. 
# Main method is available below to play with key words

##############################################################################################

import nltk
import string # to process standard python strings
import pandas as pd
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from textblob import TextBlob
import time
import inflect
import sqlite3
import pandas as pd
import ast 
import os

class Recommendation_Engine:
    def __init__(self, my_preference = [], datasource = 'countdown'):

        # read data from csv
        # self._read_csv("all_product_data.csv")
        if datasource == 'countdown':
            # read data from database
            self.get_dynamic_data('dynamic_database.db')

            # select relevent features
            self.feature_selection()

            # data pre process
            self.data_pre_processing()

        elif datasource == 'amazon':
            print("amazon object created")

        # initialize of tokanization and vectorization
        self.tokanization()

        self.init_vectorization(my_preference)

        # Initialize inflect engine
        self.inflect_engine = inflect.engine()

    def read_data_file_using_keywords(self, KEYWORD = "None"):

        all_files = os.listdir("Amazon_Data_1/") 
        # print(type(all_files))   
        csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
        
        files_found = list(filter(lambda f: f.startswith(KEYWORD), csv_files))
        print(files_found)

        if len(files_found) != 0:
            print("Data found")
            self._read_csv("Amazon_Data_1/" + files_found[0])
        else:
            print("No data found for", KEYWORD)



    def recommendations_from_keyword_filename(self, KEYWORD, THRESHOLD = 2, USER_PREFERENCE=[]):
        try: 
            # null input condition get recommendation based on preference
            if KEYWORD == '': 
                self.recommendation_list = None
                self.legnth_recommendation_list = 0
                self.empty_flag = True
                return self.recommendation_list, self.legnth_recommendation_list, self.empty_flag
            
            self.read_data_file_using_keywords(KEYWORD)
            
            # select relevent features from amazon data
            self.feature_selection(datasource='amazon')

            # data pre process
            self.data_pre_processing()

            # print(self.dynamic_data)

            # Function collocation manipulate user preference
            KEYWORD, USER_PREFERENCE = self.collocation(KEYWORD, USER_PREFERENCE)
            print("collocation: ", KEYWORD, USER_PREFERENCE)
            # Generate plural form of key word and append into KEYWORD itself
            plural = self.inflect_engine.plural(KEYWORD)
            KEYWORD += " " + plural
            print("Add plural:", KEYWORD)

            # map category with USER_PREFERENCE
            USER_PREFERENCE_TEXT = self.map_user_preference(USER_PREFERENCE)
            # print(USER_PREFERENCE_TEXT)

            KEYWORD = KEYWORD.lower()
            print(KEYWORD)

            # Standard listing without RL
            self.data_list = self.dynamic_data['Product_Title'].values.tolist()
            
            # Priority 4 : (GET all product associated with keyword using title)
            self.dynamic_data['distances'] = self.find_tfidf_and_cosine(self.data_list, KEYWORD)

            # filter distance using THRESHOLD
            self.recommendation_list = self.dynamic_data[self.dynamic_data['distances'] >= THRESHOLD/100].sort_values(by=['distances'], ascending=False)
            
            self.legnth_recommendation_list = len(self.recommendation_list.index)
            
            # for empty recommendation_list :: Helps to dispaly 'no product available with this key word' in site  
            if self.legnth_recommendation_list == 0:
                self.empty_flag = True
            else: 
                self.empty_flag = False
            
            print(self.recommendation_list.head())
            
            try:
                # Data order manipulation with three priority
                self.recommendation_list = self.get_relevance_sorted_product_with_user_priority(self.recommendation_list, KEYWORD +" "+ USER_PREFERENCE_TEXT, datasource = "amazon")
            except Exception as e:
                print('Error in Data order manipulation',e)
                pass


            # self.recommendation_list['colFromIndex'] = self.recommendation_list.index
            # self.recommendation_list = self.recommendation_list.sort_values([preset,'colFromIndex'], ascending = (False, True))
            print(self.recommendation_list.head())

            # remove features and distances column from output data
            del self.recommendation_list['features']
            del self.recommendation_list['distances']
            # del self.recommendation_list['colFromIndex']

        except Exception as e:
            # incase of error in script no recommendation
            self.recommendation_list = None
            self.legnth_recommendation_list = 0
            self.empty_flag = True
            print(e)

        return self.recommendation_list, self.legnth_recommendation_list, self.empty_flag

    def get_dynamic_data(self, dbname = 'dynamic_database.db'):
        # Create your connection.
        try:
            conn_db = sqlite3.connect(dbname)
            self.dynamic_data = pd.read_sql_query("SELECT * FROM product_data", conn_db)
            # print(self.dynamic_data)
            print("dynamic_data LOADED")
        except Exception as e:
            self.dynamic_data = None
            print("dynamic_data NOT LOADED")
            print(e)
            pass

    # get all data into dataframe  
    def _read_csv(self, FILE_NAME):
        try:
            self.dynamic_data = pd.read_csv(FILE_NAME)
            #print(self.dynamic_data.head()) 
        except Exception as e:
            print(e)

    # spell correction method to correct misspelled words of user
    def spell_correction(self, keyword):
        corrected_keyword = TextBlob(keyword)
        return corrected_keyword.correct()

    # data pre processing task on features
    def data_pre_processing(self):
        '''
        select parameter and do preprocessing on data
        '''
        try:
            # remove null records 
            self.dynamic_data['features'].dropna(inplace=True)
            
            # convert to lower case
            self.dynamic_data["features"] = self.dynamic_data["features"].str.lower() 
            #print(self.dynamic_data['features'])
        
        except Exception as e:
            print(e)

    def feature_selection(self, datasource = 'countdown'):
        try:
            if datasource == "countdown":
                self.dynamic_data['features'] = self.dynamic_data['ProductTitle'].astype(str) + ' ' + self.dynamic_data['ProductDetail'].astype(str) + ' ' + self.dynamic_data['Ingredients'].astype(str) + ' ' + self.dynamic_data['ProductPrice'].astype(str) + ' ' + self.dynamic_data['ProductVolume'].astype(str)  + ' ' + self.dynamic_data['Nutritional_information'].astype(str) + ' ' + self.dynamic_data['Allergenwarnings'].astype(str) + ' ' + self.dynamic_data['Claims'].astype(str) + ' ' + self.dynamic_data['Endorsements'].astype(str) + ' ' + self.dynamic_data['Productorigin'].astype(str)
            elif datasource == "amazon":
                self.dynamic_data['features'] = self.dynamic_data['Product_Title'].astype(str) + ' ' + self.dynamic_data['Special_Ingredients'].astype(str) + ' ' + self.dynamic_data['Product_Detail'].astype(str) + ' ' + self.dynamic_data['Product_Description'].astype(str) + ' ' + self.dynamic_data['Product_Info'].astype(str)  + ' ' + self.dynamic_data['Allergen_Info'].astype(str) + ' ' + self.dynamic_data['Important_Information'].astype(str) + ' ' + self.dynamic_data['Rating'].astype(str) + ' ' + self.dynamic_data['Product_Reviews'].astype(str) + ' ' + self.dynamic_data['Product_Price'].astype(str)
            
            # print(self.dynamic_data['features'])
        except Exception as e:
            print(e)

    # this function is use to check tokanization works properly 
    def tokanization(self):
        #print(self.dynamic_data['ProductTitle'])
        try:    
            nltk.sent_tokenize("Hello world")
        except Exception as e:
            # consume some time while first run
            nltk.download('punkt') # use only for first-time
            nltk.download('wordnet') # use only for first-time

    # WordNet is a semantically-oriented dictionary of English included in NLTK.
    def LemmeTokens(self, tokens):
        self.lemmer_obj = nltk.stem.WordNetLemmatizer()
        return [self.lemmer_obj.lemmatize(token) for token in tokens]

    # this method is user for remove lemmas form words: e.g : milks -> milk, eggs -> egg
    def LemNormalize(self, text):
        self.remove_punctuation_dict = dict((ord(punct), None) for punct in string.punctuation)
        return self.LemmeTokens(nltk.word_tokenize(text.lower().translate(self.remove_punctuation_dict)))

    # this function is use to initilize vectorization method
    def init_vectorization(self, my_preference):
        try:
            #self.TfidfVec = TfidfVectorizer(ngram_range=(1, 2),stop_words = "english", lowercase = True, max_features = 500000) 

            # Any preference contain 2 or more word than we initialized n-gram else without n-gram
            if len(my_preference) != 0:
                if any(len(x.split()) > 1 for x in my_preference):
                    print("Found a match, init with n-gram")
                    self.HashVec = HashingVectorizer(ngram_range=(1, 2), stop_words="english", lowercase=True)
                else:
                    print("Not a match, init without n-gram")
                    self.HashVec = HashingVectorizer(stop_words="english", lowercase=True)
            else:
                self.HashVec = HashingVectorizer(stop_words="english", lowercase=True)

        except Exception as e:
            # in case of any Exception
            self.HashVec = HashingVectorizer(stop_words="english", lowercase=True)

            print(e)

    # # general function to find distances using TFIDF
    # def find_tfidf_and_cosine_old(self, input_data, search_data):
    #     # Create list and append user input
    #     input_data.append(str(search_data))
    #     # generate new sparse matrix of tfidf
    #     tfidf = self.TfidfVec.fit_transform(input_data)
    #     # find recommendations on user keyword using cosine similarity
    #     distances = cosine_similarity(tfidf[-1], tfidf)
    #     return distances[0][:-1]

    # NEW function to find distances using TFIDF
    def find_tfidf_and_cosine(self, input_data, search_data):
        # Create list and append user input   
        input_data.append(str(search_data))
        t7 = time.time()
        # generate new sparse matrix of tfidf
        tfidf = self.HashVec.fit_transform(input_data)
        t8 = time.time()
        print("Time for vector generation: ", t8-t7)
        # find recommendations on user keyword using cosine similarity
        t9 = time.time()
        distances = cosine_similarity(tfidf[-1], tfidf)
        t10 = time.time()
        print("Time to find distance: ", t10-t9)

        return distances[0][:-1]

    # Data order manipulation (implementation of priority 1 & 2 rest of product are listed after p2 (i.e, priority 3))
    def get_relevance_sorted_product_with_user_priority(self, recommendation_list, USER_PREFERENCE_TEXT, datasource = 'countdown'):
        
        # detect none user PREFERENCE
        if USER_PREFERENCE_TEXT == '':
            return recommendation_list

        # else modify list using priority
        else:
            USER_PREFERENCE_TEXT = USER_PREFERENCE_TEXT.lower()
            
            if datasource == "countdown":
                # only get predicted_tags + title + Category in single string
                recommendation_list['predicted_tags_string'] = recommendation_list['predicted_tags'].apply(lambda x: ' '.join([str(i) for i in ast.literal_eval(x)])) + ' ' +  recommendation_list['ProductTitle'].astype(str) + ' ' + recommendation_list['Category'].astype(str)
                recommendation_list['features_priority'] = recommendation_list['predicted_tags_string'].astype(str)
                # print(recommendation_list['features_priority'].head())
                
                # convert to list of string for matching
                predicted_tags_data = recommendation_list['features_priority'].values.tolist()
                # print(predicted_tags_data)

                # find distance between predicted_tags and USER_PREFERENCE_TEXT
                recommendation_list['predicted_tags_distances'] = self.find_tfidf_and_cosine(predicted_tags_data, USER_PREFERENCE_TEXT)
                # print(recommendation_list['predicted_tags_distances'])

                # sort list using distance
                recommendation_list = recommendation_list.sort_values(by=['predicted_tags_distances'], ascending=False, ignore_index=True)

                # Delete processed column
                del recommendation_list['features_priority']
                del recommendation_list['predicted_tags_distances']
            
            elif datasource == "amazon":

                recommendation_list['features_priority_1'] = recommendation_list['Product_Title'].astype(str)

                # Priority 1 : user input title + words from USER_PREFERENCE
                title_data = recommendation_list['features_priority_1'].values.tolist()
            
                # find distance with title
                recommendation_list['distances_1'] = self.find_tfidf_and_cosine(title_data, USER_PREFERENCE_TEXT)

                # filter distance using THRESHOLD
                
                # THRESHOLD > 2.1 for get products form only title. (distances_1>=2.1 sava as it is)
                # CASE 1 Output
                recommendation_list_priority1 = recommendation_list[recommendation_list['distances_1'] >= 1.3/100].sort_values(by=['distances_1'], ascending=False)
            
                # # FOR RL
                # # recommendation_list_priority1 = recommendation_list[recommendation_list['distances_1'] >= 1.3/100].sort_values(by=['RL_weights'], ascending=False)

                # CASE 2 / Priority 2 user input + PREFERENCE + other attributes 
                # select column form recommendation_list for priority 2 and further process 
                recommendation_list_priority2 = recommendation_list[recommendation_list['distances_1'] < 1.3/100].sort_values(by=['distances_1'], ascending=False)
                # sort_values(['distances_1', 'RL_weights'], ascending = [False, False], inplace = False)
                #Select other features for case 2
                recommendation_list_priority2['features_priority_2'] = recommendation_list_priority2['Product_Title'].astype(str) + ' ' +recommendation_list_priority2['Special_Ingredients'].astype(str) + ' ' + recommendation_list_priority2['Product_Detail'].astype(str) + ' ' + recommendation_list_priority2['Product_Description'].astype(str) + ' ' + recommendation_list_priority2['Product_Info'].astype(str) + ' ' + recommendation_list_priority2['Allergen_Info'].astype(str) + ' ' + recommendation_list_priority2['Important_Information'].astype(str)+ ' ' + recommendation_list_priority2['Product_Reviews'].astype(str)
                
                other_data = recommendation_list_priority2['features_priority_2'].values.tolist()

                # find distance with other data
                recommendation_list_priority2['distances_2'] = self.find_tfidf_and_cosine(other_data, USER_PREFERENCE_TEXT)

                # sort data CASE 2
                recommendation_list_priority2 = recommendation_list_priority2.sort_values(by=['distances_2'], ascending=False)

                # # FOR RL
                # # recommendation_list_priority2 = recommendation_list_priority2.sort_values(by=['RL_weights'], ascending=False)

                # #print("New LEN: ", len(recommendation_list_priority2.index))
                # #print(recommendation_list_priority2)

                # # merge all data
                recommendation_list_priority1 = recommendation_list_priority1.append(recommendation_list_priority2, ignore_index = True)
                
                # Delete processed column
                del recommendation_list_priority1['features_priority_1']
                del recommendation_list_priority1['distances_1']
                del recommendation_list_priority1['features_priority_2']	
                del recommendation_list_priority1['distances_2']	
            	
        return recommendation_list_priority1

    # prioritize preference add word weights on preferences
    def prioritize_preference(self, USER_PREFERENCE, turnon = True):
        """
        input : USER_PREFERENCE
        output: add weight and return USER_PREFERENCE
        """
        preference_length = len(USER_PREFERENCE)

        my_preference = ''

        if preference_length != 0 and turnon:

            if preference_length == 1 : preference_length = 5
            print("preference_length", preference_length)
            
            for idx, single_preference in enumerate(USER_PREFERENCE):
                
                print(single_preference)
                my_preference += (" " + single_preference.lower()) * (preference_length - idx + 1)

        return my_preference.strip()

    # map category with USER_PREFERENCE
    def map_user_preference(self, USER_PREFERENCE=[]):
        # case: USER_PREFERENCE is none then use main categories only
        if len(USER_PREFERENCE) == 0:
            return ''
        else:
            
            # add weight on USER_PREFERENCE
            my_preference = self.prioritize_preference(USER_PREFERENCE)
            print("my_preference", my_preference)
            
            '''
            for preference in USER_PREFERENCE:
                # print(preference)
                if preference.lower() == "organic":
                    my_preference += ' Fruit & Veg Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Drinks Beer & Wine Ice Cream & Sorbet Health & Beauty'
                elif preference.lower() == "non gmo":
                    my_preference += ' Fruit & Veg Frozen Baby & Child Fridge & Deli Bakery Pantry'
                elif preference.lower() == "pesticide free":
                    my_preference += ' Fruit & Veg Meat & Seafood Baby & Child Fridge & Deli Pantry'
                elif preference.lower() == "free range":
                    my_preference += ' Eggs Meat & Seafood Frozen Meat Fridge & Deli Pantry Pet Cage free'
                elif preference.lower() == "nut free":
                    my_preference += ' Frozen Fridge & Deli Bakery Pantry'
                elif preference.lower() == "dairy free":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet Ice Cream & Sorbet'
                elif preference.lower() == "palm oil free":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet'
                elif preference.lower() == "additives free":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet Ice Cream & Sorbet Health & Beauty'
                elif preference.lower() == "sugar free":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet'
                elif preference.lower() == "gluten free":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet Ice Cream & Sorbet'
                elif preference.lower() == "vegan":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet Ice Cream & Sorbet'
                elif preference.lower() == "halal":
                    my_preference += ' Frozen Meat & Seafood Baby & Child Fridge & Deli Bakery Pantry Pet Ice Cream & Sorbet Health & Beauty'
            my_preference = my_preference.lower()
            '''
        
        return my_preference

     # Check collocation of the preference to improve the accuracy in case of 'Non GMO', 'Sugar Free' etc
    def collocation(self, KEYWORD, USER_PREFERENCE=[]):
        # print(KEYWORD.lower().split(" "))
        other_words = ['no', 'non', 'free', 'zero']
        # loop for all word in user input
        for word in KEYWORD.lower().split(" "):
            # loop to check other_words present or not
            if word in other_words:
                # Auto change user preference based on user text input
                # Check word is present in input KEYWORD
                if (KEYWORD.lower().find('organic') != -1):
                    # if found change preference
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("organic", "").strip()
                        # Remove from preference
                        USER_PREFERENCE.remove('Organic')
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('gmo') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("gmo", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append('Non GMO')
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('pesticide') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("pesticide", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append('Pesticide Free')

                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('range') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("range", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append(['Free Range', 'Cage free'])
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('nut') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("nut", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append('Nut Free')
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('dairy') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("dairy", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append('Dairy Free')
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('oil') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("oil", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append('Palm oil Free')
                    except Exception as e:
                        print(e)
                        pass
                elif (KEYWORD.lower().find('additives') != -1):
                    try:
                        # Remove word
                        KEYWORD = KEYWORD.replace("additives", "").strip()
                        # Append into preference
                        USER_PREFERENCE.append(['Flavour', 'Stabiliser', 'Emulsifier', 'Antioxidant', 'Preservative'])
                    except Exception as e:
                        print(e)
                        pass
                KEYWORD = KEYWORD.replace(word, "").strip()
            else:
                pass
        return KEYWORD, USER_PREFERENCE

    # this function will help to get recommendations
    def recommendations_from_keyword(self, KEYWORD, THRESHOLD = 2, USER_PREFERENCE=[], preset = "RL_weights"):
        try: 
            # null input condition get recommendation based on preference
            if KEYWORD == '': 
                self.recommendation_list = None
                self.legnth_recommendation_list = 0
                self.empty_flag = True
                return self.recommendation_list, self.legnth_recommendation_list, self.empty_flag

            # Function collocation manipulate user preference
            KEYWORD, USER_PREFERENCE = self.collocation(KEYWORD, USER_PREFERENCE)
            print("collocation: ", KEYWORD, USER_PREFERENCE)
            # Generate plural form of key word and append into KEYWORD itself
            plural = self.inflect_engine.plural(KEYWORD)
            KEYWORD += " " + plural
            print("Add plural:", KEYWORD)

            # map category with USER_PREFERENCE
            USER_PREFERENCE_TEXT = self.map_user_preference(USER_PREFERENCE)
            # print(USER_PREFERENCE_TEXT)

            KEYWORD = KEYWORD.lower()
            #print(KEYWORD)

            # Standard listing without RL
            # Create list and append user input   
            self.data_list = self.dynamic_data['ProductTitle'].values.tolist()
            # self.data_list  = self.dynamic_data['ProductTitle'].astype(str) + ' ' + self.dynamic_data['Claims'].astype(str)

            # self.data_list = self.data_list.values.tolist()
            # print(self.data_list)

            # Priority 4 : (GET all product associated with keyword using title)
            self.dynamic_data['distances'] = self.find_tfidf_and_cosine(self.data_list, KEYWORD)

            # filter distance using THRESHOLD
            self.recommendation_list = self.dynamic_data[self.dynamic_data['distances'] >= THRESHOLD/100].sort_values(by=['distances'], ascending=False)
            
            self.legnth_recommendation_list = len(self.recommendation_list.index)
            
            # for empty recommendation_list :: Helps to dispaly 'no product available with this key word' in site  
            if self.legnth_recommendation_list == 0:
                self.empty_flag = True
            else: 
                self.empty_flag = False
            
            try:
                # Data order manipulation with three priority
                self.recommendation_list = self.get_relevance_sorted_product_with_user_priority(self.recommendation_list, KEYWORD +" "+ USER_PREFERENCE_TEXT)
            except Exception as e:
                print('Error in Data order manipulation',e)
                pass


            # print("RE preset", preset)
            # print(self.recommendation_list.head())
            self.recommendation_list['colFromIndex'] = self.recommendation_list.index
            self.recommendation_list = self.recommendation_list.sort_values([preset,'colFromIndex'], ascending = (False, True))
            print(self.recommendation_list.head())

            # RL based TOP priority

            # self.dynamic_data_title = self.dynamic_data['ProductTitle'].values.tolist()

            # self.dynamic_data['distances'] = self.find_tfidf_and_cosine(self.dynamic_data_title, KEYWORD)

            # filter distance using THRESHOLD and sort by RL weight
            # self.recommendation_list = self.recommendation_list[self.recommendation_list['RL_weights'] >= 0].sort_values(by=['RL_weights'], ascending=False)

            # self.recommendation_list = self.recommendation_list.sort_values(by=['RL_weights'], ascending=False)
            # # df.sort_values(['Player', 'Year', 'Tm_Rank'], ascending = [True, True, True], inplace = True)
            # self.length_dynamic_list_1 = len(self.recommendation_list.index)

            # get top weight products dataframe
            # print(self.length_dynamic_list_1)
            # print(self.recommendation_list)


            # remove features and distances column from output data
            del self.recommendation_list['features']
            del self.recommendation_list['distances']
            del self.recommendation_list['colFromIndex']

            # merge RL data and standard data without RL
            # self.recommendation_list = self.dynamic_list_1.append(self.recommendation_list, ignore_index = True)

            #print(self.recommendation_list)
            #################################################################################
            # Uncomment this part if you required to recommended product only in fix number 
            # n = 5
            # n_largest = self.dynamic_data['distances'].nlargest(n + 1)
            # print(n_largest)
            # print(self.dynamic_data['ProductTitle'].iloc[self.dynamic_data['distances'].nlargest(n + 1)])
            #################################################################################
        
        except Exception as e:
            # incase of error in script no recommendation
            self.recommendation_list = None
            self.legnth_recommendation_list = 0
            self.empty_flag = True
            print(e)

        return self.recommendation_list, self.legnth_recommendation_list, self.empty_flag


# Main method contains input method. This can be change according to requirement from below
if __name__ == "__main__":
    try:
        # Create folder data
        import os 
        if not os.path.exists('Recommendation_results'):
            os.makedirs('Recommendation_results')
        
        while True :
            print("\nEnter your keyword: ")
            keyword = str(input())

            # create Object of Recommendation_Engine     
            t1 = time.time()
            recommendation_engine = Recommendation_Engine([])
            t2 = time.time()
            print("Time 1: ", t2-t1)
            # GET recommendation list form recommendations_from_keyword method 
            
            # Value of THRESHOLD is optional and vary between [0 to 100] 

            # which is a matching threshold use for remove totaly irrelevant products(e.g high THRESHOLD -> low output)

            # user_preference key word / text line add here which works as defualt search for empty input 

            # empty_flag is for empty recommendation_list :: Helps to dispaly 'no product available with this key word' in site
            t3 = time.time()

            recommendations, len_of_list, empty_flag = recommendation_engine.recommendations_from_keyword(keyword, THRESHOLD= 2, USER_PREFERENCE= ['Nuts Chocolate'], preset = "RL_weights")
            t4 = time.time()
            print("Time 2: ", t4-t3)

            # check full data frame
            #print(recommendations)

            # recommendations.to_csv( 'Recommendation_results/'+ keyword+'_results.csv') 
            # see only title
            print(recommendations['ProductTitle'])
            print('Length of recommendation list : ', len_of_list)
            print('Flag represent recommendation list is empty(True) or not(False): ', empty_flag)
    
    except Exception as e:
        print(e)