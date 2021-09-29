# run : python generate_product_tag.py

# This script is helpfull to create a tags field in dynamic database
from recommendation_engine import Recommendation_Engine
import pandas as pd

# read mapping file (csv)
mapping = pd.read_csv('mapping.csv', names=['category 1', 'category 2', 'buzzwords'], header=None, skiprows=2)

# function to perform_matching_and_getscore
def perform_matching_and_getscore(product_str, buzzwords_str, tag):
    score = 0.0

    # merge category2 and buzz word
    buzzwords_str += ", " + tag

    # print(buzzwords_str)

    # create array from comma seprated buzzword
    buzzwords_arr = buzzwords_str.split(", ")
    # print(buzzwords_arr)

    # match each buzzword and update score
    for each_buzzword in buzzwords_arr:

        # check buzzword size skip if it's only one char
        if len(each_buzzword.strip()) > 1:
            # our own score calculation logic
            if each_buzzword.lower() in product_str:
                score += (1/len(buzzwords_arr))

    return score

# get predicted_tags 
def get_tags_for_product(product_str):
    """
    input : product_str
    output :predicted tags list
    """
    predicted_tags = []
    # print(product_str)

    # loop over mapping file
    for index, each_buzz_word_line in mapping.iterrows():    

        # print(each_buzz_word_line["category 2"], each_buzz_word_line["buzzwords"])

        # check if buzz word is null or too small <= 2
        if str(each_buzz_word_line["buzzwords"])!="nan" and len(str(each_buzz_word_line["buzzwords"])) >2:
            # print(each_buzz_word_line["category 2"], each_buzz_word_line["buzzwords"])

            #  get score per each line
            score = perform_matching_and_getscore(product_str, each_buzz_word_line["buzzwords"], each_buzz_word_line["category 2"])
            
            # print(score)

            # check score value and add category 2 into tags field
            if score > 0.0:
                predicted_tags.append(each_buzz_word_line["category 2"])
        
    # print(predicted_tags)

    # return string for database
    return str(predicted_tags)

# re write dynamic database
def re_write_dynamic_database(dataframe):
    import sqlite3
    try:
        conn = sqlite3.connect('dynamic_database.db')
        
        # write dynamic_database once again update with all value
        dataframe.to_sql('product_data', conn, if_exists='replace', index=False)

    except Exception as identifier:
        print(identifier)

 
if __name__ == "__main__":

    # read database
    re1 = Recommendation_Engine()
    print(re1.dynamic_data.columns)
    # print(re1.dynamic_data['features'])

    # object data to dataframe
    product_list = re1.dynamic_data

    print(product_list.columns)

    # Generate new column with predicted_tags
    product_list['predicted_tags'] = product_list['features'].apply(lambda x : get_tags_for_product(x.lower()))

    # remove features
    del product_list['features']

    print(product_list.columns)
    # print(type(product_list["tag"][0]))

    # rewrite database
    re_write_dynamic_database(product_list)


    # only for manual testing

    # text1 = """mccain superfries fries crinkle cut none potato (95%), canola oil , dextrose  (maize) 3.9 900g none no artificial colours, flavours or preservatives,  low in cholesterol , no artificial colours, no artificial flavours , source of fibre , new zealand grown potatoes, low in cholesterol – we should all lower our intake of cholesterol and select unsaturated fats and oils in preference to saturated fats and oils. this product has been cooked in monounsaturated vegetable oil which means that when these chips are cooked in your oven, according to the cooking instructions, the cholesterol level will be negligible compared to chips cooked in animal fat., our recipe is simple: good food starts from real ingredients. none made in new zealand NO meat water quality Solar Power Produced"""
    
    # texts = []
    # texts.append(text1)


    # for each_product in texts:
    #     tags_1 = get_tags_for_product(each_product.lower())
    # print(tags_1)
    

