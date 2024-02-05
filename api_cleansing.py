import regex as re
import pandas as pd
from flask import Flask, jsonify, request
from flask_restx import Api, Resource, reqparse
from flasgger import Swagger
from werkzeug.datastructures import FileStorage
import sqlite3
import os

app = Flask(__name__)
api = Api(app, version='1.0', title='API Documentation for Data Processing', description='Dokumentasi API untuk Data Processing')
swagger = Swagger(app)

ns = api.namespace('api', description='API Endpoints')

parser = reqparse.RequestParser()
parser.add_argument('text', type=str, help='Text to be processed')

file_parser = reqparse.RequestParser()
file_parser.add_argument('file', type=FileStorage, location='files', required=True,
                         help='Uploaded CSV file')


def clean_text(text):
    
    # Read abusive words from CSV file
    abusive_words = set(pd.read_csv('abusive.csv', encoding='latin-1', header=None)[0].tolist())

    # Cleaning Formatted Text using Regex
    #will replace the html characters with " "
    text = re.sub(r'http\S+', '', text)
    text = re.sub('(@\w+|#\w+)','',text)
    #To remove the punctuations
    text=re.sub('<.*?>', '', text)  
    #will consider only alphabets
    text = re.sub('[^a-zA-Z]',' ',text)    
    #will replace newline with space
    text = re.sub("\n"," ",text)
    #will convert to lower case
    text = text.lower()
    # will replace a word
    text = re.sub("(username|user|url|rt|xf|fx|xe|xa|xd)\s|\s(user|url|rt|xf|fx|xe|xa)","",text)
    # will repalce repated char
    text = re.sub(r'(\w)(\1{2,})', r"\1", text)
    # will replace single word
    text = re.sub(r"\b[a-zA-Z]\b","",text)
    # will replace space more than one
    text = re.sub('(s{2,})',' ',text)
    # will replace space more than one
    text = re.sub('(s{2,})',' ',text)
    

    
    # Remove abusive words
    for word in abusive_words:
        text = re.sub(r'\b' + word + r'\b\s*', '', text)

    # Join the words
    words = text.split()
    text = ' '.join(words)

    return text


# Fungsi untuk mengganti kata-kata
kamus_df = pd.read_csv('new_kamusalay.csv', encoding = 'latin-1')
def replace_words(cleaned_tweet):
    words = cleaned_tweet.split() # Mengubah kalimat menjadi list kata
    updated_words = []
    replacement_dict = dict(zip(kamus_df['original'], kamus_df['replacement'])) # Mengubah dataframe menjadi dictionary
    for word in words:
        updated_word = replacement_dict.get(word, word) # Menggunakan dictionary.get() untuk mengganti kata-kata
        updated_words.append(updated_word)
    return ' '.join(updated_words)


@ns.route('/text-processing')
class TextProcessing(Resource):
    @api.doc(responses={200: 'Success', 400: 'Validation Error'})
    @api.expect(parser)
    def post(self):
        args = parser.parse_args()
        text = args['text']

        json_response = {
            'status_code': 200,
            'description': "Teks yang sudah diproses",
            'data': clean_text(text),
        }
        return jsonify(json_response)
    
@ns.route('/file-processing')
class FileProcessing(Resource):
    @api.doc(responses={200: 'Success', 400: 'Validation Error'})
    @api.expect(file_parser)
    def post(self):
        file = request.files['file']

        chunksize = 1000
        chunks = pd.read_csv(file, encoding='latin-1', chunksize=chunksize)
        all_data = pd.DataFrame()
        
        for chunk in chunks:
            chunk = pd.DataFrame(chunk)
            chunk = pd.DataFrame(chunk[['tweet']])
            chunk['cleaned_tweet'] = chunk['tweet'].apply(clean_text)
            chunk['cleaned_tweet'] = chunk['cleaned_tweet'].apply(replace_words)
            chunk['cleaned_tweet'] = chunk['cleaned_tweet'].apply(clean_text)  
            
            
            all_data = pd.concat([all_data, chunk], ignore_index=True) 
        # Save cleaned data to a new CSV file
        all_data.to_csv('cleaned_datalagi.csv', index=False)

        # Save cleaned data to a SQLite database
        conn = sqlite3.connect('cleaned_data.db')
        all_data.to_sql('cleaned_tweet', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        # Save cleaned data to a new CSV file
        all_data.to_csv('cleaned_data.csv', index=False)

        json_response = {
            'status_code': 200,
            'description': "Text from the CSV file has been processed successfully.",
        }

        return jsonify(json_response)


if __name__ == '__main__':
    app.run(debug=True)
