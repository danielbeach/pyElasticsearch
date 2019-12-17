from elasticsearch import Elasticsearch
from random import randint


class Book:
    def __init__(self, book_id: int, title: str, author: object, sub_title = None):
        self.book_id = book_id
        self.title = title,
        self.sub_title = sub_title
        self.author = author
        self.raw_text = None
        self.sentence_delimiter = '.'
        self.paragraph_delimiter = '\n\n\n'
        self.paragraphs = None
        self.indexed_paragraphs = []

    def load_raw_text(self):
        with open('downloads/{book}-mod.txt'.format(book=self.book_id)) as f:
            self.raw_text = f.read()

    def split_text_into_paragraphs(self):
        self.paragraphs = self.raw_text.split(self.paragraph_delimiter)
        self.raw_text = None

    def index_paragraphs(self):
        p_counter = 1
        for paragraph in self.paragraphs:
            self.indexed_paragraphs.append({"index": p_counter, "paragraph": paragraph})
            p_counter += 1
        self.paragraphs = None

    def split_paragraphs_into_sentences(self):
        s_counter = 1
        for paragraph in self.indexed_paragraphs:
            sentences = paragraph["paragraph"].split(self.sentence_delimiter)
            for sentence in sentences:
                s_counter += 1
                elastic_book_packet = self.create_data_packet(paragraph, s_counter, sentence)
            yield elastic_book_packet
        self.indexed_paragraphs = None

    def create_data_packet(self, paragraph, s_counter, sentence):
        return {"book_id": self.book_id,
                "author_id": self.author.author_id,
                "category": self.author.category,
                "chapter_id": 0,
                "paragraph": paragraph["index"],
                "sentence_id": s_counter,
                "sentence_text": sentence.replace('\n', '')}


class Author:
    def __init__(self, first_name: str, last_name: str, category: str, middle_name = None):
        self.first_name = first_name
        self.last_name = last_name
        self.middle_name = middle_name
        self.category = category
        self.author_id = randint(1,10000)


class ElasticSink:
    def __init__(self):
        try:
            self.client = Elasticsearch()
        except Exception as e:
            print('Sorry, problem trying to create Elasticsearch client.')
            exit(1)

    def index_document(self, data_packet: dict, index='books', doc_type='sentence'):
        unique_index_id = '{book_id}_{sentence_id}'.format(book_id=data_packet["book_id"],
                                                           sentence_id=data_packet["sentence_id"])
        try:
            response = self.client.index(index=index,
                                     doc_type=doc_type,
                                     body=data_packet,
                                     id=unique_index_id)
            print(response)
        except Exception as e:
            print(f'Something went wrong and I could not index.. {data_packet}')

    def search_for_word_match(self, word: str, index: str, field: str):
        result = self.client.search(index=index,body={'query':{'match':{field:word}}})
        for hit in result["hits"]:
            print(hit)

    def search_and_filter(self, index: str, field: str, word: str, author_id: str):
        result = self.client.search(index=index,
                                    body={
                                      "query": {
                                             "bool" : {
                                                  "must" : [{"term" : {field : word}},],
                                                  "filter": [{"term" : {"author_id" : author_id}}]
                                                  }
                                      }
                                    }

                                )
        for hit in result["hits"]:
            print(hit)


if __name__ == '__main__':
    a = Author(first_name='St.',
               last_name='Augustine',
               category='Early Church Father')
    b = Book(book_id=3296,
             title='The Confessions Of Saint Augustine',
             author=a)
    b.load_raw_text()
    b.split_text_into_paragraphs()
    b.index_paragraphs()
    packets = b.split_paragraphs_into_sentences()
    es = ElasticSink()
    for packet in packets:
        es.index_document(packet)
    es.search_for_word_match(word='faith',
                             index='books',
                             field='sentence_text')
    es.search_and_filter(word='faith',
                             index='books',
                             field='sentence_text',
                             author_id=1168)
