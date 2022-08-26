import pickle as pickle
import re
import string

# import gensim
import matplotlib.pyplot as plt
import nltk
import numpy as np
import pandas as pd
import spacy
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from tqdm.auto import tqdm


class Preproccesor:
    def __init__(self, df_news, col_text="body", bigram=False, vocab="es_core_news_sm"):
        self.df_news = df_news

        self.black_list = [
            "más",
            "mas",
            "os",
            "dame",
            "ene",
            "feb",
            "mar",
            "abr",
            "may",
            "jun",
            "jul",
            "ago",
            "sep",
            "sept",
            "oct",
            "nov",
            "dic" "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        ]

        nltk.download("stopwords")
        nltk.download("punkt")
        stop = set(stopwords.words("spanish"))
        additional_stopwords = set(self.black_list)
        self.stopwords = stop.union(additional_stopwords)

        # self.nlp = spacy.load("es_core_news_md")
        self.nlp = spacy.load(vocab)

        # if bigram:
        #     self.bigram = gensim.models.Phrases(
        #         self.df_news[col_text].dropna().to_list()
        #     )

    def clean_special_character(self, col_from="body", col_to="body"):
        tqdm.pandas()
        self.df_news[col_to] = self.df_news[col_from].progress_apply(
            lambda x: self.cleaner_special_character(x)
        )
        self.df_news[col_to] = self.df_news[col_to].str.replace("\ufeff", "")
        return self.df_news

    def clean(self, col_from="body", col_to="texto_limpio"):
        tqdm.pandas()
        self.df_news[col_to] = self.df_news[col_from].progress_apply(
            lambda x: self.cleaner(x)
        )
        return self.df_news

    def lemmatize(self, col_from="texto_limpio", col_to="texto_lematizado"):
        tqdm.pandas()
        self.df_news[col_to] = self.df_news[col_from].progress_apply(
            lambda x: self.lemmatizer(x)
        )
        return self.df_news

    def paragraphe(self, col_title="title"):
        tqdm.pandas()
        df_news_paragraphs = self.df_news.drop_duplicates(subset=[col_title, "ticker"])
        df_news_paragraphs = df_news_paragraphs.progress_apply(
            lambda row: self.paragrahps(row), axis=1
        )
        df_news_paragraphs = df_news_paragraphs.explode("body")
        return df_news_paragraphs

    def paragrahps(self, row):
        if not type(row.body) == str:
            row.body = str(row.body)
        row["body"] = row.body.split("<EOL>")
        return row

    def remove_entities(self, row):
        title = row["title"]
        body = row["body"]
        for entity in row["entities"]:
            title = title.replace(entity, "")
            body = body.replace(entity, "")
        row["title_no_entities"] = title
        row["body_no_entities"] = body
        return row

    def hide_entities(self, df):
        tqdm.pandas()
        df = df.progress_apply(self.remove_entities, axis=1)
        return df

    def cleaner_special_character(self, word):
        if type(word) == str:
            regex_pattern = re.compile(
                pattern="["
                u"\U0001F600-\U0001F64F"
                u"\U0001F300-\U0001F5FF"
                u"\U0001F680-\U0001F6FF"
                u"\U0001F1E0-\U0001F1FF"
                u"\U0000FEFF-\U0001FEFF"
                "]+",
                flags=re.UNICODE,
            )
            return regex_pattern.sub(r"", word)

    def cleaner(self, word):
        if type(word) == str:
            word = re.sub(
                r"((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*",
                "",
                word,
                flags=re.MULTILINE,
            )
            word = re.sub(r"(?::|;|=)(?:-)?(?:\)|\(|D|P)", "", word)
            word = re.sub(r"europa press", "", word)
            word = re.sub(r"EUROPA PRESS", "", word)
            word = re.sub(r"- Redacción -", "", word)
            word = re.sub(r"\#\.", "", word)
            word = re.sub(r"\r", "", word)
            word = re.sub(r"\n", "<EOL>", word)
            word = re.sub(r",", "", word)
            word = re.sub(r"\-", " ", word)
            word = re.sub(r"\.{3}", " ", word)
            word = re.sub(r"a{2,}", "a", word)
            word = re.sub(r"é{2,}", "é", word)
            word = re.sub(r"i{2,}", "i", word)
            word = re.sub(r"ja{2,}", "ja", word)
            word = re.sub(r"á", "a", word)
            word = re.sub(r"é", "e", word)
            word = re.sub(r"í", "i", word)
            word = re.sub(r"ó", "o", word)
            word = re.sub(r"ú", "u", word)
            word = re.sub("[^a-zA-Z]", " ", word)

            list_word_clean = []
            for w1 in word.split(" "):
                if w1.strip() == "EOL":
                    list_word_clean.append("<EOL>")
                elif w1.strip() == "<EOL>":
                    list_word_clean.append(w1)
                elif w1.lower() not in self.stopwords and w1.isalnum():
                    list_word_clean.append(w1.lower())
            out_text = " ".join(list_word_clean)
            return out_text

    def lemmatizer(self, word):
        if type(word) == str:
            list_word_clean = []
            for w1 in word.split(" "):
                list_word_clean.append(w1.lower())
            bigram_list = self.bigram[list_word_clean]
            out_text = self.lemmatization(" ".join(bigram_list))
            return out_text

    def lemmatization(self, texts, allowed_postags=["NOUN"]):
        texts_out = [
            token.text
            for token in self.nlp(texts)
            if token.pos_ in allowed_postags
            and token.text not in self.black_list
            and len(token.text) > 2
        ]
        return texts_out


if __name__ == "__main__":
    with open("data/outputs/output_202112.pkl", "rb") as f:
        df_news = pickle.load(f)
    df_news = pd.DataFrame(df_news)

    preproccesor = Preproccesor(df_news, bigram=True)

    df_news = preproccesor.clean()
    # df_news = preproccesor.lemmatize()

    df_news.to_pickle("data/outputs/output_202112_clean.pkl")
    df_news.to_csv("data/outputs/output_202112_clean.csv", sep=";")
    print(preproccesor.df_news)
