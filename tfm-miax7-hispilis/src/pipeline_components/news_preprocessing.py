import re

import nltk
import spacy
from nltk.corpus import stopwords
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
        df_news_paragraphs.body = df_news_paragraphs.body.str.split("<EOL>")
        df_news_paragraphs = df_news_paragraphs.explode("body")
        sorted_cols = ["pk_paragraph", *df_news_paragraphs.columns.to_list()]
        df_news_paragraphs["pk_paragraph"] = (
            df_news_paragraphs.index
            + "_"
            + df_news_paragraphs.reset_index().index.astype(str).str.zfill(10)
        )
        df_news_paragraphs = df_news_paragraphs.reindex(columns=sorted_cols)
        return df_news_paragraphs

    def remove_entities(self, row):
        title = row["title"]
        body = row["body"]
        for entity in row["entities"]:
            title = title.replace(entity, "").strip()
            body = body.replace(entity, "").strip()
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
