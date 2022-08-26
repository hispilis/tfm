import pandas as pd
from subject_classification_spanish import subject_classifier
from tqdm.auto import tqdm


class Classifier:
    def __init__(self, df):
        self.df = df

        self.subject_classifier = subject_classifier.SubjectClassifier()

        self.FINANZAS_LIST = [
            "economía",
            "finanzas",
            "bolsa",
            "mercados financieros",
            "bolsas internacionales",
        ]

        tqdm.pandas()

    def func_classify_topic(self, row):
        if type(row["body"]) == str:
            row["topics"] = self.subject_classifier.classify(row["body"])
        else:
            row["topics"] = {}

        v_ret = 0.0
        row["topic_classifier"] = ""
        row["finanzas_prob"] = 0
        for topic, v in row["topics"].items():
            if v > v_ret:
                v_ret = v
                row["topic_classifier"] = topic
            if topic in self.FINANZAS_LIST:
                row["finanzas_prob"] = v
                break
        return row

    def sector_in_topics_apply(self, row):
        if not pd.isna(row.sector):
            for k, v in row.topics.items():
                if k in row.sector.lower() or k in row.subsector.lower():
                    return 1
        return 0

    def get_topics(self):
        print("extrayendo topics...\n")
        self.df = self.df.progress_apply(
            lambda row: self.func_classify_topic(row), axis=1
        )

    def get_sector_in_topics(self):
        print("extrayendo sectores en topics...\n")
        self.df["sector_in_topics"] = self.df.progress_apply(
            lambda row: self.sector_in_topics_apply(row), axis=1
        )

    def sum_columns(self, row):
        if (
            not pd.isna(row["ticker_freq"])
            and not pd.isna(row["ticker_first"])
            and not pd.isna(row["ticker_title"])
            and not pd.isna(row["finanzas_prob"])
            and not pd.isna(row["sector_in_topics"])
        ):
            return (
                int(row["ticker_freq"])
                + int(row["ticker_first"])
                + int(row["ticker_title"])
                + float(row["finanzas_prob"])
                + int(row["sector_in_topics"])
            )
        return 0

    def get_intensidad(self):
        print("calculando intensidad...\n")
        self.df["intensidad"] = self.df.progress_apply(
            lambda row: self.sum_columns(row), axis=1
        )

    def get_news_properties(self, df_news, row):
        row["topic_classifier_news"] = df_news.loc[row.name].topic_classifier
        row["topics_news"] = df_news.loc[row.name].topics
        row["finanzas_prob_news"] = df_news.loc[row.name].finanzas_prob
        row["sector_in_topics_news"] = df_news.loc[row.name].sector_in_topics
        row["intensidad_news"] = df_news.loc[row.name].intensidad
        return row

    def output(
        self,
        columns,
        ruta="data/outputs",
        name="output_classifier_2021_01",
        index=False,
    ):
        self.df = self.df.reindex(columns=columns)
        # limpiamos noticias no asignadas a ticker. mas velocidad, perdemos noticias generalistas
        # self.df.to_csv(f'{ruta}/{name}_full.csv', sep=";")
        # Las nuevas columnas añadidas al dataset no pueden ser NaN, luego no añadimos dropna
        self.df.to_csv(f"{ruta}/{name}.csv", sep=";", index=index)
