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
        print("extrayendo topics...")
        self.df = self.df.progress_apply(
            lambda row: self.func_classify_topic(row), axis=1
        )

    def get_sector_in_topics(self):
        print("extrayendo sectores en topics...")
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
        print("calculando intensidad...")
        self.df["intensidad"] = self.df.progress_apply(
            lambda row: self.sum_columns(row), axis=1
        )

    def output(
        self,
        columns,
        ruta="data/outputs",
        name="output_classifier_2021_01",
        index=False,
    ):
        self.df = self.df.reindex(columns=columns)
        self.df.sort_values(["date","pk"], inplace=True)
        # limpiamos noticias no asignadas a ticker. mas velocidad, perdemos noticias generalistas
        # self.df.to_csv(f'{ruta}/{name}_full.csv', sep=";")
        df_clean = self.df.dropna()
        df_clean.to_csv(f"{ruta}/{name}.csv", sep=";", index=index)


if __name__ == "__main__":

    # df = pd.read_csv('gs://tfm_aideas_datasets/output202101.csv', sep=";", index_col=0, parse_dates=["date"])
    df = pd.read_csv(
        "data/outputs/20210101/output_ner_20210101.csv",
        sep=";",
        index_col=0,
        parse_dates=["date"],
    )

    classifier = Classifier(df)

    classifier.get_topics()

    classifier.get_sector_in_topics()

    classifier.get_intensidad()

    columns = df.columns.tolist()
    columns = columns.append(
        [
            "topic_classifier",
            "topics",
            "finanzas_prob",
            "sector_in_topics",
            "intensidad",
        ]
    )
    classifier.output(columns, "data/outputs/20210101", "output_classifier_20210101", True)
