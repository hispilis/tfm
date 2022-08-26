import math

import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow import keras
from tqdm.auto import tqdm


def smape_loss(y_true, y_pred):
    return 100 * (tf.abs(y_true - y_pred) / ((y_true + tf.abs(y_pred)) / 2))


class ModelBasic:
    def __init__(self, df_input):
        self.df_input = df_input

    def modelling(self):
        print(f"modelling...")
        raw_features = self.df_input.loc[
            :,
            [
                "ticker",
                "date",
                "rd_activo_news",
                # Las comento de momento ya que tendremos que replantear el modelo
                # y por otro lado, tendremos que evitar lo máximo posible el sesgo
                # de look-ahead
                # "r_d_menos_1",
                # "r_d_mas_1",
                # "beta",
                # "r_d_bench",
                # "r_adj_menos_1",
                # "r_adj_mas_1",
            ],
        ]
        raw_features["date"] = pd.to_datetime(raw_features.date, format="%Y-%m-%d")
        tickers = raw_features.ticker.unique()

        # One-hot encoding
        for ticker in tickers[1:]:
            raw_features[ticker] = raw_features.ticker == ticker

        # Seasonal variations (Fourier series)
        dayofyear = raw_features.date.dt.dayofyear
        for k in range(1, len(tickers)):
            raw_features[f"sin{k}"] = np.sin(dayofyear / 365 * 2 * math.pi * k)
            raw_features[f"cos{k}"] = np.cos(dayofyear / 365 * 2 * math.pi * k)
            raw_features[f"{tickers[k]}_sin{k}"] = (
                raw_features[f"sin{k}"] * raw_features[tickers[k]]
            )
            raw_features[f"{tickers[k]}_cos{k}"] = (
                raw_features[f"cos{k}"] * raw_features[tickers[k]]
            )

        raw_features = raw_features.drop(columns="ticker")
        raw_features = raw_features.drop(columns="date")
        # raw_features = pd.get_dummies(raw_features)

        features = raw_features.values
        # target = df_input.alpha_expost_label
        target = df_input.alpha_expost

        print(features, target)

        x_train, x_test, y_train, y_test = train_test_split(
            features, target, test_size=0.33
        )
        x_train = np.asarray(x_train).astype("float32")
        x_test = np.asarray(x_test).astype("float32")
        y_train = np.asarray(y_train).astype("float32")
        y_test = np.asarray(y_test).astype("float32")

        # revisar los inf
        x_train[np.isinf(x_train)] = 0
        x_test[np.isinf(x_test)] = 0
        y_train[np.isinf(y_train)] = 0
        y_test[np.isinf(y_test)] = 0

        scaler = StandardScaler()
        x_train = scaler.fit_transform(x_train)
        x_test = scaler.transform(x_test)

        # y_train = y_train.reshape(-1, 1)
        # y_test = y_test.reshape(-1, 1)

        # model = keras.Sequential()
        # model.add(keras.layers.Input(shape=(x_train.shape[1],), name="entrada"))
        # model.add(keras.layers.Dense(1, name="salida"))
        # model.compile(optimizer=keras.optimizers.SGD(learning_rate=0.01),
        #           loss='mean_squared_error',
        #           metrics=['acc'])

        model = keras.Sequential()
        model.add(keras.layers.Input(shape=(x_train.shape[1],), name="entrada"))
        model.add(
            keras.layers.Dense(units=64, kernel_regularizer=keras.regularizers.l2(0.01))
        )
        model.add(tf.keras.layers.BatchNormalization())
        model.add(tf.keras.layers.Activation("relu"))
        model.add(tf.keras.layers.Dropout(0.2))
        model.add(
            keras.layers.Dense(units=32, kernel_regularizer=keras.regularizers.l2(0.01))
        )
        model.add(tf.keras.layers.BatchNormalization())
        model.add(tf.keras.layers.Activation("relu"))
        model.add(tf.keras.layers.Dropout(0.2))
        model.add(keras.layers.Dense(1, name="salida"))

        model.compile(
            optimizer=keras.optimizers.Adam(learning_rate=0.01),
            # loss=smape_loss,
            loss="mean_squared_error",
            metrics=["acc"],
        )

        history = model.fit(
            x_train,
            y_train,
            epochs=200,
            batch_size=x_train.shape[0],
            validation_split=0.2,
        )

        y_pred = model.predict(x_test)
        # y_pred = np.exp(model.predict(x_test))

        for i in range(10):
            print(f"y_test[{i}] : {y_test[i]} , y_pred[{i}] : {y_pred[i]}")

        smape = np.mean(smape_loss(y_test, y_pred))
        print(f"SMAPE: {smape:.5f}")


if __name__ == "__main__":
    df_input = pd.read_csv(
        "data/outputs/dataset.csv", sep=";", index_col=0, parse_dates=["date"]
    )
    print(df_input)
    model = ModelBasic(df_input)
    model.modelling()
