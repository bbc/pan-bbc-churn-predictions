import numpy as np
import pandas as pd
import pytest
import datatest as dt


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def training_df_light():
    return pd.read_csv('iplayer_training_set_light.csv')


@pytest.fixture(scope='module')
@dt.working_directory(__file__)
def score_df_light():
    return pd.read_csv('iplayer_churn_score_sample_light.csv')


@pytest.fixture()
def df():
    return pd.DataFrame([{'name': 'Bob', 'age': 18, 'home': 'London', 'binary': 1, 'categories_oh': 'Red'},
                         {'name': 'John', 'age': 12, 'home': 'Oxford', 'binary': 1, 'categories_oh': 'Blue'},
                         {'name': 'Jake', 'age': 38, 'home': 'New York', 'binary': 0, 'categories_oh': 'Green'},
                         {'name': 'Alice', 'age': np.nan, 'home': 'Paris', 'binary': 0, 'categories_oh': 'Red'},
                         {'name': 'Charlie', 'age': 16, 'home': 'Amsterdam', 'binary': 1, 'categories_oh': 'Blue'},
                         {'name': 'Tony', 'age': 33, 'home': 'Edinburgh', 'binary': 0, 'categories_oh': 'Green'},
                         {'name': 'Kevin', 'age': 28, 'home': 'Cambridge', 'binary': 0, 'categories_oh': 'Red'},
                         {'name': 'Rick', 'age': 8, 'home': 'San Fransisco', 'binary': 1, 'categories_oh': 'Blue'}])


@pytest.fixture()
def df_with_mvi():
    return pd.DataFrame([{'age': 18.0},
                         {'age': 12.0},
                         {'age': 38.0},
                         {'age': 33.0},
                         {'age': 16.0},
                         {'age': 33.0},
                         {'age': 28.0},
                         {'age': 8.0}])

@pytest.fixture()
def mvi_impute_strategies():
    impute_strategies = pd.DataFrame({
        'colname': ['age'],
        'strategy': ['regression']
    })
    return impute_strategies


@pytest.fixture()
def helper_features():
    helper_features = ['binary']
    return helper_features


@pytest.fixture()
def df_with_oh():
    df = pd.DataFrame([{'categories_oh_Blue': 0, 'categories_oh_Green': 0, 'categories_oh_Red': 1},
                         {'categories_oh_Blue': 1, 'categories_oh_Green': 0, 'categories_oh_Red': 0},
                         {'categories_oh_Blue': 0, 'categories_oh_Green': 1, 'categories_oh_Red': 0},
                         {'categories_oh_Blue': 0, 'categories_oh_Green': 0, 'categories_oh_Red': 1},
                         {'categories_oh_Blue': 1, 'categories_oh_Green': 0, 'categories_oh_Red': 0},
                         {'categories_oh_Blue': 0, 'categories_oh_Green': 1, 'categories_oh_Red': 0},
                         {'categories_oh_Blue': 0, 'categories_oh_Green': 0, 'categories_oh_Red': 1},
                         {'categories_oh_Blue': 1, 'categories_oh_Green': 0, 'categories_oh_Red': 0}])
    df = df.astype(np.uint8)
    return df


@pytest.fixture()
def one_hot_candidates():
    one_hot_candidates = ['categories_oh']
    return one_hot_candidates


@pytest.fixture()
def tprfpr_inputs():
    y_pred = np.array([1,0.1,0.5,0.4,0.6,0.2,0.5,0.3,0.4,0.9,0.9,0.95,1,0.8, 0.1])
    y_true = np.array([0.9,0.1,0.4,0.4,0.6,0.2,0.9,0.3,0.4,0.8,0.85,0.95,1,1,1])
    threshold = 0.6

    return {"y_pred": y_pred,
            "y_true": y_true,
            "threshold": threshold}

@pytest.fixture()
def tprfpr_confusion_matrix():

    return np.array([(8,4), (1,2)])

@pytest.fixture()
def tprfpr_confusion_matrix_zeros():

    return np.array([(6,2), (0,0)])


@pytest.fixture()
def stacking_scaler():
    return pd.DataFrame(np.array([[0.47368421, 1.],
                                  [0.31578947, 1.],
                                  [1., 0.],
                                  [np.nan, 0.],
                                  [0.42105263, 1.],
                                  [0.86842105, 0.],
                                  [0.73684211, 0.],
                                  [0.21052632, 1.]]),
    index = pd.RangeIndex(start=0, stop=8, step=1),
            columns = pd.Index(['age', 'binary'], dtype='object'))


@pytest.fixture()
def training_df_light_n_cols():
return {"n_cols": 190}