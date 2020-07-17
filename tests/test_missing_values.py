from pandas.testing import assert_frame_equal
from conftest import StoreInModule
from src.prep import missing_values

temporary_storage = StoreInModule

def test_assert_value_stored():
    temporary_storage.new_value = 42
    assert temporary_storage.initial == "value"


def test_mvi_train(df, df_with_mvi, mvi_impute_strategies, helper_features):
    mvi = missing_values.missing_value_imputer(
        impute_strategies=mvi_impute_strategies,
        helper_features=helper_features)

    expected = df_with_mvi

    result = mvi.train(df)

    temporary_storage.new_value = mvi

    assert_frame_equal(expected, result)


def test_mvi_score(df, df_with_mvi):
    mvi = temporary_storage.new_value

    expected = df_with_mvi

    result = mvi.score(df)

    assert_frame_equal(expected, result)
