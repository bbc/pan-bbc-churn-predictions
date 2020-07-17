from pandas.testing import assert_frame_equal
from conftest import StoreInModule
from src.prep import one_hot

temporary_storage = StoreInModule

def test_one_hot_train(df, df_with_oh, one_hot_candidates):
    ohe = one_hot.one_hot_encoder(one_hot_candidates)

    expected = df_with_oh

    result = ohe.train(df)

    temporary_storage.new_value = ohe

    assert_frame_equal(expected, result)


def test_one_hot_score(df, df_with_oh):
    one_hot = temporary_storage.new_value

    expected = df_with_oh

    result = one_hot.score(df)

    assert_frame_equal(expected, result)
