from pandas.testing import assert_frame_equal
from src.learn import model_stacking

def test_scale_my_data(df, stacking_scaler):

    expected = stacking_scaler

    result = model_stacking.scale_my_data(df[['age', 'binary']])

    assert_frame_equal(expected, result)
