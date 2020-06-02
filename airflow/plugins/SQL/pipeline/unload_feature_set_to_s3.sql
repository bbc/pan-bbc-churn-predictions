UNLOAD($$
    SELECT * from central_insights_sandbox.ap_churn_sounds_score_sample
    $$)
TO 's3://int-insights-pan-bbc-churn-predictions/data/input/churn_training_set.csv'
CREDENTIALS 'aws_access_key_id=ASIAZJVHSKVAIF5J6TO2;aws_secret_access_key=Vub8ro1q6cclE9a3+1Jf/48GBxkpViWr/MbGUvZS;token=FwoGZXIvYXdzEJ///////////wEaDFmBwLK+Vg/BUCt2DSK8ATwgz0TZTQVVm8h67bpV8mdvH+Qsa1eweTiphHHjh4cyjGc9d5Uu8Zedl9HvbnDbhwoLsZ2EqSL7VlpEleDyZzz9WiW/e/Gv4arCl42rW7Wyf6BzCzZDqwPF55G9evujFTntn9tBz5oFsoo/sbYRTjfF8/Y9nfpqfN77/fvtmjKlXyALWHdPGdOJpkbhHbUJkBoJ87AlCFQVP+ugohQ34oerO9uDxjqcPSvvF6cuia9fMTW6Z75FGjYb/WalKM6civYFMi3bbO7QlKEdEcfBf5bLxESz8y8ot6YZUpsHJoZPfPi1xOuaVHI1xOlCevqqRUg='
ALLOWOVERWRITE
PARALLEL OFF
BZIP2;