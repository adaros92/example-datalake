# Beyond Bets

## Before you begin

Install PySpark.

#### Mac
To install PySpark on most linux/mac machines just run

`pip install pyspark`

#### Windows
PySpark requires java. On Windows we recommend using OpenJDK which can be downloaded from [here](https://learn.microsoft.com/en-us/java/openjdk/download).

If you are using windows you will also likely need to set the `PYSPARK_PYTHON` environment variable. When using from within a virtual environment you can set it as `PYSPARK_PYTHON=python`. Otherwise, you will need to provide the path to the executable.

## Install the package
You will need to install the `beyond_bets` package to complete this project. There are multiple ways to do this, for an editable install navigate to the root directory of the package and run the following:

`pip install -e .`

To test the install run `hello_world.py` located in the `examples` directory. You should see a table with the numbers 1-10 in the console.

You can also run unit tests by running `pytest` at the top level. To run specific tests, you can provide a keyword: `pytest -k daily_transform`. 

## Problem
Beyond Bets is a new venture from BeyondTrust to track metrics for sports betting markets. The current codebase works, but the rapid increase in feature requests has left the code in significant need of refactoring. WITHOUT breaking any backwards compatability please add the following new features. It is highly recommended that you improve the codebase as you implement your features.

* The `PlayerDaily` and `MarketHourly` transforms have been very popular. Analysts now are requesting `PlayerHourly`, `MarketDaily`, and `PlayerMarketDaily` transforms.

* Management wants to start a new VIP program for our biggest bettors. Implement `TopPlayers`, which will return the top 1% of all players by total spend in the past week.

* Finance wants to know if the market conditions are changing drastically. Implement `BetGrader`, which will grade each bet relative to all bets within the same market in the last 15 minutes. For example, if a bet for $15 was placed in market A at 1:15pm, and the average bet size in market A between 1:00 and 1:15 was $10 then the grade for this bet would be $15/$10.

Remember the goal is NOT to perfect the codebase. The goal is to implement the requested features while making some improvements.

Please note, in a real data warehouse the data would sit in physical tables. In order to not write a bunch of junk to your hard drive the datasets are randomly generated.

Lastly, all logic needs to be contained within the beyond_bets package itself. Any code in the `examples` directory is just to demonstrate how the code might be used.
