# Backtesting Pipeline

This repository contains the code used for a lecture presented to the DevilQuant club at ASU on 2/24/26. You can find the presentation slides [here](https://docs.google.com/presentation/d/1jpQMjMDxtMqR5HQcxEjwXADQq66WB-MuIypmEpkElJI/edit?usp=sharing).

The project is broken down into three main folders:

### 1. Data Ingestion
Contains scripts for fetching the data we want to test our strategies on. Currently, it includes `polymarket_ingestion.py` which handles pulling data from Polymarket.

### 2. BackTrader
This folder shows how to use a standard, off-the-shelf Python backtesting library. It includes `fetch_data.py` to grab historical stock data, and `run_strategy.py` which runs a basic strategy, logs trades, and generates a clean tear sheet using QuantStats.

### 3. Backtesting Engine
A custom-built backtesting engine to demonstrate how event-driven backtesting works under the hood. It includes the core engine logic (`engine.py`), a place to write your own logic (`my_strategy.py`), and a script to generate performance reports (`generate_report.py`).

---

### Architecture

**Class Diagram**
*(Paste class diagram image here)*

**Core Event Loop**
*(Paste core event loop diagram here)*

---

Created by Cedric Claessens - connect with me on [LinkedIn](https://linkedin.com/cedric-cl/).
