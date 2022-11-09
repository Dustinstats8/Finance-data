# Finance-data
Finance-data is a simple script that scrapes stock and crypto data from Yahoo Finance and CNN Market.
The data is then uploaded to a google sheet for a centralized viewing of all the data.

In order to run daily, repl.it is used to run a flask server which then executes the script. Uptimerobot is then used to periodically ping the server in order to keep it up and running indefinitely.
