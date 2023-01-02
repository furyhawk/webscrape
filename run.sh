#!/bin/sh

if command -v caffeinate &> /dev/null
then
    echo "<caffeinate> activated"
    caffeinate -i -w $$ &
fi

echo "Scraping companies finance details"
python scrape_companies_stock.py