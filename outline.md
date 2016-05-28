

[Data source](https://data.iowa.gov/Economy/Iowa-Liquor-Sales/m3tr-qhgy#About)

Tools:
* sqlalchemy
    - stores and transactions (m2m)
    - sqlite and/or postgres
    - automatic updating?

* flask
    - dashboard for stores, counties, etc.
    - plots for visualization

* predictive analytics
    - statsmodels
    - sci-kit learn


Issues with the data:
* Massive redundancy due to wide format, really three tables
* Inconsistent header names (units for example)
* Unecessary $ symbols
* Address / location entries are a mess: longitude and latitude should be their own columns

Grade: B


**Visualization Ideas**
* Consumption per county/zip_code per alcohol type
    * per capita
    * per day of the week
* Total sales per month, year
    * state wide
    * for each county
* Mash up with other data:
    * demographics
    * health outcomes
    * mean salary

**Predictive Models**
* Sales projections
* Seasonality of alcohol categories
* Product popularity over time
