# Exasol Sample Data
This repository contains data generators for sample data to get quickly started with Exasol. Those AI-generated Python scripts can be used by everyone to generate their own data and adapt as needed.

Currently the scripts generate two types of Parquet files:
* a set of online products of an imaginary webshop
* reviews to those products
  
See [definitions](#Datatypes-and-Mappings) for a detailed description of fields and datatypes.

## Sample Data on S3
Exasol also provides two generated sample Parquet files on S3 for direct import via IMPORT command:
* Online Products (1.000.000 rows, 28.6 MB): https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/online_products.parquet
* Product Reviews (1.822.007 rows, 162 MB): https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/product_reviews.parquet

## DDL and IMPORT
To quickly work with above datasets just run the following DDL and IMPORT commands:
```sql
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID        DECIMAL(18,0),
    PRODUCT_CATEGORY  VARCHAR(100),
    PRODUCT_NAME      VARCHAR(2000000),
    PRICE_USD         DOUBLE,
    INVENTORY_COUNT   DECIMAL(10,0),
    MARGIN            DOUBLE,
    DISTRIBUTE BY PRODUCT_ID
);

IMPORT INTO PRODUCTS
  FROM PARQUET AT 'https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/'
  FILE 'online_products.parquet';
```
```sql
CREATE OR REPLACE TABLE PRODUCT_REVIEWS (
    REVIEW_ID          DECIMAL(18,0),
    PRODUCT_ID         DECIMAL(18,0),
    PRODUCT_NAME       VARCHAR(2000000),
    PRODUCT_CATEGORY   VARCHAR(100),
    RATING             DECIMAL(2,0),
    REVIEW_TEXT        VARCHAR(100000),
    REVIEWER_NAME      VARCHAR(200),
    REVIEWER_PERSONA   VARCHAR(100),
    REVIEWER_AGE       DECIMAL(3,0),
    REVIEWER_LOCATION  VARCHAR(200),
    REVIEW_DATE       VARCHAR(200),
    DISTRIBUTE BY PRODUCT_ID
);

IMPORT INTO PRODUCT_REVIEWS
  FROM PARQUET AT 'https://exasol-easy-data-access.s3.eu-central-1.amazonaws.com/sample-data/'
  FILE 'product_reviews.parquet';
```


## Datatypes and Mappings
Currently there are two sample Parquet files, see their Parquet types and Exasol mapping below:

Online Products:
|Attribute|Parquet datatype|Exasol datatype|
|---|---|---|
|product_id	|INT64	|DECIMAL(18,0)|
|product_category	|BYTE_ARRAY (STRING)|	VARCHAR(100)|
|product_name	|BYTE_ARRAY (STRING)	|VARCHAR(2000000)|
|price_usd	|DOUBLE	|DOUBLE|
|inventory_count	|INT32	|DECIMAL(10,0)|
|margin|	DOUBLE|	DOUBLE|

Product Reviews:
|Attribute	|Parquet datatype|	Exasol datatype|
|---|---|---|
|review_id	|INT64	|DECIMAL(18,0)|
|product_id|	INT64	|DECIMAL(18,0)|
|product_name|	BYTE_ARRAY (STRING)	|VARCHAR(2000000)|
|product_category	|BYTE_ARRAY (STRING)	|VARCHAR(100)|
|rating	|INT32	|DECIMAL(2,0)|
|review_text|	BYTE_ARRAY (STRING)|	VARCHAR(100000)|
|reviewer_name	|BYTE_ARRAY (STRING)	|VARCHAR(200)|
|reviewer_persona|	BYTE_ARRAY (STRING)|	VARCHAR(100)|
|reviewer_age|	INT32	|DECIMAL(3,0)|
|reviewer_location|	BYTE_ARRAY (STRING)|	VARCHAR(200)|
|review_date	|BYTE_ARRAY (STRING)	|VARCHAR(200)|

## Data Excerpt

Online Products:
|PRODUCT_ID|PRODUCT_CATEGORY|PRODUCT_NAME|PRICE_USD|INVENTORY_COUNT|MARGIN|
|---|---|---|---|---|---|
|970771|Health & Household|Fitbit Heavy Duty Hand Sanitizer Gel Large - Black (3 Pack)|103.79|179199|0.4232|
|653200|Arts, Crafts & Sewing|Fitbit Upgraded Knitting Needles Set 128GB - Blue Bundle|953.65|165610|0.4681|
|532495|Books|TP-Link Productivity Planner Weekly 2TB - White Bundle|996.56|41317|0.6499|
|970751|Health & Household|Philips Fast Charging Digital Thermometer 20000mAh - Gray|651.82|41347|0.5584|
|983014|Toys & Games|Instant Pot Upgraded STEM Science Kit (3 Pack)|113.36|78752|0.4643|

Product Reviews:
|REVIEW_ID|PRODUCT_ID|PRODUCT_NAME|PRODUCT_CATEGORY|RATING|REVIEW_TEXT|REVIEWER_NAME|REVIEWER_PERSONA|REVIEWER_AGE|REVIEWER_LOCATION|REVIEW_DATE|
|---|---|---|---|---|---|---|---|---|---|---|
|1607402|882565|LEGO Compact Adjustable Bar Stools Set of 2 - White|General|4|Solid performance for LEGO Compact Adjustable Bar Stools Set of 2 - White. I travel a lot, so A few small issues, but overall positive.  In the General category, this product. Materials and fit/finish are impressive for the price. I'd purchase again.  - Samir Johnson, Frequent Traveler, Los Angeles, CA|Samir Johnson|Frequent Traveler|51|Los Angeles, CA|2024-08-13 07:38:01 AM|
|1607418|882580|TP-Link Upgraded Sewing Thread Set 60 Spools 128GB|General|4|Solid performance for TP-Link Upgraded Sewing Thread Set 60 Spools 128GB. I travel a lot, so Performs reliably and feels well constructed.  In the General category, this product. It consistently performs well in daily use. Good buy.  - Chris Johnson, Frequent Traveler, Los Angeles, CA|Chris Johnson|Frequent Traveler|55|Los Angeles, CA|2024-09-30 08:58:01 AM|
|1680476|922669|Lenovo 2026 Model Training Shoes Lightweight XL - White (4 Pack)|Shoes|5|Exceeded expectations with Lenovo 2026 Model Training Shoes Lightweight XL - White (4 Pack). I use it outdoors often, so Top-tier quality and attention to detail.  In the Shoes category, this product. It consistently performs well in daily use. Worth every penny.  - Harper Stone, Outdoor Enthusiast, Rotorua, NZ|Harper Stone|Outdoor Enthusiast|47|Rotorua, NZ|2023-03-15 12:37:30 PM|
|1511172|829752|Anker Ultra Baby Monitor with Camera - Black Bundle|Baby|1|Terrible experience with Anker Ultra Baby Monitor with Camera - Black Bundle. I cook a lot and care about ease of use, so The build felt cheap and failed.  In the Baby category, this product. I encountered multiple defects and usability problems. Save your money.  - Liam Singh, Home Chef, Rome, IT|Liam Singh|Home Chef|52|Rome, IT|2025-10-30 02:35:26 AM|
|1709357|938583|Dyson Smart LED Light Bulb Color Changing - Blue|General|4|Pretty pleased with Dyson Smart LED Light Bulb Color Changing - Blue. With kids in the house, Met expectations and pleasant to use.  In the General category, this product. Setup was straightforward and painless. Good buy.  - Kelly Williams, Parent Reviewer, Toronto, CA|Kelly Williams|Parent Reviewer|38|Toronto, CA|2024-02-09 05:14:57 PM|
