# Exasol Sample Data
This repository contains some artificially created sample data to get you started with Exasol. In addition, we also provide the AI-generated Python data generation scripts for everyone to use and adapt.

## Datatypes and Mappings
Currently there are two sample Parquet files, see their Parquet types and Exasol mapping below:

Online Products:
|Attribute|Parquet datatype|Exasol datatype|
|---|---|---|
|product_id	|INT64	|DECIMAL(18,0)|
|product_category	|BYTE_ARRAY (STRING)|	VARCHAR(100)|
|product_name	|BYTE_ARRAY (STRING)	|VARCHAR(2000000)|
|price_usd	|DOUBLE	|DECIMAL(12,2)|
|inventory_count	|INT32	|DECIMAL(10,0)|
|margin|	DOUBLE|	DECIMAL(5,4)|

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
|review_date	|BYTE_ARRAY (STRING)	|TIMESTAMP|
