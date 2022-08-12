from typing import List

class Config:
    BOOKS_FNAME: str = "books.csv"
    CHECKOUTS_FNAME: str = "checkouts.csv"
    CHECKOUTS_FNAME_CLEAN: str = "checkouts_clean.parquet"
    CUSTOMERS_FNAME: str = "customers.csv"
    LIBRARIES_FNAME: str = "libraries.csv"
    DELAY_DAY: int = 28
    PREPARED_DATASET_FNAME: str = "preprocessed.parquet"
    MISSING_COMMON: str = "UNK"
        
class DataEnc:
    TARGET: List[str] = ["is_delayed"]
    CAT_ENC: List[str] = [
        "name_lib", "city_lib", "region", "publisher", "categories",
        "city_customer", "state", "gender", "education", "occupation",
    ] 

    NUM_ENC: List[str] = ["price", "pages", "book_age_days", "customer_age_days"]
    DATETIME_ENC: List[str] = ["birth_date", "publishedDate"]
    HYPOTHESIS: List[str] = ["authors", "date_checkout", "date_returned", "street_address_lib", "title"]