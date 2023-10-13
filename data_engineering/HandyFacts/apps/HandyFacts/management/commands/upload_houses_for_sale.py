from django.core.management.base import BaseCommand
from apps.HandyFacts.models import Houses_for_sale
import pandas as pd

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        df = pd.read_csv('data/Houses_for_sale_processed_with_pred.csv')
        houses_obj = [
        Houses_for_sale(    
            property_id = row['property_id'] if not pd.isna(row['property_id']) else None ,
            lon = row['lon'] if not pd.isna(row['lon']) else None ,
            lat = row['lat'] if not pd.isna(row['lat']) else None ,
            postal_code = row['postal_code'] if not pd.isna(row['postal_code']) else None ,
            state = row['state'] if not pd.isna(row['state']) else None ,
            city = row['city'] if not pd.isna(row['city']) else None ,
            state_code = row['state_code'] if not pd.isna(row['state_code']) else None ,
            line = row['line'] if not pd.isna(row['line']) else None ,
            fips_code = row['fips_code'] if not pd.isna(row['fips_code']) else None ,
            name = row['name'] if not pd.isna(row['name']) else None ,
            is_new_construction = row['is_new_construction'] if not pd.isna(row['is_new_construction']) else None ,
            is_plan = row['is_plan'] if not pd.isna(row['is_plan']) else None ,
            is_price_reduced = row['is_price_reduced'] if not pd.isna(row['is_price_reduced']) else None ,
            is_foreclosure = row['is_foreclosure'] if not pd.isna(row['is_foreclosure']) else None ,
            is_coming_soon = row['is_coming_soon'] if not pd.isna(row['is_coming_soon']) else None ,
            is_contingent = row['is_contingent'] if not pd.isna(row['is_contingent']) else None ,
            street_view_url = row['street_view_url'] if not pd.isna(row['street_view_url']) else None ,
            sqft = row['sqft'] if not pd.isna(row['sqft']) else None ,
            baths = row['baths'] if not pd.isna(row['baths']) else None ,
            lot_sqft = row['lot_sqft'] if not pd.isna(row['lot_sqft']) else None ,
            year_built = row['year_built'] if not pd.isna(row['year_built']) else None ,
            garage = row['garage'] if not pd.isna(row['garage']) else None ,
            stories = row['stories'] if not pd.isna(row['stories']) else None ,
            beds = row['beds'] if not pd.isna(row['beds']) else None ,
            type =  row['type'] if not pd.isna(row['type']) else None ,
            primary_photo = row['primary_photo'] if not pd.isna(row['primary_photo']) else None ,
            tags = row['tags'] if not pd.isna(row['tags']) else None ,
            list_date = row['list_date'] if not pd.isna(row['list_date']) else None ,
            photos = row['photos'] if not pd.isna(row['photos']) else None ,
            list_price = row['list_price'] if not pd.isna(row['list_price']) else None ,
            listing_id = row['listing_id'] if not pd.isna(row['listing_id']) else None ,
            primary = row['primary'] if not pd.isna(row['primary']) else None ,
            status = row['status'] if not pd.isna(row['status']) else None,
            prediction = row['prediction'] if not pd.isna(row['prediction']) else None
        )
            for i, row in df.iterrows()
        ]

        blk_msj = Houses_for_sale.objects.bulk_create(
            houses_obj
        )

        print(blk_msj)