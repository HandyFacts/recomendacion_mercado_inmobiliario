from django.core.management.base import BaseCommand
from apps.HandyFacts.models import Property
import pandas as pd

class Command(BaseCommand):

    def handle(self, *args, **kwargs):
        df = pd.read_csv('data/property.csv')[['property_id', 
        'longitude', 'latitude', 'postal_code',
       'line', 'fips_code', 'name', 'is_new_construction',
       'is_plan', 'is_price_reduced', 
       'is_foreclosure', 'is_coming_soon',
       'is_contingent', 'street_view_url', 
       'sqft', 'baths', 'lot_sqft',
       'year_built', 'garage', 'stories', 
       'beds', 'type', 'primary_photo',
       'list_date', 'list_price','status']]
        
        property_list = [
            Property(
                property_id = row['property_id'] if not pd.isna(row['property_id']) else None,
                longitude = row['longitude'] if not pd.isna(row['longitude']) else None,
                latitude = row['latitude'] if not pd.isna(row['latitude']) else None,
                postal_code = row['postal_code'] if not pd.isna(row['postal_code']) else None,
                line = row['line'] if not pd.isna(row['line']) else None,
                fips_code = row['fips_code'] if not pd.isna(row['fips_code']) else None,
                name = row['name'] if not pd.isna(row['name']) else None,
                is_new_construction = row['is_new_construction'] if not pd.isna(row['is_new_construction']) else None,
                is_plan = row['is_plan'] if not pd.isna(row['is_plan']) else None,
                is_price_reduced = row['is_price_reduced'] if not pd.isna(row['is_price_reduced']) else None,
                is_foreclosure = row['is_foreclosure'] if not pd.isna(row['is_foreclosure']) else None,
                is_coming_soon = row['is_coming_soon'] if not pd.isna(row['is_coming_soon']) else None,
                is_contigent = row['is_contingent'] if not pd.isna(row['is_contingent']) else None,
                street_view_url = row['street_view_url'] if not pd.isna(row['street_view_url']) else None,
                sqft = row['sqft'] if not pd.isna(row['sqft']) else None,
                baths = row['baths'] if not pd.isna(row['baths']) else None,
                lot_sqft = row['lot_sqft'] if not pd.isna(row['lot_sqft']) else None,
                year_built = row['year_built'] if not pd.isna(row['year_built']) else None,
                garage = row['garage'] if not pd.isna(row['garage']) else None,
                stories = row['stories'] if not pd.isna(row['stories']) else None,
                beds = row['beds'] if not pd.isna(row['beds']) else None,
                type = row['type'] if not pd.isna(row['type']) else None,
                primary_photo = row['primary_photo'] if not pd.isna(row['primary_photo']) else None,
                list_date = row['list_date'] if not pd.isna(row['list_date']) else None,
                list_price = row['list_price'] if not pd.isna(row['list_price']) else None,
                status = row['status'] if not pd.isna(row['status']) else None
            )
            for i, row in df.iterrows()
        ]
        bulk_msj = Property.objects.bulk_create(
            property_list
        )
        print(bulk_msj)
        