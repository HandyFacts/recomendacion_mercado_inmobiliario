from typing import Any
from django.core.management.base import BaseCommand
from apps.HandyFacts.models import City
import pandas as pd

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        df = pd.read_csv('cities.csv')
        cities_list = [ City(name=row['city'])
        for i, row in df.iterrows()]
        blk_msj = City.objects.bulk_create(
            cities_list
        )
        print(blk_msj)
        