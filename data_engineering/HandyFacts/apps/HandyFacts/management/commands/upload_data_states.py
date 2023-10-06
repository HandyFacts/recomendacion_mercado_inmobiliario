from django.core.management.base import BaseCommand
from apps.HandyFacts.models import State
import pandas as pd

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        df : pd.DataFrame = pd.read_csv('states.csv')[['state','state_code']]
        state_list = [
            State(
                name = row['state'],
                code = row['state_code']
            )
            for i, row in df.iterrows()
        ]
        blk_msj = State.objects.bulk_create(
            state_list
        )
        print(blk_msj)