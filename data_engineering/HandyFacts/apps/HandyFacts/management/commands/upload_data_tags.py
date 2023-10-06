from typing import Any
from django.core.management.base import BaseCommand
from apps.HandyFacts.models import State
import pandas as pd

class Command(BaseCommand):
    def handle(self, *args: Any, **options: Any) -> str | None:
        df = pd.read_csv('tags.csv')
        