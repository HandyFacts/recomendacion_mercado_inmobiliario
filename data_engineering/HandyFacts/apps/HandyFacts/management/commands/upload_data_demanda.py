from django.core.management.base import BaseCommand
from apps.HandyFacts.models import Demanda
import pandas as pd

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        df = pd.read_csv('data/demanda_concatenada.csv')
        demanda_object = [
            Demanda(
                Date = row['Date'],
                AL_usa = row['AL'],
                AR_usa = row['AR'], 
                AZ_usa = row['AZ'], 
                CA_usa = row['CA'], 
                CO_usa = row['CO'], 
                CT_usa = row['CT'], 
                FL_usa = row['FL'], 
                GA_usa = row['GA'], 
                HI_usa = row['HI'], 
                IA_usa = row['IA'],
                ID_usa = row['ID'], 
                IL_usa = row['IL'], 
                IN_usa = row['IN'], 
                KS_usa = row['KS'], 
                KY_usa = row['KY'], 
                LA_usa = row['LA'], 
                MA_usa = row['MA'], 
                MD_usa = row['MD'], 
                MI_usa = row['MI'], 
                MN_usa = row['MN'], 
                MO_usa = row['MO'], 
                NC_usa = row['NC'],
                NE_usa = row['NE'], 
                NM_usa = row['NM'], 
                NV_usa = row['NV'], 
                NY_usa = row['NY'], 
                OH_usa = row['OH'], 
                OK_usa = row['OK'], 
                OR_usa = row['OR'], 
                PA_usa = row['PA'], 
                RI_usa = row['RI'], 
                SC_usa = row['SC'], 
                TN_usa = row['TN'], 
                TX_usa = row['TX'],
                UT_usa = row['UT'], 
                VA_usa = row['VA'], 
                WA_usa = row['WA'], 
                WI_usa = row['WI'],
            )
            for i, row in df.iterrows()
        ]

        blk_msj = Demanda.objects.bulk_create(
            demanda_object
        )

        print(blk_msj)