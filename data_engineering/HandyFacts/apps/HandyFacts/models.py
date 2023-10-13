from django.db import models

# Create your models here.
class State(models.Model):
    name = models.CharField(max_length=30)
    code = models.CharField(max_length=5)

class City(models.Model):
    name = models.CharField(max_length=50)

class Tag(models.Model):
    name = models.CharField(max_length=50)

class Property(models.Model):
    property_id = models.BigIntegerField(primary_key=True)
    longitude = models.FloatField(verbose_name='Longitude', null=True)
    latitude = models.FloatField(verbose_name='Latitude', null=True)
    postal_code = models.IntegerField(verbose_name='Postal Code')
    line = models.CharField(max_length=300, verbose_name='Line', null=True)
    fips_code = models.FloatField(verbose_name='Fips Code', null=True)
    name = models.CharField(max_length=300, verbose_name='Name', null=True)
    is_new_construction = models.BooleanField(null=True)
    is_plan = models.BooleanField(null=True)
    is_price_reduced = models.BooleanField(null=True)
    is_foreclosure = models.BooleanField(null=True)
    is_coming_soon = models.BooleanField(null=True)
    is_contigent = models.BooleanField(null=True)
    street_view_url = models.URLField(max_length=400,verbose_name='Street View')
    sqft = models.FloatField(null=True)
    baths = models.FloatField(null=True)
    lot_sqft = models.FloatField(null=True)
    year_built = models.FloatField(null=True)
    garage = models.FloatField(null=True)
    stories = models.FloatField(null=True)
    beds = models.FloatField(null=True)
    type = models.CharField(max_length=100, null=True)
    primary_photo = models.URLField(max_length=400,null=True)
    list_date = models.DateField(verbose_name='List Date')
    list_price = models.FloatField(null=True)
    status = models.CharField(max_length=50)

    state = models.ManyToManyField(State)
    city = models.ManyToManyField(City)
    tags = models.ManyToManyField(Tag)

    def __str__(self):
        return f'Id: {self.property_id}-Name:{self.name}'

class Demanda(models.Model):
    Date = models.DateField() 
    AL_usa = models.FloatField() 
    AR_usa = models.FloatField() 
    AZ_usa = models.FloatField() 
    CA_usa = models.FloatField() 
    CO_usa = models.FloatField() 
    CT_usa = models.FloatField() 
    FL_usa = models.FloatField() 
    GA_usa = models.FloatField() 
    HI_usa = models.FloatField() 
    IA_usa = models.FloatField()
    ID_usa = models.FloatField() 
    IL_usa = models.FloatField() 
    IN_usa = models.FloatField() 
    KS_usa = models.FloatField() 
    KY_usa = models.FloatField() 
    LA_usa = models.FloatField() 
    MA_usa = models.FloatField() 
    MD_usa = models.FloatField() 
    MI_usa = models.FloatField() 
    MN_usa = models.FloatField() 
    MO_usa = models.FloatField() 
    NC_usa = models.FloatField()
    NE_usa = models.FloatField() 
    NM_usa = models.FloatField() 
    NV_usa = models.FloatField() 
    NY_usa = models.FloatField() 
    OH_usa = models.FloatField() 
    OK_usa = models.FloatField() 
    OR_usa = models.FloatField() 
    PA_usa = models.FloatField() 
    RI_usa = models.FloatField() 
    SC_usa = models.FloatField() 
    TN_usa = models.FloatField() 
    TX_usa = models.FloatField()
    UT_usa = models.FloatField() 
    VA_usa = models.FloatField() 
    WA_usa = models.FloatField() 
    WI_usa = models.FloatField()

    class Meta:
        db_table = 'demanda'

class Houses_for_sale(models.Model):
    property_id = models.BigIntegerField(primary_key=True)
    lon = models.FloatField(null=True)
    lat = models.FloatField(null=True)
    postal_code = models.IntegerField(null=True)
    state = models.CharField(max_length=300, null=True)
    city = models.CharField(max_length=200, null=True)
    state_code = models.CharField(max_length=200, null=True)
    line = models.CharField(max_length=300, null=True)
    fips_code = models.FloatField(null=True)
    name = models.CharField(null=True)
    is_new_construction = models.BooleanField(null=True)
    is_plan = models.BooleanField(null=True)
    is_price_reduced = models.BooleanField(null=True)
    is_foreclosure = models.BooleanField(null=True)
    is_coming_soon = models.BooleanField(null=True)
    is_contingent = models.BooleanField(null=True)
    street_view_url = models.URLField(max_length=400,null=True)
    sqft = models.FloatField(null=True)
    baths = models.FloatField(null=True)
    lot_sqft = models.FloatField(null=True)
    year_built = models.FloatField(null=True)
    garage = models.FloatField(null=True)
    stories = models.FloatField(null=True)
    beds = models.FloatField(null=True)
    type = models.CharField(null=True, max_length=200)
    primary_photo = models.TextField(null=True)
    tags = models.TextField(null=True)
    list_date = models.DateField(null=True)
    photos = models.URLField(max_length=400, null=True)
    list_price = models.FloatField(null=True)
    listing_id = models.CharField(max_length=200, null=True)
    primary = models.BooleanField(null=True)
    status = models.CharField(max_length=200, null=True)
    prediction = models.FloatField(null=True)

    class Meta:
        db_table = 'houses_for_sale'

class Houses_sold(models.Model):
    property_id = models.BigIntegerField(primary_key=True)
    lon = models.FloatField(null=True)
    lat = models.FloatField(null=True)
    postal_code = models.IntegerField(null=True)
    state = models.CharField(max_length=300, null=True)
    city = models.CharField(max_length=200, null=True)
    state_code = models.CharField(max_length=200, null=True)
    line = models.CharField(max_length=300, null=True)
    fips_code = models.FloatField(null=True)
    name = models.CharField(null=True)
    is_new_construction = models.BooleanField(null=True)
    is_plan = models.BooleanField(null=True)
    is_price_reduced = models.BooleanField(null=True)
    is_foreclosure = models.BooleanField(null=True)
    is_coming_soon = models.BooleanField(null=True)
    is_contingent = models.BooleanField(null=True)
    street_view_url = models.URLField(max_length=400,null=True)
    sqft = models.FloatField(null=True)
    baths = models.FloatField(null=True)
    lot_sqft = models.FloatField(null=True)
    year_built = models.FloatField(null=True)
    garage = models.FloatField(null=True)
    stories = models.FloatField(null=True)
    beds = models.FloatField(null=True)
    type = models.CharField(null=True, max_length=200)
    primary_photo = models.TextField(null=True)
    tags = models.TextField(null=True)
    list_date = models.DateField(null=True)
    photos = models.URLField(max_length=400, null=True)
    list_price = models.FloatField(null=True)
    listing_id = models.CharField(max_length=200, null=True)
    status = models.CharField(max_length=200, null=True)

    class Meta:
        db_table = 'houses_sold'

class Graph(models.Model):
    name = models.CharField(max_length=200)
    link = models.URLField(max_length=400)