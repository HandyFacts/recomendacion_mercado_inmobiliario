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