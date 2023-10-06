from django.db import models

# Create your models here.
class Members(models.Model):
    name = models.CharField(max_length=200, verbose_name='User')
    github =  models.URLField(verbose_name='Github url')
    rol = models.CharField(max_length=100)
