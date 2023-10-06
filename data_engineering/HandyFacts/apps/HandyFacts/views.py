from django.shortcuts import render
from . import models

# Create your views here.
def index(request):
    return render(request, 'index.html')

def about(request):
    return render(request, 'about.html')

def products(request):
    propertys = models.Property.objects.all()[:30]

    return render(request, 'products.html', {
        'propertys': propertys
    })