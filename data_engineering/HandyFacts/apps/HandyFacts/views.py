import json
from typing import Any
from django.db.models.query import QuerySet
import pandas as pd
from . import models
from django.shortcuts import render, redirect
from django.core.paginator import Paginator
from django.db.models.functions import Cast
from django.db.models import F, IntegerField
from django.views.generic.list import ListView
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
# Create your views here.
def index(request):
    return render(request, 'HandyFacts/index.html')

@method_decorator(login_required, name='dispatch')
class Houses_list(ListView):
    model = models.Houses_for_sale
    paginate_by = 30

    def get_queryset(self) :
        search = self.request.GET.get('search_text')
        state = self.request.GET.get('state_list')
        city = self.request.GET.get('city')
        option_price = self.request.GET.get('option_price')
        if search:
            return models.Houses_for_sale.objects.filter(
                name__icontains = search
            )
        if state:
            return models.Houses_for_sale.objects.filter(
                state = state
            )
        if city:
            return models.Houses_for_sale.objects.filter(
                city = city
            )
        if option_price:
            if option_price == 'Low to High':
                return models.Houses_for_sale.objects.all().order_by('list_price')
            elif option_price == 'High to Low':
                return models.Houses_for_sale.objects.all().order_by('-list_price')
        return models.Houses_for_sale.objects.all()

    def get_context_data(self, **kwargs: Any):
        context =  super().get_context_data(**kwargs)
        context['states'] = models.Houses_for_sale.objects.values_list('state', flat=True).distinct()
        context['cities'] = models.Houses_for_sale.objects.values_list('city', flat=True).distinct()
        
        return context
    
def about(request):
    return render(request, 'HandyFacts/about.html')

@method_decorator(login_required, name='dispatch')
class GraphList(ListView):
    model = models.Graph
    template_name = 'HandyFacts/graph_list.html'

@login_required
def graph(request, graph_id):
    graph = models.Graph.objects.get(pk=graph_id)
    return render(request, 'HandyFacts/graph.html',{
        'graph':graph
    })

@login_required
def graph_create_form(request):
    return render(request, 'HandyFacts/graph_create.html')

@login_required
def graph_create(request):
    name = request.POST['name']
    url = request.POST['url']
    graph = models.Graph(name=name, link=url)
    graph.save()
    return redirect('handy-facts:graph_list')


@csrf_exempt
def create_house(request):
    if request.method == 'GET':
        return HttpResponse('<h1>Hola vista de prueba</h1>')
    if request.method =='POST':
        data = eval(json.loads(request.body.decode('utf-8')))
        property_id = data.get('property_id')
        lon = data.get('lon')
        lat = data.get('lat')
        postal_code = data.get('postal_code')
        state = data.get('state')
        city = data.get('city')
        state_code = data.get('state_code')
        line = data.get('line')
        fips_code = data.get('fips_code')
        name = data.get('name')
        is_new_construction = data.get('is_new_construction')
        is_plan = data.get('is_plan')
        is_price_reduced = data.get('is_price_reduced')
        is_foreclosure = data.get('is_foreclosure')
        is_coming_soon = data.get('is_coming_soon')
        is_contingent = data.get('is_contingent')
        street_view_url = data.get('street_view_url')
        sqft = data.get('sqft')
        baths = data.get('baths')
        lot_sqft = data.get('lot_sqft')
        year_built = data.get('year_built')
        garage = data.get('garage')
        stories = data.get('stories')
        beds = data.get('beds')
        type = data.get('type')
        primary_photo = data.get('primary_photo')
        tags = data.get('tags')
        list_date = data.get('list_date')
        photos = data.get('photos')
        list_price = data.get('list_price')
        listing_id = data.get('listing_id')
        primary = data.get('primary')
        status = data.get('status')
        prediction = data.get('prediction')

        msj_crt = models.Houses_for_sale.objects.create(
            property_id = property_id,
            lon = lon,
            lat = lat,
            postal_code = postal_code,
            state = state,
            city = city,
            state_code = state_code,
            line = line,
            fips_code = fips_code,
            name = name,
            is_new_construction = is_new_construction,
            is_plan = is_plan,
            is_price_reduced = is_price_reduced,
            is_foreclosure = is_foreclosure,
            is_coming_soon = is_coming_soon,
            is_contingent = is_contingent,
            street_view_url = street_view_url,
            sqft = sqft,
            baths = baths,
            lot_sqft = lot_sqft,
            year_built = year_built,
            garage = garage,
            stories = stories,
            beds = beds,
            type = type,
            primary_photo = primary_photo,
            tags = tags,
            list_date = list_date,
            photos = photos,
            list_price = list_price,
            listing_id = listing_id,
            primary = primary,
            status = status,
            prediction = prediction
        )

    return JsonResponse({'message':'Created succesfuly'})

def get_started(request):
    return render(request, 'HandyFacts/get-started.html')