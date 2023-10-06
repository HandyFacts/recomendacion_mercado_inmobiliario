from django.urls import path, include
from . import views
from apps.HandyFacts.api.views import PropertyIdsApiView

app_name = 'handy-facts'

urlpatterns = [
    path('', views.index, name='home'),
    path('about/', views.about, name='about'),
    path('products/', views.products, name='products'),
    path('api/property/id/', PropertyIdsApiView.as_view())
]