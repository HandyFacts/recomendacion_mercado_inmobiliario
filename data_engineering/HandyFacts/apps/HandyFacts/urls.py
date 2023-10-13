from django.urls import path, include
from . import views
#from apps.HandyFacts.api.views import PropertyIdsApiView

app_name = 'handy-facts'

urlpatterns = [
    path('', views.index, name='home'),
    path('about/', views.about, name='about'),
    path('house_post/', views.create_house, name='house_create'),
    # path('products/', views.products, name='products'),
    path('houses/', views.Houses_list.as_view(), name='houses'),
    path('graph_list/', views.GraphList.as_view(), name='graph_list'),
    path('graph/<int:graph_id>', views.graph, name='graph_detail'),
    path('graph_create_form/', views.graph_create_form, name='graph_create_form'),
    path('graph_create/', views.graph_create, name='graph_create'),
    path('get_started/', views.get_started, name='started')
    #path('api/property/id/', PropertyIdsApiView.as_view())
]