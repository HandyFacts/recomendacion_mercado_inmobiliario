from django.urls import path
from . import views
from .api import views as api_views
urlpatterns = [
    path('signup/', views.SignUpView.as_view(), name='signup'),
]