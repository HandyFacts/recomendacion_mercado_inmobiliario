from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('apps.HandyFacts.urls')),
    #Path users
    path('accounts/', include('django.contrib.auth.urls')),
    path('accounts/', include('apps.Members.urls'))
]
