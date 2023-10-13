from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('apps.HandyFacts.urls')),
    #Path users
    path('accounts/', include('django.contrib.auth.urls')),
    path('accounts/', include('apps.Members.urls'))
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL,
document_root = settings.STATIC_ROOT)