from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from apps.HandyFacts import models
from apps.HandyFacts.api.serializer import PropertySerializer

class PropertyIdsApiView(APIView):
    def get(self, request):
        list_data = list(models.Property.objects.values('property_id').all())
        list_ids = [index['property_id'] for index in list_data]
        return Response(
            status = status.HTTP_200_OK,
            data = {
                'ids':list_ids
            })