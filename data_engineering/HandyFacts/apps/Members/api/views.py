from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from apps.Members import models
from apps.Members.api.serializer import MemberSerializer

class MemberApiView(APIView):
    def get(self, request):
        serializer = MemberSerializer(models.Members.objects.all(), many = True)
        return Response(status=status.HTTP_200_OK,
                        data= serializer)
    
    def post(self, request):
        serializer = MemberSerializer(data=request.POST)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(status=status.HTTP_200_OK,data=serializer.data )