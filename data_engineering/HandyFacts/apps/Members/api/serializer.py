from rest_framework.serializers import ModelSerializer
from apps.Members import models

class MemberSerializer(ModelSerializer):
    class Meta:
        model = models.Members
        fields = ['name','github','rol']
