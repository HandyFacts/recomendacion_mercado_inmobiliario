from django.contrib.auth.forms import UserCreationForm
from django.views.generic import CreateView
from django.urls import reverse_lazy
from django import forms
# Create your views here.
class SignUpView(CreateView):
    form_class = UserCreationForm
    template_name = 'registration/signup.html'

    def get_success_url(self):
        return reverse_lazy('login') + '?register'

    def get_form(self, form_class=None):
        form = super(SignUpView, self).get_form()

        form.fields['username'].widget = forms.TextInput(
            attrs={
                'class':'input-box',
                'placeholder':'Username'
            }
        )
        form.fields['password1'].widget = forms.PasswordInput(
            attrs={
                'class':'input-box',
                'placeholder':'Password'
            }
        )
        form.fields['password2'].widget = forms.PasswordInput(
            attrs={
                'class':'input-box',
                'placeholder':'Repeat password'
            }
        )

        return form