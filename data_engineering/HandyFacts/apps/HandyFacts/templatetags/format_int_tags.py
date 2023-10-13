from django import template
from django.template.defaultfilters import floatformat
import ast

register = template.Library()

@register.filter
def format_price(value):
    return "${:,.0f}".format(value)

@register.filter
def create_list(value):
    if isinstance(value, str):
        return ast.literal_eval(value)
    else:
        return ['No tags',]
    
@register.filter
def replace_and_upper(value : str):
    return value.replace('_',' ').capitalize()