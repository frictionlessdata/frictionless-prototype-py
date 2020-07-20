# Errors Reference

This document provides a full reference of the Frictionless errors.
{% for Error in Errors %}
## {{ Error.name }}

Code: {{ Error.code }}
Tags: {{ Error.tags|join(' ') }}
Template: {{ Error.template }}
Description: {{ Error.description }}
{% endfor %}
