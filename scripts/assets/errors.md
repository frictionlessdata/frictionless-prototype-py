# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.
{% for Error in Errors %}
## {{ Error.name }}

> `{{ Error.code }}`

Tags: `{{ Error.tags|join(' ') }}` <br>
Template: {{ Error.template }} <br>
Description: {{ Error.description }}

{% endfor %}
