# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.
{% for Error in Errors %}
## {{ Error.name }}

> `{{ Error.code }}`

Template: {{ Error.template }}
Description: {{ Error.description }}
Tags: `{{ Error.tags|join(' ') }}`
{% endfor %}
