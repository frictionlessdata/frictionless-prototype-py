# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.
{% for Error in Errors %}
## {{ Error.name }}

Code: `{{ Error.code }}` <br>
Tags: `{{ Error.tags|join(' ') or '-' }}` <br>
Template: `{{ Error.template }}` <br>
Description: `{{ Error.description }}` <br>

{% endfor %}
