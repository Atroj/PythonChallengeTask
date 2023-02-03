from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('fetch_data', views.fetch_data, name="fetch_data"),
    path('files/show/<int:file_id>/<int:limit>', views.browse_data, name="browse_data"),
    path('files/show-count/<int:file_id>/<selected_columns>', views.browse_aggregate_data,
         name="browse_aggregate_data"),
]
