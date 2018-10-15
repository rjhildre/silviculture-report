from django.urls import path

from . import views

app_name = 'report'
urlpatterns= [
    path('', views.index, name='index'),
    path('fma-report/', views.fma_report, name='fma-report'),
]